import os
import logging
import asyncio
import sqlite3
import cv2
import numpy as np
from dotenv import load_dotenv
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, InputFile
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, CallbackContext, filters
)
from PIL import Image
import pytesseract
import fitz
from gtts import gTTS
from g4f.client import AsyncClient
from rutube import Rutube

g4f_client = AsyncClient()

load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

MAX_PHOTO_SIZE = 10 * 1024 * 1024
MAX_DOCUMENT_SIZE = 50 * 1024 * 1024
MAX_TELEGRAM_FILE_SIZE = 50 * 1024 * 1024

user_links = {}

def create_db():
    logging.info("Создание базы данных...")
    conn = sqlite3.connect("files.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            file_path TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()
    logging.info("База данных готова.")

def save_file_to_db(user_id, file_path):
    logging.info(f"Сохранение пути файла в БД: user_id={user_id}, file_path={file_path}")
    conn = sqlite3.connect("files.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO files (user_id, file_path) VALUES (?, ?)", (user_id, file_path))
    conn.commit()
    conn.close()

def delete_old_file(user_id):
    logging.info(f"Удаление предыдущего файла пользователя {user_id}")
    conn = sqlite3.connect("files.db")
    cursor = conn.cursor()
    cursor.execute("SELECT file_path FROM files WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user_id,))
    result = cursor.fetchone()
    if result:
        file_path = result[0]
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Файл удалён: {file_path}")
        cursor.execute("DELETE FROM files WHERE user_id = ?", (user_id,))
        conn.commit()
    conn.close()

def extract_text_from_pdf(pdf_path):
    logging.info(f"Извлечение текста из PDF: {pdf_path}")
    doc = fitz.open(pdf_path)
    return "".join([page.get_text("text") for page in doc]).strip()

def convert_pdf_to_images(pdf_path):
    logging.info(f"Конвертация PDF в изображения: {pdf_path}")
    doc = fitz.open(pdf_path)
    images = []
    for page in doc:
        pix = page.get_pixmap()
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        images.append(img)
    return images

def preprocess_image(image):
    logging.info("Предобработка изображения")
    gray = cv2.cvtColor(np.array(image), cv2.COLOR_BGR2GRAY)
    gray = cv2.resize(gray, None, fx=3, fy=3, interpolation=cv2.INTER_LANCZOS4)
    gray = cv2.medianBlur(gray, 3)
    return Image.fromarray(gray)

async def start(update: Update, context: CallbackContext):
    logging.info(f"Пользователь {update.effective_user.id} вызвал /start")
    context.user_data.clear()
    keyboard = [["Распознать", "Распознать как фотографию"], ["Распознать с помощью ChatGPT"], ["Скачать Rutube Shorts"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("Привет! Выбери, что хочешь сделать.", reply_markup=reply_markup)

async def handle_file(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    logging.info(f"Пользователь {user_id} отправил файл.")
    delete_old_file(user_id)

    if update.message.document:
        doc = update.message.document
        if doc.file_size > MAX_DOCUMENT_SIZE:
            logging.warning(f"Файл слишком большой: {doc.file_size}")
            return await update.message.reply_text("Файл слишком большой.")
        file = await doc.get_file()
        ext = doc.file_name.split(".")[-1]
        path = f"files/{user_id}_file.{ext}"
        await file.download_to_drive(path)
    elif update.message.photo:
        photo = update.message.photo[-1]
        if photo.file_size > MAX_PHOTO_SIZE:
            logging.warning(f"Фото слишком большое: {photo.file_size}")
            return await update.message.reply_text("Фото слишком большое.")
        file = await photo.get_file()
        path = f"files/{user_id}_photo.jpg"
        await file.download_to_drive(path)
    else:
        logging.warning("Неподдерживаемый файл.")
        return

    save_file_to_db(user_id, path)
    await update.message.reply_text("Файл получен. Выберите действие.")

async def handle_resolution(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    res = query.data  # Например: "1920x1080" или просто число (1080)
    user_id = query.from_user.id
    ru = user_links.get(user_id)
    
    if not ru:
        await query.edit_message_text("Ошибка: ссылка не найдена.")
        return

    msg = await query.edit_message_text(f"Загрузка видео в {res}...")
    
    try:
        # Получаем числовое значение разрешения
        if "x" in res:  # Если формат "1920x1080"
            resolution_value = int(res.split("x")[-1])
        else:  # Если уже число (например, 1080)
            resolution_value = int(res)

        video = ru.get_by_resolution(resolution_value)
        path = f"downloads/{video.title}.mp4"
        os.makedirs("downloads", exist_ok=True)

        # Загрузка с прогрессом
        queue = asyncio.Queue()
        task_progress = asyncio.create_task(update_progress_worker(msg, res, queue))
        await run_download(video, path, queue)
        await queue.put((None, None))
        await task_progress

        # Проверка размера файла
        if os.path.getsize(path) > MAX_TELEGRAM_FILE_SIZE:
            logging.warning("Файл слишком большой для Telegram.")
            
            # Получаем доступные разрешения меньше текущего
            available_lower_resolutions = []
            for r in ru.available_resolutions:
                if isinstance(r, str) and "x" in r:  # Если формат "1920x1080"
                    res_value = int(r.split("x")[-1])
                else:  # Если уже число
                    res_value = int(r)
                
                if res_value < resolution_value:
                    available_lower_resolutions.append(r)

            if not available_lower_resolutions:
                await msg.edit_text("Файл слишком большой, и нет меньшего разрешения.")
                os.remove(path)
                return

            # Предлагаем выбрать меньшее разрешение
            buttons = [[InlineKeyboardButton(str(r), callback_data=str(r))] for r in available_lower_resolutions]
            reply_markup = InlineKeyboardMarkup(buttons)
            await msg.edit_text(
                "Файл слишком большой. Выберите меньшее разрешение:",
                reply_markup=reply_markup
            )
            return

        # Отправляем видео, если размер подходит
        await msg.edit_text("Отправка видео...")
        with open(path, "rb") as f:
            await context.bot.send_video(chat_id=query.message.chat_id, video=f, caption=video.title)
        await msg.edit_text("Готово!")
        os.remove(path)

    except Exception as e:
        logging.error(f"Ошибка: {e}", exc_info=True)
        await msg.edit_text(f"Ошибка: {e}")

async def update_progress_worker(message, resolution, queue):
    last_percent = 0
    while True:
        current, total = await queue.get()
        if current is None:
            break
        percent = int((current / total) * 100)
        if percent != last_percent:
            last_percent = percent
            await message.edit_text(f"Загрузка: {percent}%")

async def run_download(video, path, queue):
    loop = asyncio.get_running_loop()  # получаем event loop ОДИН РАЗ в главном потоке

    def callback(cur, total):
        asyncio.run_coroutine_threadsafe(queue.put((cur, total)), loop).result()

    await asyncio.to_thread(video.download, path=os.path.dirname(path), workers=8, progress_callback=callback)

async def recognize_text_from_file(user_id, as_photo=False):
    conn = sqlite3.connect("files.db")
    cursor = conn.cursor()
    cursor.execute("SELECT file_path FROM files WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user_id,))
    result = cursor.fetchone()
    conn.close()

    if not result:
        logging.warning(f"Файл пользователя {user_id} не найден.")
        return None, "Файл не найден."
    path = result[0]
    ext = path.split(".")[-1].lower()

    try:
        if ext == "pdf":
            if as_photo:
                images = convert_pdf_to_images(path)
                text = "".join(pytesseract.image_to_string(preprocess_image(img), lang="rus+eng") for img in images)
                return text.strip(), None
            else:
                text = extract_text_from_pdf(path)
                return text.strip(), None
        else:
            img = Image.open(path)
            text = pytesseract.image_to_string(preprocess_image(img), lang="rus+eng")
            return text.strip(), None
    except Exception as e:
        logging.exception("Ошибка распознавания текста")
        return None, f"Ошибка распознавания: {e}"

async def text_to_speech(update: Update, context: CallbackContext, text: str):
    logging.info(f"Генерация аудио для пользователя {update.effective_user.id}")
    await update.message.reply_text("Озвучиваю текст...")
    tts = gTTS(text, lang="ru")
    path = f"files/audio_{update.message.from_user.id}.mp3"
    tts.save(path)
    with open(path, "rb") as f:
        await update.message.reply_audio(audio=InputFile(f))
    os.remove(path)

async def send_long(update: Update, text: str):
    for i in range(0, len(text), 4096):
        await update.message.reply_text(text[i:i+4096])

async def handle_text(update: Update, context: CallbackContext):
    text = update.message.text
    user_id = update.message.from_user.id
    logging.info(f"Пользователь {user_id} ввёл текст: {text}")

    if text == "Скачать Rutube Shorts":
        context.user_data["mode"] = "rutube"
        return await update.message.reply_text("Отправьте ссылку на Rutube Shorts.")
    elif text == "Распознать":
        recognized, error = await recognize_text_from_file(user_id, False)
    elif text == "Распознать как фотографию":
        recognized, error = await recognize_text_from_file(user_id, True)
    elif text == "Распознать с помощью ChatGPT":
        await update.message.reply_text("Распознаю...")
        recognized, error = await recognize_text_from_file(user_id, False)
        if not error:
            context.user_data["recognized_text"] = recognized
            keyboard = [["Краткий пересказ", "Перевод на английский"], ["Объясни текст", "Собрать по смыслу с помощью в GPT"]]
            return await update.message.reply_text("Текст готов. Что сделать?", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
        else:
            return await update.message.reply_text(error)
    elif text in ["Краткий пересказ", "Перевод на английский", "Объясни текст", "Собрать по смыслу с помощью в GPT"]:
        recognized = context.user_data.get("recognized_text")
        if not recognized:
            return await update.message.reply_text("Нет текста.")
        prompt_map = {
            "Краткий пересказ": "Сделай краткий пересказ:\n",
            "Перевод на английский": "Переведи на английский:\n",
            "Объясни текст": "Объясни смысл:\n",
            "Собрать по смыслу с помощью в GPT": "Собери текст по смыслу, без лишнего:\n"
        }
        await update.message.reply_text("Обращаюсь к GPT...")
        resp = await g4f_client.chat.completions.create(model="gpt-4", messages=[{"role": "user", "content": prompt_map[text] + recognized}])
        gpt_text = resp.choices[0].message.content.strip()
        context.user_data["gpt_text"] = gpt_text
        await send_long(update, gpt_text)
        return await update.message.reply_text("Выберите действие:", reply_markup=ReplyKeyboardMarkup([["Перевести в аудио"], ["Назад"]], resize_keyboard=True))
    elif text == "Перевести в аудио":
        text = context.user_data.get("gpt_text") or context.user_data.get("recognized_text")
        if text:
            return await text_to_speech(update, context, text)
    elif text == "Назад":
        return await start(update, context)
    elif context.user_data.get("mode") == "rutube" and "rutube.ru" in text:
        try:
            ru = Rutube(text)
            user_links[user_id] = ru
            res_buttons = [[InlineKeyboardButton(res, callback_data=res)] for res in ru.available_resolutions]
            context.user_data["mode"] = None
            return await update.message.reply_text("Выберите разрешение:", reply_markup=InlineKeyboardMarkup(res_buttons))
        except Exception as e:
            logging.exception("Ошибка при парсинге ссылки Rutube")
            return await update.message.reply_text(f"Ошибка: {e}")

def main():
    logging.info("Запуск бота...")
    create_db()
    os.makedirs("downloads", exist_ok=True)
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_resolution))
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL, handle_file))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.run_polling()

if __name__ == "__main__":
    main()
    logging.info("Бот запущен.")
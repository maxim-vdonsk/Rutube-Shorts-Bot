import os
import logging
import asyncio
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)
from rutube import Rutube

# Загрузка переменных окружения
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Настройка логов
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Временное хранилище ссылок
user_links = {}

# Максимальный размер файла для Telegram (в байтах)
MAX_TELEGRAM_FILE_SIZE = 50 * 1024 * 1024  # 50 MB


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Я бот для скачивания Shorts \nОтправь мне ссылку Shorts на Rutube-видео.")


async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text
    try:
        ru = Rutube(url)
        user_links[update.effective_user.id] = ru

        resolutions = ru.available_resolutions
        keyboard = [
            [InlineKeyboardButton(text=res, callback_data=res)] for res in resolutions
        ]
        await update.message.reply_text(
            "Выбери разрешение:", reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def handle_resolution(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    res = query.data
    user_id = query.from_user.id
    ru = user_links.get(user_id)

    if not ru:
        await query.edit_message_text("Ссылка не найдена. Попробуй сначала.")
        return

    # Создаем сообщение с прогрессом
    progress_message = await query.edit_message_text(f"🔄 Начинаю загрузку видео в {res}...")
    
    video_path = None
    try:
        video = ru.get_by_resolution(int(res.split("x")[-1]))
        video_path = f"{video.title}.mp4"
        full_path = os.path.join("downloads", video_path)
        
        # Создаем очередь для обновления прогресса
        progress_queue = asyncio.Queue()
        
        # Запускаем задачу для обновления прогресса
        progress_task = asyncio.create_task(
            update_progress_worker(progress_message, res, progress_queue)
        )
        
        # Запускаем загрузку в фоне
        download_task = asyncio.create_task(
            run_download(
                video=video,
                path=full_path,
                progress_queue=progress_queue
            )
        )
        
        # Ждем завершения загрузки
        await download_task
        
        # Завершаем задачу прогресса
        await progress_queue.put((None, None))
        await progress_task

        # Проверяем результат загрузки
        if not os.path.exists(full_path):
            await progress_message.edit_text("❌ Ошибка при загрузке видео")
            return
            
        file_size = os.path.getsize(full_path)
        if file_size > MAX_TELEGRAM_FILE_SIZE:
            await progress_message.edit_text(
                f"⚠️ Файл слишком большой для отправки ({file_size//(1024*1024)}MB > {MAX_TELEGRAM_FILE_SIZE//(1024*1024)}MB)"
            )
            return
            
        await progress_message.edit_text("📤 Отправляю файл...")
        with open(full_path, "rb") as video_file:
            await context.bot.send_video(
                chat_id=query.message.chat_id,
                video=video_file,
                caption=video.title,
                read_timeout=60,
                write_timeout=60,
                connect_timeout=60
            )
        await progress_message.edit_text("✅ Видео успешно отправлено!")
    except Exception as e:
        await progress_message.edit_text(f"❌ Произошла ошибка: {e}")
    finally:
        if video_path and os.path.exists(full_path):
            try:
                os.remove(full_path)
            except Exception as e:
                logger.error(f"Ошибка при удалении файла: {e}")


async def update_progress_worker(message, resolution, queue):
    """Обновляет прогресс загрузки"""
    last_percent = 0
    while True:
        current, total = await queue.get()
        if current is None:  # Сигнал завершения
            break
            
        percent = int((current / total) * 100)
        if percent != last_percent:
            last_percent = percent
            stages = {
                0: "🔄 Загрузка видео...",
                30: "📦 Обработка файла...",
                70: "📤 Подготовка к отправке...",
                100: "✅ Файл готов к отправке!"
            }
            stage = next((v for k, v in stages.items() if percent >= k), stages[100])
            await message.edit_text(
                f"{stage}\nПрогресс: {percent}%"
            )


async def run_download(video, path, progress_queue):
    """Запускает загрузку видео с обновлением прогресса"""
    def progress_callback(current, total):
        # Добавляем в очередь из синхронного кода
        asyncio.run_coroutine_threadsafe(
            progress_queue.put((current, total)),
            asyncio.get_event_loop()
        ).result()
    
    # Создаем папку если нет
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    # Запускаем синхронную загрузку
    await asyncio.to_thread(
        video.download,
        path=os.path.dirname(path),
        workers=8,
        progress_callback=progress_callback
    )


def main():
    if not os.path.exists("downloads"):
        os.makedirs("downloads")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    app.add_handler(CallbackQueryHandler(handle_resolution))

    app.run_polling()


if __name__ == "__main__":
    main()
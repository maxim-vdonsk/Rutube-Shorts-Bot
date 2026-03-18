"""
Модуль Telegram-бота для скачивания видео из Rutube Shorts.

Этот файл содержит упрощённую версию бота, которая поддерживает только
скачивание видео. Для полного функционала используйте text_rutube.py.

Автор: maxim_vdonsk
"""

import os
import logging
import asyncio
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from rutube import Rutube

# =============================================================================
# КОНФИГУРАЦИЯ И ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# =============================================================================

# Загрузка переменных окружения из файла .env
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Временное хранилище ссылок для пользователей (user_id -> Rutube объект)
user_links: dict = {}

# Максимальный размер файла для отправки через Telegram (50 MB)
MAX_TELEGRAM_FILE_SIZE = 50 * 1024 * 1024


# =============================================================================
# ОБРАБОТЧИКИ КОМАНД И СООБЩЕНИЙ
# =============================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик команды /start.

    Отправляет приветственное сообщение пользователю.
    """
    await update.message.reply_text(
        "Привет! Я бот для скачивания Shorts.\n"
        "Отправь мне ссылку на Rutube-видео."
    )


async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик ссылок на Rutube.

    Парсит ссылку, получает доступные разрешения и предлагает пользователю выбор.
    """
    url = update.message.text
    logger.info(f"Получена ссылка от пользователя {update.effective_user.id}: {url}")

    try:
        # Создаём объект Rutube для работы с видео
        ru = Rutube(url)
        user_links[update.effective_user.id] = ru

        # Получаем доступные разрешения видео
        resolutions = ru.available_resolutions

        # Создаём клавиатуру с кнопками разрешений
        keyboard = [
            [InlineKeyboardButton(text=res, callback_data=res)]
            for res in resolutions
        ]

        await update.message.reply_text(
            "Выбери разрешение:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Ошибка при обработке ссылки: {e}", exc_info=True)
        await update.message.reply_text(f"Ошибка: {e}")


async def handle_resolution(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Обработчик выбора разрешения видео.

    Загружает видео в выбранном качестве и отправляет пользователю.
    """
    query = update.callback_query
    await query.answer()

    resolution = query.data  # Формат: "1920x1080" или "1080"
    user_id = query.from_user.id
    logger.info(f"Пользователь {user_id} выбрал разрешение: {resolution}")

    # Получаем объект Rutube из хранилища
    ru = user_links.get(user_id)
    if not ru:
        await query.edit_message_text("Ссылка не найдена. Попробуй сначала.")
        return

    # Создаём сообщение с прогрессом загрузки
    progress_message = await query.edit_message_text(
        f"🔄 Начинаю загрузку видео в {resolution}..."
    )

    video_path = None
    full_path = None

    try:
        # Извлекаем числовое значение разрешения (например, 1080 из "1920x1080")
        resolution_value = (
            int(resolution.split("x")[-1])
            if "x" in resolution
            else int(resolution)
        )

        # Получаем видео с нужным разрешением
        video = ru.get_by_resolution(resolution_value)
        video_path = f"{video.title}.mp4"
        full_path = os.path.join("downloads", video_path)

        # Создаём очередь для обновления прогресса
        progress_queue = asyncio.Queue()

        # Запускаем задачу для обновления прогресса
        progress_task = asyncio.create_task(
            update_progress_worker(progress_message, resolution, progress_queue)
        )

        # Запускаем загрузку видео
        download_task = asyncio.create_task(
            run_download(video=video, path=full_path, progress_queue=progress_queue)
        )

        # Ждем завершения загрузки
        await download_task

        # Завершаем задачу прогресса
        await progress_queue.put((None, None))
        await progress_task

        # Проверяем, что файл существует
        if not os.path.exists(full_path):
            await progress_message.edit_text("❌ Ошибка при загрузке видео")
            return

        # Проверяем размер файла
        file_size = os.path.getsize(full_path)
        if file_size > MAX_TELEGRAM_FILE_SIZE:
            await progress_message.edit_text(
                f"⚠️ Файл слишком большой для отправки "
                f"({file_size // (1024 * 1024)}MB > {MAX_TELEGRAM_FILE_SIZE // (1024 * 1024)}MB)"
            )
            return

        # Отправляем видео пользователю
        await progress_message.edit_text("📤 Отправляю файл...")
        with open(full_path, "rb") as video_file:
            await context.bot.send_video(
                chat_id=query.message.chat_id,
                video=video_file,
                caption=video.title,
                read_timeout=60,
                write_timeout=60,
                connect_timeout=60,
            )

        await progress_message.edit_text("✅ Видео успешно отправлено!")
        logger.info(f"Видео успешно отправлено пользователю {user_id}")

    except Exception as e:
        logger.error(f"Ошибка при загрузке видео: {e}", exc_info=True)
        await progress_message.edit_text(f"❌ Произошла ошибка: {e}")
    finally:
        # Удаляем временный файл после отправки
        if video_path and full_path and os.path.exists(full_path):
            try:
                os.remove(full_path)
                logger.debug(f"Временный файл удалён: {full_path}")
            except Exception as e:
                logger.error(f"Ошибка при удалении файла: {e}")


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

async def update_progress_worker(
    message, resolution: str, queue: asyncio.Queue
) -> None:
    """
    Обновляет сообщение с прогрессом загрузки.

    Args:
        message: Сообщение Telegram для обновления
        resolution: Выбранное разрешение видео
        queue: Очередь с данными о прогрессе (current, total)
    """
    last_percent = 0
    stages = {
        0: "🔄 Загрузка видео...",
        30: "📦 Обработка файла...",
        70: "📤 Подготовка к отправке...",
        100: "✅ Файл готов к отправке!",
    }

    while True:
        current, total = await queue.get()

        # Сигнал завершения загрузки
        if current is None:
            break

        # Вычисляем процент выполнения
        percent = int((current / total) * 100)

        # Обновляем сообщение только при изменении процента
        if percent != last_percent:
            last_percent = percent
            stage = next(
                (v for k, v in stages.items() if percent >= k), stages[100]
            )
            await message.edit_text(f"{stage}\nПрогресс: {percent}%")


async def run_download(
    video, path: str, progress_queue: asyncio.Queue
) -> None:
    """
    Запускает загрузку видео в отдельном потоке.

    Args:
        video: Объект видео для загрузки
        path: Путь для сохранения файла
        progress_queue: Очередь для обновления прогресса
    """
    # Получаем event loop для использования в callback
    loop = asyncio.get_running_loop()

    def progress_callback(current: int, total: int) -> None:
        """Callback для обновления прогресса загрузки."""
        asyncio.run_coroutine_threadsafe(
            progress_queue.put((current, total)), loop
        ).result()

    # Создаем директорию, если она не существует
    download_dir = os.path.dirname(path)
    if download_dir:
        os.makedirs(download_dir, exist_ok=True)

    # Запускаем синхронную загрузку в отдельном потоке
    await asyncio.to_thread(
        video.download,
        path=os.path.dirname(path),
        workers=8,  # Количество потоков для загрузки
        progress_callback=progress_callback,
    )


# =============================================================================
# ТОЧКА ВХОДА
# =============================================================================

def main() -> None:
    """Инициализация и запуск бота."""
    # Создаем директорию для загрузок
    if not os.path.exists("downloads"):
        os.makedirs("downloads")
        logger.info("Директория downloads создана")

    # Создаем приложение
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Регистрируем обработчики
    app.add_handler(CommandHandler("start", start))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link)
    )
    app.add_handler(CallbackQueryHandler(handle_resolution))

    # Запускаем бота
    logger.info("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main()

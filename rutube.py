"""
Модуль для работы с API Rutube.

Предоставляет классы для загрузки видео с Rutube:
- Обычные видео
- Shorts (короткие видео)
- Yappy (вертикальные видео)

Поддерживает многопоточную загрузку с отображением прогресса.

Автор: maxim_vdonsk
"""

from __future__ import annotations

import abc
import enum
import json
import logging
import re
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Thread
from typing import BinaryIO, List, Optional, Text, Union

import m3u8
import requests
from alive_progress import alive_bar

# =============================================================================
# КОНСТАНТЫ
# =============================================================================

# Символы, запрещённые в именах файлов
FORBIDDEN_CHARS = ('/', '\\', ':', '*', '?', '"', '<', '>', '|')

# Таймаут между попытками (в секундах)
TIMEOUT = 50

# Максимальное количество попыток загрузки
RETRY = 5

# Шаблоны URL для API Rutube
DATA_URL_TEMPLATE = (
    r'https://rutube.ru/api/play/options/{}/?'
    r'no_404=true&referer=https%253A%252F%252Frutube.ru&pver=v2'
)
YAPPY_URL_TEMPLATE = (
    r'https://rutube.ru/pangolin/api/web/yappy/yappypage/'
    r'?client=wdp&source=shorts&videoId={}'
)

# Настройка логирования
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


# =============================================================================
# ПЕРЕЧИСЛЕНИЯ
# =============================================================================

class VideoType(enum.Enum):
    """Типы видео Rutube."""
    VIDEO = 'video'
    SHORTS = 'shorts'
    YAPPY = 'yappy'


# =============================================================================
# АБСТРАКТНЫЕ КЛАССЫ
# =============================================================================

class VideoAbstract(abc.ABC):
    """
    Базовый класс для всех типов видео.

    Определяет интерфейс для загрузки видео.
    """

    @abc.abstractproperty
    def title(self) -> str:
        """Название видео."""
        ...

    @abc.abstractproperty
    def resolution(self) -> str:
        """Разрешение видео в формате 'WIDTHxHEIGHT'."""
        ...

    @abc.abstractmethod
    def _write(
        self,
        stream: Optional[BinaryIO] = None,
        *args,
        **kwargs
    ) -> None:
        """Запись видео в поток."""
        ...

    def _build_file_path(self, path: Text = None) -> str:
        """
        Строит полный путь к файлу.

        Args:
            path: Базовая директория (опционально)

        Returns:
            Полный путь к файлу .mp4
        """
        filename = f'{self.title}.mp4'

        if not path:
            return filename

        target_path = Path(path.rstrip('/').rstrip('\\')).resolve()
        if not target_path.exists():
            target_path.mkdir(parents=True, exist_ok=True)

        return str(target_path / filename)

    def download(
        self,
        path: Optional[Text] = None,
        stream: Optional[BinaryIO] = None,
        workers: int = 0,
        progress_callback=None,
        *args,
        **kwargs
    ) -> None:
        """
        Загружает видео в файл или поток.

        Args:
            path: Путь для сохранения файла
            stream: Поток для записи
            workers: Количество потоков (0 = однопоточный)
            progress_callback: Callback для обновления прогресса
        """
        if stream:
            self._write(
                stream,
                workers=workers,
                progress_callback=progress_callback,
                *args,
                **kwargs
            )
        else:
            file_path = self._build_file_path(path)
            with open(file_path, 'wb') as file:
                self._write(
                    file,
                    workers=workers,
                    progress_callback=progress_callback,
                    *args,
                    **kwargs
                )


# =============================================================================
# RUTUBE VIDEO
# =============================================================================

class RutubeVideo(VideoAbstract):
    """
    Видео Rutube с загрузкой по сегментам.

    Загружает видео из m3u8 плейлиста, поддерживает многопоточность.
    """

    def __init__(
        self,
        playlist,
        data,
        params: dict,
        *args,
        **kwargs
    ):
        """
        Инициализация видео.

        Args:
            playlist: Плейлист с информацией о видео
            data: Данные плейлиста
            params: Параметры (video_id, title, duration)
        """
        self._id = params.get('video_id')
        self._title = params.get('title')
        self._duration = params.get('duration')
        self._base_path = playlist.uri
        self._resolution = playlist.stream_info.resolution
        self._codecs = playlist.stream_info.codecs
        self._reserve_path = None
        self._segment_urls = None

    def __str__(self) -> str:
        return f'{self._title} ({self.resolution})'

    def __repr__(self) -> str:
        return f'{self._title} ({self.resolution})'

    @property
    def title(self) -> str:
        """Название видео."""
        return str(self)

    @property
    def resolution(self) -> str:
        """Разрешение в формате 'WIDTHxHEIGHT'."""
        return 'x'.join(map(str, self._resolution))

    def _get_segment_urls(self) -> List[str]:
        """Получает URL всех сегментов из m3u8 плейлиста."""
        if self._segment_urls:
            return self._segment_urls

        r = requests.get(self._base_path)
        if r.status_code != 200:
            r = requests.get(self._reserve_path)
            if r.status_code != 200:
                raise Exception(
                    f'Cannot get segments. Status code: {r.status_code}'
                )

        data = m3u8.loads(r.text)
        self._segment_urls = [
            segment['uri'] for segment in data.data['segments']
        ]

        return self._segment_urls

    @staticmethod
    def _make_segment_uri(base_uri: str, segment_uri: str) -> str:
        """Преобразует относительный URL сегмента в полный."""
        base = base_uri[:base_uri.index(".m3u8")]
        segment = segment_uri.split("/")[-1]
        return f'{base}/{segment}'

    def _get_segment_data(self, uri: str) -> requests.Response:
        """
        Загружает сегмент с повторными попытками.

        Args:
            uri: URL сегмента

        Returns:
            Response с данными сегмента
        """
        r = None
        retry = RETRY

        while retry > 0:
            try:
                r = requests.get(uri, timeout=(10, 30))
                if r.status_code == 200:
                    return r
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout: {uri}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error: {uri} - {e}")

            retry -= 1
            time.sleep(TIMEOUT)

        raise Exception(f'Error code: {r and r.status_code}')

    def _get_segment_content(self, args: tuple) -> bytes:
        """Получает содержимое сегмента."""
        uri, bar = args
        r = (
            self._get_segment_data(
                self._make_segment_uri(self._reserve_path, uri)
            )
            or self._get_segment_data(
                self._make_segment_uri(self._base_path, uri)
            )
        )
        bar()
        return r.content

    @staticmethod
    def _write_from_deque(
        deq: deque,
        stream: BinaryIO,
        flag: list
    ) -> None:
        """Поток записи данных из очереди в файл."""
        while True:
            if deq:
                stream.write(deq.popleft())
            if flag:
                break

    def _write_threads(
        self,
        bar,
        stream: BinaryIO,
        workers: int = 0,
        progress_callback=None
    ) -> None:
        """Многопоточная запись видео."""
        deq = deque(maxlen=sys.maxsize)
        flag = []
        total_segments = len(self._get_segment_urls())
        processed_segments = 0

        writer = Thread(
            target=self._write_from_deque,
            args=(deq, stream, flag),
            daemon=True
        )
        writer.start()

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for content in pool.map(
                self._get_segment_content,
                [(uri, bar) for uri in self._get_segment_urls()]
            ):
                deq.append(content)
                processed_segments += 1

                if progress_callback:
                    progress_callback(processed_segments, total_segments)
            else:
                flag.append(True)
                writer.join()

    def _write(
        self,
        stream: BinaryIO,
        workers: int = 0,
        progress_callback=None,
        *args,
        **kwargs
    ) -> None:
        """Записывает видео в поток."""
        total_segments = len(self._get_segment_urls())
        if total_segments == 0:
            return

        with alive_bar(total_segments, title=self.title) as bar:
            if workers:
                self._write_threads(bar, stream, workers, progress_callback)
            else:
                processed_segments = 0
                for uri in self._get_segment_urls():
                    stream.write(self._get_segment_content((uri, bar)))
                    processed_segments += 1

                    if progress_callback:
                        progress_callback(processed_segments, total_segments)


# =============================================================================
# YAPPY VIDEO
# =============================================================================

class YappyVideo(VideoAbstract):
    """
    Yappy видео (вертикальные короткие видео).

    Использует прямую ссылку для загрузки.
    """

    def __init__(self, video_id: str, link: str, *args, **kwargs):
        """
        Инициализация Yappy видео.

        Args:
            video_id: ID видео
            link: Прямая ссылка на видео
        """
        self._id = video_id
        self._link = link
        self._resolution = (1920, 1080)

    def __str__(self) -> str:
        return self.title

    def __repr__(self) -> str:
        return str(self)

    @property
    def title(self) -> str:
        """ID видео как название."""
        return self._id

    @property
    def resolution(self) -> str:
        """Разрешение в формате 'WIDTHxHEIGHT'."""
        return 'x'.join(map(str, self._resolution))

    def _write(
        self,
        stream: Optional[BinaryIO] = None,
        *args,
        **kwargs
    ) -> None:
        """Загружает и записывает Yappy видео."""
        with alive_bar(2, title=self.title) as bar:
            r = requests.get(self._link)
            if r.status_code != 200:
                raise Exception(f'Error code: {r and r.status_code}')

            bar()
            stream.write(r.content)
            bar()


# =============================================================================
# ПЛЕЙЛИСТЫ
# =============================================================================

class BasePlaylist(abc.ABC):
    """Базовый класс для плейлистов."""

    _playlist: List[Union[RutubeVideo, YappyVideo]] = []

    @abc.abstractmethod
    def __init__(self, *args, **kwargs):
        ...

    def __iter__(self):
        return iter(self._playlist)

    def __repr__(self) -> str:
        return str(self._playlist)

    def __getitem__(self, i: int):
        return self._playlist[i]

    def __len__(self) -> int:
        return len(self._playlist) if self._playlist else 0

    @property
    def available_resolutions(self) -> List[Text]:
        """Список доступных разрешений."""
        return [v._resolution[-1] for v in self._playlist]

    def get_best(self) -> Union[RutubeVideo, YappyVideo, None]:
        """Видео с лучшим качеством."""
        if self._playlist:
            return self._playlist[-1]
        return None

    def get_worst(self) -> Union[RutubeVideo, YappyVideo, None]:
        """Видео с худшим качеством."""
        if self._playlist:
            return self._playlist[0]
        return None

    def get_by_resolution(
        self, value: int
    ) -> Union[RutubeVideo, YappyVideo, None]:
        """
        Поиск видео по разрешению.

        Args:
            value: Высота кадра (например, 1080)

        Returns:
            Объект видео или None
        """
        value = int(value)
        if self._playlist:
            for video in reversed(self._playlist):
                if video._resolution[-1] == value:
                    return video
        return None


class RutubePlaylist(BasePlaylist):
    """Плейлист обычных видео Rutube."""

    def __init__(self, data, params: dict, *args, **kwargs):
        """
        Создание плейлиста из данных API.

        Args:
            data: Данные плейлиста
            params: Параметры видео
        """
        _playlist_dict = {}

        for playlist in data.playlists:
            res = playlist.stream_info.resolution

            if res in _playlist_dict:
                _playlist_dict[res]._reserve_path = playlist.uri
            else:
                _playlist_dict[res] = RutubeVideo(playlist, data, params)

        self._playlist: List[RutubeVideo] = list(_playlist_dict.values())


class YappyPlaylist(BasePlaylist):
    """Плейлист Yappy видео."""

    def __init__(self, video_id: str, *args, **kwargs):
        """
        Создание плейлиста с одним Yappy видео.

        Args:
            video_id: ID видео
        """
        self._video_id = video_id
        self._playlist = [
            YappyVideo(self._video_id, self._get_video_link())
        ]

    def _get_videos(self) -> list:
        """Получение списка видео из API."""
        r = requests.get(YAPPY_URL_TEMPLATE.format(self._video_id))
        if r.status_code != 200:
            raise Exception(f'Error code: {r and r.status_code}')

        results: list = r.json().get('results')
        if not results:
            raise Exception('No results found')

        return results

    def _get_video_link(self) -> str:
        """Прямая ссылка на видео."""
        return self._get_videos()[0].get('link')


# =============================================================================
# RUTUBE - ОСНОВНОЙ КЛАСС
# =============================================================================

class Rutube:
    """
    Основной класс для работы с Rutube.

    Автоматически определяет тип видео и предоставляет интерфейс для загрузки.

    Пример:
        ru = Rutube("https://rutube.ru/shorts/abc123/")
        video = ru.get_by_resolution(1080)
        video.download(path="./downloads")
    """

    def __init__(self, video_url: str, *args, **kwargs):
        """
        Инициализация Rutube.

        Args:
            video_url: URL видео на Rutube
        """
        self._video_url = video_url
        self._playlist: Union[RutubePlaylist, YappyPlaylist, None] = None
        self._type = VideoType.VIDEO

        if self._check_url():
            if f'/{VideoType.SHORTS.value}/' in self._video_url:
                self._type = VideoType.SHORTS
            elif f'/{VideoType.YAPPY.value}/' in self._video_url:
                self._type = VideoType.YAPPY

            if self._type == VideoType.YAPPY:
                self._video_id = self._get_video_id()
            else:
                self._video_id = self._get_video_id()
                self._data_url = self._get_data_url()
                self._data = self._get_data()
                self._m3u8_url = self._get_m3u8_url()
                self._m3u8_data = self._get_m3u8_data()
                self._title = self._get_title()

    def __len__(self) -> int:
        """Количество доступных версий видео."""
        return len(self.playlist) if self.playlist else 0

    @property
    def is_video(self) -> bool:
        """Обычное ли видео."""
        return self._type == VideoType.VIDEO

    @property
    def is_shorts(self) -> bool:
        """Является ли Shorts."""
        return self._type == VideoType.SHORTS

    @property
    def is_yappy(self) -> bool:
        """Является ли Yappy."""
        return self._type == VideoType.YAPPY

    @property
    def playlist(self) -> Union[RutubePlaylist, YappyPlaylist, None]:
        """Плейлист с версиями видео."""
        if not self._playlist:
            self._playlist = self._get_playlist()
        return self._playlist

    @property
    def available_resolutions(self) -> List[Text]:
        """Список доступных разрешений."""
        return self.playlist.available_resolutions if self.playlist else []

    def get_best(self) -> Union[RutubeVideo, YappyVideo, None]:
        """Видео с лучшим качеством."""
        if self.playlist:
            return self._playlist.get_best()
        return None

    def get_worst(self) -> Union[RutubeVideo, YappyVideo, None]:
        """Видео с худшим качеством."""
        if self.playlist:
            return self.playlist.get_worst()
        return None

    def get_by_resolution(
        self, value: int
    ) -> Union[RutubeVideo, YappyVideo, None]:
        """
        Поиск видео по разрешению.

        Args:
            value: Высота кадра (1080, 720, etc.)

        Returns:
            Объект видео или None
        """
        if self.playlist:
            return self.playlist.get_by_resolution(value)
        return None

    # -------------------------------------------------------------------------
    # Приватные методы
    # -------------------------------------------------------------------------

    def _get_data_url(self) -> str:
        """URL для получения данных о видео."""
        return DATA_URL_TEMPLATE.format(self._video_id)

    @property
    def _params(self) -> dict:
        """Параметры видео."""
        return dict(
            video_id=self._video_id,
            video_url=self._video_url,
            title=self._title,
            duration=self._duration,
        )

    def _get_video_id(self) -> str:
        """Извлечение ID видео из URL."""
        pattern = rf'{self._type.value}\/([(\w+\d+)+]+)'
        result = re.findall(pattern, self._video_url)

        if not result:
            raise Exception('Cannot get the video ID from URL')

        return result[0]

    def _get_data(self) -> dict:
        """Получение данных из API."""
        r = requests.get(self._data_url)
        return json.loads(r.content)

    def _check_url(self) -> bool:
        """Проверка доступности видео."""
        if requests.get(self._video_url).status_code != 200:
            raise Exception(f'{self._video_url} is unavailable')
        return True

    def _get_title(self) -> str:
        """Получение и очистка названия."""
        return self._clean_title(
            self._data.get('title')
        ) or self._video_id

    @staticmethod
    def _clean_title(title: str) -> str:
        """Удаление запрещённых символов из названия."""
        if not title:
            return title

        return ''.join(
            filter(lambda x: x not in FORBIDDEN_CHARS, title)
        )

    def _get_playlist(
        self
    ) -> Union[RutubePlaylist, YappyPlaylist]:
        """Создание плейлиста по типу видео."""
        if self._type == VideoType.YAPPY:
            return YappyPlaylist(self._video_id)
        return RutubePlaylist(self._m3u8_data, self._params)

    def _get_m3u8_url(self) -> str:
        """Получение URL m3u8 плейлиста."""
        try:
            return self._data['video_balancer']['m3u8']
        except KeyError:
            logger.error('video_balancer not found in API response')
            logger.debug(json.dumps(self._data, indent=2, ensure_ascii=False))
            raise

    def _get_m3u8_data(self) -> m3u8.M3U8:
        """Загрузка и парсинг m3u8 плейлиста."""
        r = requests.get(self._m3u8_url)
        return m3u8.loads(r.text)

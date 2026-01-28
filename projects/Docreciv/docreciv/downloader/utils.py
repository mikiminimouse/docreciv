"""
Utility functions for the Downloader Microservice.
Includes environment loading, filename sanitization, MongoDB connection, VPN checking.
"""
import os
import time
import threading
from pathlib import Path
from typing import Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pymongo import MongoClient


# Global session for requests
_session: Optional[requests.Session] = None
_session_lock = threading.Lock()

# Browser headers
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
}


def load_env_file():
    """Загружает переменные окружения из .env файла если он существует."""
    env_file = Path(__file__).parent.parent.parent.parent / ".env"  # Теперь на уровень выше
    if env_file.exists():
        try:
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        value = value.strip('"').strip("'")
                        os.environ[key] = value
        except Exception as e:
            print(f"⚠️  Не удалось загрузить .env файл: {e}")


def sanitize_filename(filename: str, max_length: int = 200) -> str:
    """
    Sanitizes filename by removing dangerous characters and limiting length.

    Args:
        filename: Исходное имя файла
        max_length: Максимальная длина имени (по умолчанию 200, оставляем запас до 255)

    Returns:
        Sanitized имя файла с безопасной длиной

    Note:
        - Linux ограничивает имя файла до 255 байт
        - Используем max_length=200 для запаса (учитывая UTF-8)
        - Сохраняет расширение файла при обрезке
    """
    # Заменяем опасные символы
    filename = filename.replace('..', '').replace('/', '_').replace('\\', '_')
    dangerous_chars = ['<', '>', ':', '"', '|', '?', '*']
    for char in dangerous_chars:
        filename = filename.replace(char, '_')

    # Разделяем имя и расширение
    name_parts = filename.rsplit('.', 1)
    if len(name_parts) == 2 and len(name_parts[1]) <= 10:  # Проверяем, что это расширение
        base_name, extension = name_parts
        extension = '.' + extension
    else:
        base_name = filename
        extension = ''

    # Обрезаем базовое имя если слишком длинное
    # Учитываем, что в UTF-8 русские буквы занимают 2+ байта
    base_name = base_name[:max_length]

    # Дополнительно обрезаем по байтам, чтобы не превысить 255 байт total
    full_name = base_name + extension
    while len(full_name.encode('utf-8')) > 255:
        base_name = base_name[:-1]
        full_name = base_name + extension

    return full_name


def get_metadata_client() -> Optional[MongoClient]:
    """Создает клиент для локальной Mongo с метаданными."""
    server = os.environ.get("MONGO_METADATA_SERVER", "localhost:27017")
    user = os.environ.get("MONGO_METADATA_USER", "admin")
    password = os.environ.get("MONGO_METADATA_PASSWORD", "password")
    
    url = f"mongodb://{user}:{password}@{server}/?authSource=admin"

    try:
        client = MongoClient(
            url,
            serverSelectionTimeoutMS=10_000,
            connectTimeoutMS=10_000,
            socketTimeoutMS=10_000,
        )
        client.admin.command("ping")
        return client
    except Exception as e:
        print(f"❌ Ошибка подключения к MongoDB: {e}")
        return None


def get_session() -> requests.Session:
    """Gets or creates a global requests session with retry mechanism."""
    global _session
    
    if _session is None:
        with _session_lock:
            if _session is None:
                _session = requests.Session()
                retry_strategy = Retry(
                    total=3,
                    backoff_factor=0.5,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET", "POST", "HEAD"]
                )
                adapter = HTTPAdapter(max_retries=retry_strategy)
                _session.mount("http://", adapter)
                _session.mount("https://", adapter)
                _session.headers.update(BROWSER_HEADERS)
    
    return _session


def reset_session():
    """Resets the global session."""
    global _session
    with _session_lock:
        if _session:
            _session.close()
        _session = None


def check_zakupki_health() -> bool:
    """
    Проверяет доступность zakupki.gov.ru через VPN.
    
    Returns:
        True если сервис доступен, False если недоступен
    """
    try:
        session = get_session()
        response = session.get(
            "https://www.zakupki.gov.ru",
            timeout=10,
            allow_redirects=False
        )
        return response.status_code in [200, 301, 302]
    except Exception:
        return False


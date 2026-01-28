"""
Утилиты для CLI приложения.
"""

import hashlib
from pathlib import Path


def calculate_sha256(file_path: Path) -> str:
    """Вычисляет SHA256 хэш файла."""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def sanitize_filename(filename: str) -> str:
    """Очищает имя файла от опасных символов."""
    import os
    filename = filename.replace("../", "").replace("..\\", "")
    filename = os.path.basename(filename)
    dangerous = ['<', '>', ':', '"', '|', '?', '*', '\x00']
    for char in dangerous:
        filename = filename.replace(char, '_')
    return filename

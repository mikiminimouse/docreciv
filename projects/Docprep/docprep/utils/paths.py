"""
Утилиты для работы с путями и директориями UNIT.
"""
from pathlib import Path
from typing import List, Optional, Set


def find_units(directory: Path, pattern: str = "UNIT_*") -> List[Path]:
    """
    Находит все директории UNIT в указанной директории.

    Args:
        directory: Директория для поиска
        pattern: Шаблон поиска (по умолчанию "UNIT_*")

    Returns:
        Список путей к директориям UNIT
    """
    if not directory.exists():
        return []

    units = [d for d in directory.iterdir() if d.is_dir() and d.name.startswith("UNIT_")]
    return sorted(units)


def find_all_units(directory: Path, recursive: bool = True) -> List[Path]:
    """
    Рекурсивно находит все директории UNIT в указанной директории.

    Args:
        directory: Директория для поиска
        recursive: Если True, выполняет рекурсивный поиск

    Returns:
        Список путей к директориям UNIT
    """
    if not directory.exists():
        return []

    units: List[Path] = []
    seen_unit_ids: Set[str] = set()

    if recursive:
        # Рекурсивный поиск через rglob
        for item in directory.rglob("UNIT_*"):
            if item.is_dir() and item.name.startswith("UNIT_"):
                unit_id = item.name
                # Избегаем дубликатов (если UNIT найден на разных уровнях)
                if unit_id not in seen_unit_ids:
                    # Проверяем, что это не вложенный UNIT внутри другого UNIT
                    # (например, UNIT_xxx/files/UNIT_yyy)
                    parent_has_unit = any(
                        p.name.startswith("UNIT_") for p in item.parents if p != directory
                    )
                    if not parent_has_unit:
                        units.append(item)
                        seen_unit_ids.add(unit_id)
    else:
        # Не рекурсивный поиск
        units = find_units(directory)

    return sorted(units)


def get_unit_files(unit_path: Path) -> List[Path]:
    """
    Получает список всех файлов UNIT.

    Ищет файлы рекурсивно во всех поддиректориях UNIT:
    1. unit_path/files/ (если существует) - рекурсивно
    2. unit_path/ (все поддиректории) - рекурсивно
    3. Исключает служебные файлы: manifest.json, audit.log.jsonl, metadata.json

    Args:
        unit_path: Путь к директории UNIT

    Returns:
        Список путей к файлам UNIT
    """
    if not unit_path.exists():
        return []

    files: List[Path] = []
    # Служебные файлы, которые не должны учитываться при классификации
    excluded_files = {
        "manifest.json",
        "audit.log.jsonl",
        "metadata.json",
        "raw_url_map.json",  # Служебный файл с URL маппингом
        "unit.meta.json",    # Служебный файл с метаданными UNIT
    }
    excluded_dirs = {".git", "__pycache__", ".pytest_cache"}

    # Рекурсивный поиск всех файлов в UNIT директории
    # Используем rglob для поиска во всех поддиректориях
    for file_path in unit_path.rglob("*"):
        # Пропускаем директории
        if not file_path.is_file():
            continue
        
        # Пропускаем служебные файлы
        if file_path.name in excluded_files:
            continue
        
        # Пропускаем файлы в исключенных директориях
        if any(excluded_dir in file_path.parts for excluded_dir in excluded_dirs):
            continue
        
        # Пропускаем скрытые файлы и временные файлы
        if file_path.name.startswith(".") or file_path.name.startswith("~"):
            continue
        
        files.append(file_path)

    return sorted(files)


def get_unit_path(base_dir: Path, unit_id: str) -> Path:
    """
    Получает путь к директории UNIT.

    Args:
        base_dir: Базовая директория
        unit_id: Идентификатор UNIT

    Returns:
        Путь к директории UNIT
    """
    return base_dir / unit_id


def ensure_directory(path: Path) -> Path:
    """
    Создает директорию если она не существует.

    Args:
        path: Путь к директории

    Returns:
        Путь к директории
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_unit_structure(unit_path: Path) -> Path:
    """
    Создает базовую структуру директорий для UNIT.

    Создает:
    - unit_path/ (основная директория)
    - unit_path/files/ (директория для файлов, опционально)

    Args:
        unit_path: Путь к директории UNIT

    Returns:
        Путь к директории UNIT
    """
    unit_path.mkdir(parents=True, exist_ok=True)
    # Директория files создается по необходимости
    return unit_path


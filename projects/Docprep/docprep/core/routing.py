"""
Unified Route Registry — единая точка правды для маппингов типов файлов.

Консолидирует все маппинги extension → route, category, target_dir,
которые ранее были размазаны по manifest.py, classifier.py, merger.py, contract.py.
"""
from typing import Dict, Optional, Tuple, Set
from dataclasses import dataclass


@dataclass(frozen=True)
class RouteConfig:
    """Конфигурация маршрута для типа файла."""
    route: str           # pdf_text, pdf_scan, docx, xlsx, convert, extract, etc.
    category: str        # direct, convert, extract, normalize, special
    target_subdir: str   # Поддиректория в Ready2Docling
    needs_ocr: bool = False
    is_archive: bool = False


# === ЕДИНЫЙ РЕЕСТР МАРШРУТОВ ===
ROUTE_REGISTRY: Dict[str, RouteConfig] = {
    # --- DIRECT: Готовы к Docling без обработки ---
    "pdf": RouteConfig("pdf_text", "direct", "pdf/text"),
    "docx": RouteConfig("docx", "direct", "docx"),
    "xlsx": RouteConfig("xlsx", "direct", "xlsx"),
    "pptx": RouteConfig("pptx", "direct", "pptx"),
    "html": RouteConfig("html", "direct", "html"),
    "xml": RouteConfig("xml", "direct", "xml"),
    "md": RouteConfig("md", "direct", "md"),
    
    # --- DIRECT с OCR (сканы) ---
    "jpg": RouteConfig("image_ocr", "direct", "jpg", needs_ocr=True),
    "jpeg": RouteConfig("image_ocr", "direct", "jpeg", needs_ocr=True),
    "png": RouteConfig("image_ocr", "direct", "png", needs_ocr=True),
    "tiff": RouteConfig("image_ocr", "direct", "tiff", needs_ocr=True),
    "tif": RouteConfig("image_ocr", "direct", "tiff", needs_ocr=True),
    "bmp": RouteConfig("image_ocr", "direct", "bmp", needs_ocr=True),
    
    # --- CONVERT: Требуют конвертации в PDF/DOCX ---
    "doc": RouteConfig("convert", "convert", "doc"),
    "xls": RouteConfig("convert", "convert", "xls"),
    "ppt": RouteConfig("convert", "convert", "ppt"),
    "rtf": RouteConfig("convert", "convert", "rtf"),
    "odt": RouteConfig("convert", "convert", "odt"),
    "ods": RouteConfig("convert", "convert", "ods"),
    "odp": RouteConfig("convert", "convert", "odp"),
    
    # --- EXTRACT: Архивы ---
    "zip": RouteConfig("extract", "extract", "zip", is_archive=True),
    "rar": RouteConfig("extract", "extract", "rar", is_archive=True),
    "7z": RouteConfig("extract", "extract", "7z", is_archive=True),
    "tar": RouteConfig("extract", "extract", "tar", is_archive=True),
    "gz": RouteConfig("extract", "extract", "gz", is_archive=True),
    
    # --- NORMALIZE: Требуют нормализации имени/расширения ---
    # (определяется динамически по detected_type != original_extension)
}

# Расширения, неподдерживаемые Docling (отправляются в Exceptions)
UNSUPPORTED_EXTENSIONS: Set[str] = {
    "exe", "dll", "bin", "msi", "bat", "cmd", "sh",
    "mp3", "mp4", "avi", "mov", "wav", "flac",
    "iso", "dmg", "img",
}

# Типы архивов (для проверки в Merger)
ARCHIVE_TYPES: Set[str] = {"zip", "rar", "7z", "tar", "gz", "zip_archive", "rar_archive", "7z_archive"}


def get_route_config(detected_type: str) -> Optional[RouteConfig]:
    """
    Получает конфигурацию маршрута для типа файла.
    
    Args:
        detected_type: Определенный тип файла (pdf, docx, doc, zip, etc.)
    
    Returns:
        RouteConfig или None если тип неизвестен
    """
    # Нормализуем тип (убираем _archive суффиксы)
    normalized = detected_type.lower().replace("_archive", "")
    return ROUTE_REGISTRY.get(normalized)


def determine_route(detected_type: str) -> str:
    """
    Определяет route для типа файла.
    
    Args:
        detected_type: Определенный тип файла
    
    Returns:
        Строка route (pdf_text, convert, extract, unknown)
    """
    config = get_route_config(detected_type)
    return config.route if config else "unknown"


def determine_category(detected_type: str) -> str:
    """
    Определяет категорию обработки для типа файла.
    
    Args:
        detected_type: Определенный тип файла
    
    Returns:
        Категория (direct, convert, extract, normalize, unknown)
    """
    config = get_route_config(detected_type)
    return config.category if config else "unknown"


def get_target_subdir(detected_type: str) -> str:
    """
    Определяет целевую поддиректорию в Ready2Docling.
    
    Args:
        detected_type: Определенный тип файла
    
    Returns:
        Путь поддиректории (pdf/text, docx, jpg, etc.) или "other"
    """
    config = get_route_config(detected_type)
    return config.target_subdir if config else "other"


def is_supported_extension(extension: str) -> bool:
    """Проверяет, поддерживается ли расширение."""
    ext = extension.lower().lstrip(".")
    return ext not in UNSUPPORTED_EXTENSIONS


# УДАЛЕНО: is_archive_type() и needs_ocr() — неиспользуемые функции
# Функциональность доступна через RouteConfig.is_archive и RouteConfig.needs_ocr


def determine_route_from_files(files: list) -> str:
    """
    Определяет route для UNIT на основе списка файлов.
    
    Если все файлы одного типа — возвращает route этого типа.
    Если файлы разных типов — возвращает "mixed".
    
    Args:
        files: Список словарей с информацией о файлах (detected_type)
    
    Returns:
        route строка
    """
    if not files:
        return "unknown"
    
    # Реестр приоритетов для выбора основного маршрута в случае mixed
    # Чем выше, тем приоритетнее для Docling (сканы и таблицы требуют OCR/сложных моделей)
    ROUTE_PRIORITY = [
        "pdf_scan_table",
        "pdf_scan",
        "pdf_text",
        "docx",
        "xlsx",
        "pptx",
        "rtf",
        "html",
        "xml",
        "image_ocr",
    ]
    
    found_routes = set()
    for f in files:
        detected_type = f.get("detected_type", "unknown")
        route = determine_route(detected_type)

        # Для PDF учитываем needs_ocr для определения scan vs text
        if detected_type == "pdf":
            needs_ocr = f.get("needs_ocr", False)
            if needs_ocr:
                route = "pdf_scan"
            else:
                route = "pdf_text"

        if route != "unknown":
            found_routes.add(route)
    
    if not found_routes:
        return "unknown"
    
    if len(found_routes) == 1:
        return list(found_routes)[0]

    # ИСПРАВЛЕНО: Если несколько разных маршрутов - это Mixed UNIT
    # Раньше выбирался приоритетный маршрут, что приводило к потере Mixed статуса
    if len(found_routes) > 1:
        return "mixed"

    # Если ничего не подошло (что странно), берем первый попавшийся
    return list(found_routes)[0]

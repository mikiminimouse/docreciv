"""
Document Contract - контрактный слой между DocPrep и Docling.

docprep.contract.json определяет формализованный вход для Docling,
гарантируя контрактную строгость и однозначность обработки.

ВАЖНО: Docling использует ТОЛЬКО contract, manifest.json НЕ используется.
"""
import json
import hashlib
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from collections import Counter

from .manifest import load_manifest


def calculate_file_checksum(file_path: Path) -> str:
    """
    Вычисляет SHA256 хеш файла.
    
    Args:
        file_path: Путь к файлу
        
    Returns:
        SHA256 хеш в виде hex строки
    """
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception:
        return ""


def _document_type_from_route(route: str, file_extension: str = "") -> str:
    """
    Определяет document_type на основе route или расширения файла.

    Args:
        route: Маршрут обработки (pdf_text, pdf_scan, xml, etc.)
        file_extension: Расширение файла (без точки)

    Returns:
        Тип документа (pdf, docx, xlsx, xml, etc.)
    """
    # Маппинг route -> document_type
    route_to_type = {
        "pdf_text": "pdf",
        "pdf_scan": "pdf",
        "pdf_scan_table": "pdf",
        "docx": "docx",
        "xlsx": "xlsx",
        "pptx": "pptx",
        "html": "html",
        "html_text": "html",
        "xml": "xml",
        "rtf": "rtf",
        "image_ocr": "image",
        "mixed": "mixed",  # Mixed UNIT содержат документы разных типов
    }

    # Сначала пробуем определить из route
    if route in route_to_type:
        return route_to_type[route]

    # Fallback на расширение файла
    ext = file_extension.lower().lstrip(".")
    if ext in ["pdf"]:
        return "pdf"
    elif ext in ["docx", "doc"]:
        return "docx"
    elif ext in ["xlsx", "xls"]:
        return "xlsx"
    elif ext in ["pptx", "ppt"]:
        return "pptx"
    elif ext in ["html", "htm"]:
        return "html"
    elif ext == "xml":
        return "xml"
    elif ext == "rtf":
        return "rtf"
    elif ext in ["jpg", "jpeg"]:
        return "jpeg"
    elif ext == "png":
        return "png"
    elif ext in ["tiff", "tif"]:
        return "tiff"

    # Если ничего не определили, возвращаем "unknown"
    return "unknown"


def estimate_processing_cost(route: str, page_count: int = 1, file_size_bytes: int = 0) -> Dict[str, Any]:
    """
    Оценивает стоимость обработки документа на основе route.
    
    Args:
        route: Маршрут обработки (pdf_text, pdf_scan, pdf_scan_table, etc.)
        page_count: Количество страниц
        file_size_bytes: Размер файла в байтах
        
    Returns:
        Словарь с оценкой стоимости:
        - cpu_seconds_estimate: примерное время обработки в секундах
        - cost_usd_estimate: примерная стоимость в USD
    """
    # Базовые оценки производительности по маршрутам (CPU-only)
    # Основано на реальных измерениях производительности
    route_estimates = {
        "pdf_text": {
            "seconds_per_page": 1.5,  # ~40-70 PDF/hour
            "base_seconds": 5,
        },
        "pdf_scan": {
            "seconds_per_page": 8.0,  # ~10-20 PDF/hour
            "base_seconds": 10,
        },
        "pdf_scan_table": {
            "seconds_per_page": 20.0,  # ~4-8 PDF/hour (самый тяжелый)
            "base_seconds": 30,
        },
        "image_ocr": {
            "seconds_per_image": 12.0,  # ~60-100 images/hour
            "base_seconds": 5,
        },
        "docx": {
            "seconds_per_doc": 3.0,
            "base_seconds": 2,
        },
        "xlsx": {
            "seconds_per_doc": 5.0,
            "base_seconds": 3,
        },
        "pptx": {
            "seconds_per_doc": 4.0,
            "base_seconds": 2,
        },
        "html": {
            "seconds_per_doc": 2.0,
            "base_seconds": 1,
        },
        "xml": {
            "seconds_per_doc": 2.0,
            "base_seconds": 1,
        },
        "rtf": {
            "seconds_per_doc": 3.0,
            "base_seconds": 2,
        },
    }
    
    estimate = route_estimates.get(route, {"seconds_per_page": 5.0, "base_seconds": 5})
    
    # Вычисляем время обработки
    if route == "image_ocr":
        cpu_seconds = estimate.get("base_seconds", 5) + (estimate.get("seconds_per_image", 12.0) * max(1, page_count))
    elif route in ["docx", "xlsx", "pptx", "html", "xml", "rtf"]:
        cpu_seconds = estimate.get("base_seconds", 2) + estimate.get("seconds_per_doc", 3.0)
    else:
        cpu_seconds = estimate.get("base_seconds", 5) + (estimate.get("seconds_per_page", 5.0) * max(1, page_count))
    
    # Оценка стоимости (примерная: $0.05 за CPU-hour для облачных инстансов)
    # Это приблизительная оценка, реальная стоимость зависит от инфраструктуры
    cost_per_cpu_hour = 0.05
    cost_usd = (cpu_seconds / 3600.0) * cost_per_cpu_hour
    
    return {
        "cpu_seconds_estimate": int(cpu_seconds),
        "cost_usd_estimate": round(cost_usd, 6),
    }


def generate_contract_from_manifest(
    unit_path: Path,
    manifest: Optional[Dict[str, Any]] = None,
    main_file_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Генерирует docprep.contract.json на основе manifest.json.
    
    Contract - это формализованный вход для Docling, обеспечивающий
    контрактную строгость и однозначность обработки.
    
    ВАЖНО: Docling использует ТОЛЬКО contract.json, manifest.json НЕ используется как вход.
    
    Args:
        unit_path: Путь к директории UNIT в Ready2Docling
        manifest: Manifest данные (опционально, если None - загружается из unit_path)
        main_file_path: Путь к главному файлу для обработки (опционально)
        
    Returns:
        Словарь с контрактом документа
        
    Raises:
        FileNotFoundError: Если manifest.json не найден
        ValueError: Если route не определен или равен "mixed"
    """
    # Загружаем manifest если не передан
    if manifest is None:
        manifest = load_manifest(unit_path)
    
    unit_id = manifest.get("unit_id", unit_path.name)
    protocol_id = manifest.get("protocol_id", "")
    protocol_date = manifest.get("protocol_date", "")
    
    # Получаем route из manifest
    processing = manifest.get("processing", {})
    route = processing.get("route", "")
    
    # Если route="mixed" или отсутствует, определяем из файлов
    if route == "mixed" or not route:
        logger = logging.getLogger(__name__)
        logger.warning(
            f"Route is 'mixed' or missing in manifest for unit {unit_id}. "
            f"Determining route from files and directory structure..."
        )
        
        # Используем функцию определения route из файлов из manifest.py
        from .manifest import _determine_route_from_files
        route = _determine_route_from_files(manifest.get("files", []))
        
        if route == "mixed" or not route:
            # Если все еще mixed, пытаемся определить из структуры директорий и файлов
            path_str = str(unit_path)
            
            # Проверяем путь
            if "/pdf/text/" in path_str or "/pdf/text" in path_str:
                route = "pdf_text"
            elif "/pdf/scan/" in path_str or "/pdf/scan" in path_str:
                route = "pdf_scan"
            elif "/docx/" in path_str:
                route = "docx"
            elif "/xlsx/" in path_str:
                route = "xlsx"
            elif "/pptx/" in path_str:
                route = "pptx"
            elif "/html/" in path_str:
                route = "html"
            elif "/xml/" in path_str:
                route = "xml"
            elif "/jpg/" in path_str or "/jpeg/" in path_str or "/png/" in path_str or "/tiff/" in path_str:
                route = "image_ocr"
            elif "/rtf/" in path_str:
                route = "rtf"
            
            # Если не определили по пути, определяем по расширению главного файла
            if (route == "mixed" or not route) and main_file_path is None:
                files_dir = unit_path / "files"
                if not files_dir.exists():
                    files_dir = unit_path
                
                all_files = [
                    f for f in files_dir.rglob("*")
                    if f.is_file() and f.name not in ["manifest.json", "audit.log.jsonl", "docprep.contract.json", "raw_url_map.json", "unit.meta.json"]
                ]
                
                if all_files:
                    main_file_ext = all_files[0].suffix.lower()
                    if main_file_ext == ".pdf":
                        route = "pdf_text"  # По умолчанию pdf_text, можно улучшить детекцию scan
                    elif main_file_ext == ".docx":
                        route = "docx"
                    elif main_file_ext == ".xlsx":
                        route = "xlsx"
                    elif main_file_ext == ".pptx":
                        route = "pptx"
                    elif main_file_ext in [".html", ".htm"]:
                        route = "html"
                    elif main_file_ext == ".xml":
                        route = "xml"
                    elif main_file_ext in [".jpg", ".jpeg", ".png", ".tiff"]:
                        route = "image_ocr"
                    elif main_file_ext == ".rtf":
                        route = "rtf"
            
            # Mixed UNIT допустимы — они содержат документы разных типов
            # Оставляем route="mixed" и продолжаем генерацию контракта
            if not route:
                raise ValueError(
                    f"Cannot determine route for unit {unit_id}. "
                    f"Route is empty and cannot be determined from files or path."
                )
        
        logger.info(f"Determined route '{route}' for unit {unit_id} from files/path")
    
    # Финальная валидация: допускаем "mixed"
    if not route:
        raise ValueError(
            f"Route cannot be empty for unit {unit_id}."
        )
    
    # Определяем главный файл если не передан
    if main_file_path is None:
        files_dir = unit_path / "files"
        if not files_dir.exists():
            files_dir = unit_path
        
        # Ищем файлы документа (не служебные)
        all_files = [
            f for f in files_dir.rglob("*")
            if f.is_file() and f.name not in ["manifest.json", "audit.log.jsonl", "docprep.contract.json"]
        ]
        
        # Определяем главный файл по типу документа
        main_file = None
        for file_path in all_files:
            ext = file_path.suffix.lower()
            # Приоритет файлам документов по route
            if route.startswith("pdf") and ext == ".pdf":
                main_file = file_path
                break
            elif route == "docx" and ext == ".docx":
                main_file = file_path
                break
            elif route == "xlsx" and ext == ".xlsx":
                main_file = file_path
                break
            elif route == "pptx" and ext == ".pptx":
                main_file = file_path
                break
            elif route in ["html", "html_text"] and ext in [".html", ".htm"]:
                main_file = file_path
                break
            elif route == "xml" and ext == ".xml":
                main_file = file_path
                break
            elif route == "image_ocr" and ext in [".jpg", ".jpeg", ".png", ".tiff"]:
                main_file = file_path
                break
            elif route == "rtf" and ext == ".rtf":
                main_file = file_path
                break
        
        # Fallback: первый файл
        if main_file is None and all_files:
            main_file = all_files[0]
        
        if main_file is None:
            raise ValueError(f"No files found in unit {unit_id}")
        
        main_file_path = main_file
    
    # Получаем информацию о файле из manifest
    manifest_files = manifest.get("files", [])
    file_info = None
    for mf in manifest_files:
        current_name = mf.get("current_name", "")
        original_name = mf.get("original_name", "")
        if main_file_path.name in [current_name, original_name]:
            file_info = mf
            break
    
    # Извлекаем метаданные файла
    original_filename = file_info.get("original_name", main_file_path.name) if file_info else main_file_path.name
    detected_mime = file_info.get("mime_detected", "") if file_info else ""
    pages_or_parts = file_info.get("pages_or_parts", 1) if file_info else 1
    
    # Вычисляем размер и checksum
    file_size = main_file_path.stat().st_size if main_file_path.exists() else 0
    checksum = calculate_file_checksum(main_file_path) if main_file_path.exists() else ""
    
    # Определяем document_profile
    detected_type = file_info.get("detected_type", "").lower() if file_info else ""
    needs_ocr = file_info.get("needs_ocr", False) if file_info else False
    
    # Определяем content_type для PDF
    content_type = "text"
    if route.startswith("pdf"):
        content_type = "scan" if needs_ocr or route == "pdf_scan" else "text"
        if route == "pdf_scan_table":
            content_type = "scan_table"
    elif route == "image_ocr":
        content_type = "image"
    
    # Определяем наличие таблиц и форм (на основе route или детекции)
    has_tables = route in ["pdf_scan_table", "xlsx"]
    has_forms = False  # Можно расширить детекцию форм
    
    # Оцениваем стоимость обработки
    cost_estimation = estimate_processing_cost(
        route=route,
        page_count=pages_or_parts,
        file_size_bytes=file_size
    )
    
    # Получаем историю трансформаций
    transformations = file_info.get("transformations", []) if file_info else []
    transformation_history = []
    for trans in transformations:
        transformation_history.append({
            "type": trans.get("type"),
            "timestamp": trans.get("timestamp"),
        })
    
    # Генерируем correlation_id если его нет
    correlation_id = manifest.get("correlation_id")
    if not correlation_id:
        import uuid
        correlation_id = str(uuid.uuid4())
    
    # Формируем контракт
    contract = {
        "contract_version": "1.0",
        "unit": {
            "unit_id": unit_id,
            "batch_date": protocol_date,
            "state": "READY_FOR_DOCLING",
            "correlation_id": correlation_id,
        },
        "source": {
            "original_filename": original_filename,
            "detected_mime": detected_mime,
            "true_extension": main_file_path.suffix.lower().lstrip("."),
            "size_bytes": file_size,
            "checksum_sha256": checksum,
        },
        "document_profile": {
            "document_type": detected_type if detected_type else _document_type_from_route(route, main_file_path.suffix),
            "content_type": content_type,
            "language_hint": ["ru"],  # Можно расширить детекцию языка
            "page_count": pages_or_parts,
            "needs_ocr": needs_ocr,
            "has_tables": has_tables,
            "has_forms": has_forms,
            "quality_score": 0.8,  # Можно расширить детекцию качества
        },
        "routing": {
            "docling_route": route,
            "priority": "normal",
            "pipeline_version": "2025-01",
        },
        "processing_constraints": {
            "allow_gpu": False,  # CPU-only inference
            "max_runtime_sec": 180,
            "max_memory_mb": 4096,
        },
        "history": {
            "docprep_cycles": processing.get("current_cycle", 1),
            "transformations": transformation_history,
        },
        "cost_estimation": cost_estimation,
    }
    
    return contract


def save_contract(unit_path: Path, contract: Dict[str, Any]) -> Path:
    """
    Сохраняет docprep.contract.json в директорию UNIT.
    
    Args:
        unit_path: Путь к директории UNIT
        contract: Словарь с контрактом
        
    Returns:
        Путь к сохраненному файлу контракта
    """
    contract_path = unit_path / "docprep.contract.json"
    
    with open(contract_path, "w", encoding="utf-8") as f:
        json.dump(contract, f, indent=2, ensure_ascii=False)
    
    return contract_path


def load_contract(unit_path: Path) -> Dict[str, Any]:
    """
    Загружает docprep.contract.json из директории UNIT.
    
    Args:
        unit_path: Путь к директории UNIT
        
    Returns:
        Словарь с контрактом
        
    Raises:
        FileNotFoundError: Если docprep.contract.json не найден
    """
    contract_path = unit_path / "docprep.contract.json"
    if not contract_path.exists():
        raise FileNotFoundError(f"Contract not found: {contract_path}")
    
    with open(contract_path, "r", encoding="utf-8") as f:
        return json.load(f)


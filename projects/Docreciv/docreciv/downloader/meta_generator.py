"""
MetaGenerator - генерация meta-файлов для UNIT директорий.

Создает unit.meta.json и raw_url_map.json для трассировки и аудита.
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class MetaGenerator:
    """Генератор meta-файлов для UNIT директорий."""
    
    @staticmethod
    def create_unit_meta(
        unit_dir: Path,
        unit_id: str,
        protocol: Dict[str, Any],
        source_date: str,
        files_success: int,
        files_failed: int,
        files_total: int
    ) -> None:
        """
        Создать unit.meta.json файл для UNIT директории.

        Args:
            unit_dir: Path к UNIT директории
            unit_id: Идентификатор UNIT
            protocol: Протокол документ из MongoDB
            source_date: Дата источника (YYYY-MM-DD)
            files_success: Количество успешно загруженных файлов
            files_failed: Количество неудачных загрузок
            files_total: Общее количество файлов
        """
        try:
            # Local MongoDB _id (for reference)
            local_mongo_id = str(protocol.get("_id", ""))

            # ★ PRIMARY TRACE ID: Original remote MongoDB _id
            remote_mongo_id = protocol.get("remote_mongo_id", "")

            # Извлекаем purchaseNoticeNumber
            purchase_info = protocol.get("purchaseInfo", {})
            purchase_notice_number = None
            if isinstance(purchase_info, dict):
                purchase_notice_number = purchase_info.get("purchaseNoticeNumber")

            meta_data = {
                "unit_id": unit_id,

                # ★ TRACEABILITY FIELDS
                "remote_mongo_id": remote_mongo_id,  # Primary trace ID (from remote MongoDB)
                "local_mongo_id": local_mongo_id,    # Local MongoDB _id (reference)
                "record_id": local_mongo_id,         # Legacy field (kept for compatibility)

                # Download metadata
                "source_date": source_date,
                "downloaded_at": datetime.utcnow().isoformat() + "Z",
                "files_total": files_total,
                "files_success": files_success,
                "files_failed": files_failed,

                # Business metadata
                "purchase_notice_number": purchase_notice_number,
                "source": protocol.get("source", "unknown"),
                "url_count": protocol.get("url_count", 0),
                "multi_url": protocol.get("multi_url", False),

                # ★ TRACE INFO (for easy reference)
                "trace_id": remote_mongo_id,  # Same as remote_mongo_id for convenience
            }

            meta_file = unit_dir / "unit.meta.json"
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(meta_data, f, ensure_ascii=False, indent=2)

            logger.debug(f"Created unit.meta.json for {unit_id} with remote_mongo_id={remote_mongo_id[:8]}...")
        except Exception as e:
            logger.error(f"Error creating unit.meta.json for {unit_id}: {e}")
            raise
    
    @staticmethod
    def create_raw_url_map(
        unit_dir: Path,
        download_results: List[Dict[str, Any]]
    ) -> None:
        """
        Создать raw_url_map.json файл для трассировки URL.
        
        Args:
            unit_dir: Path к UNIT директории
            download_results: Список результатов загрузки с информацией о URL
        """
        try:
            url_map_file = unit_dir / "raw_url_map.json"
            
            # Форматируем результаты для лучшей читаемости
            formatted_results = []
            for result in download_results:
                formatted_result = {
                    "url": result.get("url", ""),
                    "filename": result.get("filename", ""),
                    "original_filename": result.get("original_filename", ""),
                    "status": result.get("status", "unknown"),
                }
                
                # Добавляем опциональные поля если они есть
                if "guid" in result and result["guid"]:
                    formatted_result["guid"] = result["guid"]
                if "contentUid" in result and result["contentUid"]:
                    formatted_result["contentUid"] = result["contentUid"]
                if "description" in result and result["description"]:
                    formatted_result["description"] = result["description"]
                if "error" in result:
                    formatted_result["error"] = result["error"]
                
                formatted_results.append(formatted_result)
            
            with open(url_map_file, 'w', encoding='utf-8') as f:
                json.dump(formatted_results, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"Created raw_url_map.json with {len(formatted_results)} entries")
        except Exception as e:
            logger.error(f"Error creating raw_url_map.json: {e}")
            raise
    
    @staticmethod
    def read_unit_meta(unit_dir: Path) -> Optional[Dict[str, Any]]:
        """
        Прочитать unit.meta.json из UNIT директории.
        
        Args:
            unit_dir: Path к UNIT директории
            
        Returns:
            Словарь с метаданными или None если файл не найден
        """
        try:
            meta_file = unit_dir / "unit.meta.json"
            if not meta_file.exists():
                return None
            
            with open(meta_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading unit.meta.json from {unit_dir}: {e}")
            return None
    
    @staticmethod
    def read_raw_url_map(unit_dir: Path) -> Optional[List[Dict[str, Any]]]:
        """
        Прочитать raw_url_map.json из UNIT директории.
        
        Args:
            unit_dir: Path к UNIT директории
            
        Returns:
            Список словарей с информацией о URL или None если файл не найден
        """
        try:
            url_map_file = unit_dir / "raw_url_map.json"
            if not url_map_file.exists():
                return None
            
            with open(url_map_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading raw_url_map.json from {unit_dir}: {e}")
            return None


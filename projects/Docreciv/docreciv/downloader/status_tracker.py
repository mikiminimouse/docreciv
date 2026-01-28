"""
DownloadStatusTracker - отслеживание статуса загрузки на основе файловой системы.

Проверяет наличие UNIT директорий в файловой системе и связывает их с записями в БД.
"""

import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Set
from collections import defaultdict

from docreciv.downloader.file_manager import FileManager
from docreciv.downloader.meta_generator import MetaGenerator

logger = logging.getLogger(__name__)


class DownloadStatusTracker:
    """
    Трекер статуса загрузки протоколов на основе файловой системы.
    
    Сканирует директории Input и создает индекс загруженных UNIT,
    связывая их с записями в MongoDB через record_id из unit.meta.json.
    """
    
    def __init__(self, base_input_dir: Path):
        """
        Инициализация трекера.
        
        Args:
            base_input_dir: Базовая директория INPUT_DIR (final_preprocessing/Data)
        """
        self.base_input_dir = Path(base_input_dir)
        self.file_manager = FileManager(base_input_dir)
        self.meta_generator = MetaGenerator()
        
        # Кэш индекса загруженных UNIT
        self._downloaded_index: Dict[str, Dict[str, Any]] = {}
        self._index_by_unit_id: Dict[str, Dict[str, Any]] = {}
        self._index_by_date: Dict[str, List[str]] = defaultdict(list)
        self._index_cache_time: Optional[datetime] = None
        self._cache_ttl_seconds = 300  # 5 минут
    
    def scan_downloaded_units(
        self,
        days: Optional[int] = None,
        date_range: Optional[Tuple[datetime, datetime]] = None,
        force_rescan: bool = False
    ) -> Dict[str, Any]:
        """
        Сканировать файловую систему для поиска загруженных UNIT.
        
        Args:
            days: Количество дней для сканирования (от текущей даты назад)
            date_range: Диапазон дат для сканирования (from_date, to_date)
            force_rescan: Принудительное пересканирование (игнорировать кэш)
            
        Returns:
            Словарь с индексом загруженных UNIT:
            {
                "by_record_id": {record_id: unit_info},
                "by_unit_id": {unit_id: unit_info},
                "by_date": {date: [unit_ids]},
                "summary": {total_units, total_files, dates_count}
            }
        """
        # Проверяем кэш
        if not force_rescan and self._index_cache_time:
            cache_age = (datetime.utcnow() - self._index_cache_time).total_seconds()
            if cache_age < self._cache_ttl_seconds:
                logger.debug("Using cached download index")
                return self._get_index_summary()
        
        logger.info("Scanning file system for downloaded units...")
        
        # Очищаем индекс
        self._downloaded_index = {}
        self._index_by_unit_id = {}
        self._index_by_date = defaultdict(list)
        
        # Определяем диапазон дат для сканирования
        dates_to_scan = self._get_dates_to_scan(days, date_range)
        
        total_units = 0
        total_files = 0
        
        for date_str in dates_to_scan:
            date_dir = self.base_input_dir / date_str / "Input"
            if not date_dir.exists() or not date_dir.is_dir():
                continue
            
            logger.debug(f"Scanning date directory: {date_dir}")
            
            for unit_dir in date_dir.iterdir():
                if not unit_dir.is_dir() or not unit_dir.name.startswith("UNIT_"):
                    continue
                
                unit_id = unit_dir.name
                unit_info = self._scan_unit_directory(unit_dir, date_str, unit_id)
                
                if unit_info:
                    record_id = unit_info.get("record_id")
                    if record_id:
                        self._downloaded_index[record_id] = unit_info
                    self._index_by_unit_id[unit_id] = unit_info
                    self._index_by_date[date_str].append(unit_id)
                    total_units += 1
                    total_files += unit_info.get("files_count", 0)
        
        self._index_cache_time = datetime.utcnow()
        
        logger.info(f"Scanned {total_units} downloaded units with {total_files} files")
        
        return self._get_index_summary()
    
    def _get_dates_to_scan(
        self,
        days: Optional[int],
        date_range: Optional[Tuple[datetime, datetime]]
    ) -> List[str]:
        """Получить список дат для сканирования."""
        dates = []
        
        if date_range:
            from_date, to_date = date_range
            current = from_date
            while current <= to_date:
                dates.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
        elif days:
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=days)
            current = start_date
            while current <= end_date:
                dates.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
        else:
            # Сканируем все доступные даты
            if self.base_input_dir.exists():
                for date_dir in self.base_input_dir.iterdir():
                    if date_dir.is_dir() and date_dir.name.startswith("20"):  # YYYY-MM-DD format
                        dates.append(date_dir.name)
        
        return sorted(set(dates))
    
    def _scan_unit_directory(
        self,
        unit_dir: Path,
        date_str: str,
        unit_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Сканировать UNIT директорию и извлечь информацию.
        
        Args:
            unit_dir: Путь к UNIT директории
            date_str: Дата в формате YYYY-MM-DD
            unit_id: Идентификатор UNIT
            
        Returns:
            Словарь с информацией о UNIT или None
        """
        try:
            # Читаем метаданные
            meta = self.meta_generator.read_unit_meta(unit_dir)
            if not meta:
                # UNIT без метаданных - считаем неполным
                logger.debug(f"Unit {unit_id} has no meta file")
                return None
            
            record_id = meta.get("record_id")
            if not record_id:
                logger.warning(f"Unit {unit_id} has no record_id in meta")
                return None
            
            # Получаем файлы
            files = self.file_manager.get_unit_files(unit_dir)
            files_count = len(files)
            
            # Читаем URL map
            url_map = self.meta_generator.read_raw_url_map(unit_dir)
            
            # Парсим downloaded_at
            downloaded_at = None
            downloaded_at_str = meta.get("downloaded_at")
            if downloaded_at_str:
                try:
                    downloaded_at_str = downloaded_at_str.replace('Z', '+00:00')
                    downloaded_at = datetime.fromisoformat(downloaded_at_str)
                except (ValueError, TypeError):
                    pass
            
            unit_info = {
                "unit_id": unit_id,
                "record_id": record_id,
                "date": date_str,
                "unit_dir": unit_dir,
                "files_count": files_count,
                "has_meta": True,
                "has_url_map": url_map is not None,
                "downloaded_at": downloaded_at,
                "files_success": meta.get("files_success", 0),
                "files_failed": meta.get("files_failed", 0),
                "files_total": meta.get("files_total", 0),
                "source_date": meta.get("source_date", date_str),
                "purchase_notice_number": meta.get("purchase_notice_number"),
                "is_complete": files_count > 0 and files_count >= meta.get("files_success", 0)
            }
            
            return unit_info
            
        except Exception as e:
            logger.error(f"Error scanning unit directory {unit_dir}: {e}")
            return None
    
    def _get_index_summary(self) -> Dict[str, Any]:
        """Получить сводку индекса."""
        return {
            "by_record_id": self._downloaded_index,
            "by_unit_id": self._index_by_unit_id,
            "by_date": dict(self._index_by_date),
            "summary": {
                "total_units": len(self._index_by_unit_id),
                "total_records": len(self._downloaded_index),
                "dates_count": len(self._index_by_date),
                "total_files": sum(
                    info.get("files_count", 0)
                    for info in self._index_by_unit_id.values()
                )
            }
        }
    
    def get_unit_status(
        self,
        record_id: Optional[str] = None,
        unit_id: Optional[str] = None,
        date: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Получить статус конкретного UNIT.
        
        Args:
            record_id: ID записи в БД
            unit_id: ID UNIT
            date: Дата в формате YYYY-MM-DD
            
        Returns:
            Информация о UNIT или None если не найден
        """
        if record_id and record_id in self._downloaded_index:
            return self._downloaded_index[record_id]
        
        if unit_id and unit_id in self._index_by_unit_id:
            unit_info = self._index_by_unit_id[unit_id]
            if not date or unit_info.get("date") == date:
                return unit_info
        
        # Если не в кэше, проверяем файловую систему напрямую
        if unit_id and date:
            unit_dir = self.file_manager.get_unit_dir(date, unit_id)
            if unit_dir.exists():
                return self._scan_unit_directory(unit_dir, date, unit_id)
        
        return None
    
    def is_unit_downloaded(
        self,
        record_id: Optional[str] = None,
        unit_id: Optional[str] = None,
        date: Optional[str] = None
    ) -> bool:
        """
        Проверить, загружен ли UNIT.
        
        Args:
            record_id: ID записи в БД
            unit_id: ID UNIT
            date: Дата в формате YYYY-MM-DD
            
        Returns:
            True если UNIT загружен и содержит файлы
        """
        status = self.get_unit_status(record_id, unit_id, date)
        if not status:
            return False
        
        # UNIT считается загруженным, если есть файлы
        return status.get("files_count", 0) > 0
    
    def find_missing_units(
        self,
        protocols: List[Dict[str, Any]],
        date_range: Optional[Tuple[datetime, datetime]] = None
    ) -> List[Dict[str, Any]]:
        """
        Найти протоколы, для которых нет загруженных UNIT.
        
        Args:
            protocols: Список протоколов из БД
            date_range: Диапазон дат для проверки
            
        Returns:
            Список протоколов без загруженных UNIT
        """
        # Сканируем файловую систему для актуального индекса
        self.scan_downloaded_units(date_range=date_range, force_rescan=True)
        
        missing = []
        
        for protocol in protocols:
            record_id = str(protocol.get("_id", ""))
            unit_id = protocol.get("unit_id")
            load_date = protocol.get("loadDate")
            
            if not unit_id:
                # Протокол без unit_id - пропускаем
                continue
            
            # Определяем дату
            date_str = None
            if load_date:
                if isinstance(load_date, datetime):
                    date_str = load_date.strftime("%Y-%m-%d")
                elif isinstance(load_date, str):
                    try:
                        date_obj = datetime.fromisoformat(load_date.replace('Z', '+00:00'))
                        date_str = date_obj.strftime("%Y-%m-%d")
                    except (ValueError, TypeError):
                        pass
            
            if not date_str:
                # Не можем определить дату - считаем отсутствующим
                missing.append(protocol)
                continue
            
            # Проверяем наличие UNIT
            if not self.is_unit_downloaded(record_id=record_id, unit_id=unit_id, date=date_str):
                missing.append(protocol)
        
        return missing
    
    def generate_download_report(
        self,
        date_range: Tuple[datetime, datetime],
        db_protocols: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Сгенерировать детальный отчет о статусе загрузки.
        
        Args:
            date_range: Диапазон дат (from_date, to_date)
            db_protocols: Список протоколов из БД для сравнения (опционально)
            
        Returns:
            Детальный отчет с разбивкой по датам
        """
        from_date, to_date = date_range
        
        # Сканируем файловую систему
        index = self.scan_downloaded_units(date_range=date_range, force_rescan=True)
        
        # Группируем по датам
        by_date: Dict[str, Dict[str, Any]] = {}
        
        current_date = from_date
        while current_date <= to_date:
            date_str = current_date.strftime("%Y-%m-%d")
            
            # Получаем UNIT для этой даты
            unit_ids = self._index_by_date.get(date_str, [])
            downloaded_units = [
                self._index_by_unit_id[uid] for uid in unit_ids
                if uid in self._index_by_unit_id
            ]
            
            # Если есть протоколы из БД, сравниваем
            db_protocols_for_date = []
            if db_protocols:
                for protocol in db_protocols:
                    load_date = protocol.get("loadDate")
                    if load_date:
                        if isinstance(load_date, datetime):
                            protocol_date = load_date.strftime("%Y-%m-%d")
                        else:
                            try:
                                date_obj = datetime.fromisoformat(str(load_date).replace('Z', '+00:00'))
                                protocol_date = date_obj.strftime("%Y-%m-%d")
                            except (ValueError, TypeError):
                                continue
                        
                        if protocol_date == date_str:
                            db_protocols_for_date.append(protocol)
            
            # Находим загруженные и незагруженные записи
            downloaded_record_ids = {unit["record_id"] for unit in downloaded_units if unit.get("record_id")}
            
            pending_records = []
            downloaded_records = []
            
            for protocol in db_protocols_for_date:
                record_id = str(protocol.get("_id", ""))
                if record_id in downloaded_record_ids:
                    downloaded_records.append(protocol)
                else:
                    pending_records.append(protocol)
            
            by_date[date_str] = {
                "date": date_str,
                "total_in_db": len(db_protocols_for_date),
                "downloaded_units": len(downloaded_units),
                "pending_units": len(pending_records),
                "downloaded_records": downloaded_records[:10],  # Ограничиваем для отчета
                "pending_records": pending_records[:10],  # Ограничиваем для отчета
                "downloaded_record_ids": list(downloaded_record_ids),
                "total_files": sum(unit.get("files_count", 0) for unit in downloaded_units)
            }
            
            current_date += timedelta(days=1)
        
        # Сводная статистика
        total_in_db = sum(info["total_in_db"] for info in by_date.values())
        total_downloaded = sum(info["downloaded_units"] for info in by_date.values())
        total_pending = sum(info["pending_units"] for info in by_date.values())
        
        # Рекомендации
        recommendations = []
        if total_pending > 0:
            recommendations.append(f"Найдено {total_pending} протоколов для загрузки в указанном диапазоне дат")
        
        dates_with_pending = [
            date_str for date_str, info in by_date.items()
            if info["pending_units"] > 0
        ]
        if dates_with_pending:
            recommendations.append(f"Даты с незагруженными протоколами: {', '.join(dates_with_pending[:10])}")
        
        if total_downloaded > 0:
            recommendations.append(f"Уже загружено {total_downloaded} UNIT в указанном диапазоне")
        
        return {
            "date_range": (from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")),
            "by_date": by_date,
            "summary": {
                "total_in_db": total_in_db,
                "downloaded": total_downloaded,
                "pending": total_pending,
                "dates_count": len(by_date),
                "dates_with_pending": dates_with_pending,
                "recommendations": recommendations
            }
        }
    
    def get_downloaded_record_ids(
        self,
        date_range: Optional[Tuple[datetime, datetime]] = None
    ) -> Set[str]:
        """
        Получить множество record_id уже загруженных протоколов.
        
        Args:
            date_range: Диапазон дат для фильтрации
            
        Returns:
            Множество record_id
        """
        if date_range:
            self.scan_downloaded_units(date_range=date_range, force_rescan=True)
        else:
            self.scan_downloaded_units(force_rescan=False)
        
        return set(self._downloaded_index.keys())


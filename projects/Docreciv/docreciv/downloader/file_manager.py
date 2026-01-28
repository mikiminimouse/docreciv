"""
FileManager - управление файлами и директориями для Downloader.

Отвечает за создание и валидацию структуры UNIT директорий.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class FileManager:
    """Менеджер файлов для работы с UNIT директориями."""
    
    def __init__(self, base_input_dir: Path):
        """
        Инициализация FileManager.
        
        Args:
            base_input_dir: Базовая директория INPUT_DIR
        """
        self.base_input_dir = Path(base_input_dir)
        self.base_input_dir.mkdir(parents=True, exist_ok=True)
    
    def get_unit_dir(self, source_date: str, unit_id: str) -> Path:
        """
        Получить путь к UNIT директории.

        Args:
            source_date: Дата источника в формате YYYY-MM-DD
            unit_id: Идентификатор UNIT

        Returns:
            Path к UNIT директории
        """
        # Если base_input_dir уже заканчивается на "Input", не дублируем структуру
        if self.base_input_dir.name == "Input":
            # Путь: /base/.../Input/UNIT_xxx (без дублирования даты)
            unit_dir = self.base_input_dir / unit_id
        else:
            # Стандартный путь: /base/YYYY-MM-DD/Input/UNIT_xxx
            date_dir = self.base_input_dir / source_date / "Input"
            unit_dir = date_dir / unit_id
        return unit_dir
    
    def create_unit_dir(self, source_date: str, unit_id: str) -> Path:
        """
        Создать UNIT директорию.
        
        Args:
            source_date: Дата источника в формате YYYY-MM-DD
            unit_id: Идентификатор UNIT
            
        Returns:
            Path к созданной UNIT директории
        """
        unit_dir = self.get_unit_dir(source_date, unit_id)
        unit_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Created unit directory: {unit_dir}")
        return unit_dir
    
    def unit_dir_exists(self, source_date: str, unit_id: str) -> bool:
        """
        Проверить существование UNIT директории.
        
        Args:
            source_date: Дата источника в формате YYYY-MM-DD
            unit_id: Идентификатор UNIT
            
        Returns:
            True если директория существует, False иначе
        """
        unit_dir = self.get_unit_dir(source_date, unit_id)
        return unit_dir.exists() and unit_dir.is_dir()
    
    def validate_unit_structure(self, unit_dir: Path) -> bool:
        """
        Валидировать структуру UNIT директории.
        
        Проверяет наличие обязательных файлов и структуры.
        
        Args:
            unit_dir: Path к UNIT директории
            
        Returns:
            True если структура валидна, False иначе
        """
        if not unit_dir.exists():
            return False
        
        if not unit_dir.is_dir():
            return False
        
        # Проверяем наличие meta файла (опционально, но желательно)
        meta_file = unit_dir / "unit.meta.json"
        if not meta_file.exists():
            logger.warning(f"Unit directory {unit_dir} missing unit.meta.json")
        
        return True
    
    def get_unit_files(self, unit_dir: Path) -> list[Path]:
        """
        Получить список файлов в UNIT директории (исключая meta файлы).
        
        Args:
            unit_dir: Path к UNIT директории
            
        Returns:
            Список Path к файлам
        """
        if not unit_dir.exists():
            return []
        
        # Исключаем meta файлы и скрытые файлы
        excluded = {"unit.meta.json", "raw_url_map.json", ".DS_Store"}
        files = [
            f for f in unit_dir.iterdir()
            if f.is_file() and f.name not in excluded and not f.name.startswith(".")
        ]
        return files
    
    def count_unit_files(self, unit_dir: Path) -> int:
        """
        Подсчитать количество файлов в UNIT директории.
        
        Args:
            unit_dir: Path к UNIT директории
            
        Returns:
            Количество файлов
        """
        return len(self.get_unit_files(unit_dir))
    
    def is_unit_empty(self, unit_dir: Path) -> bool:
        """
        Проверить, пуста ли UNIT директория (нет файлов, кроме meta).
        
        Args:
            unit_dir: Path к UNIT директории
            
        Returns:
            True если директория пуста, False иначе
        """
        return self.count_unit_files(unit_dir) == 0


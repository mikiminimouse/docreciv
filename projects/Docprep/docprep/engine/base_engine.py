"""
BaseEngine — базовый класс для всех движков обработки (Converter, Extractor, Normalizer).

Унифицирует:
- Обработку ошибок и перемещение в Exceptions
- Обновление манифеста при успехе/неудаче
- Логирование операций
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Optional
import logging
import shutil

from ..core.state_machine import UnitState, UnitStateMachine
from ..core.manifest import load_manifest, save_manifest, update_manifest_operation
from ..core.audit import get_audit_logger
from ..core.routing import is_supported_extension, UNSUPPORTED_EXTENSIONS


logger = logging.getLogger(__name__)


class BaseEngine(ABC):
    """
    Базовый класс для движков обработки.
    
    Предоставляет унифицированные методы для:
    - Перемещения юнитов в Exceptions при ошибках
    - Обновления манифеста с информацией об операциях
    - Логирования операций в audit log
    """
    
    def __init__(self, protocol_date: Optional[str] = None, dry_run: bool = False):
        self.protocol_date = protocol_date
        self.dry_run = dry_run
        self.audit_logger = get_audit_logger()
    
    @property
    @abstractmethod
    def engine_name(self) -> str:
        """Имя движка для логирования (convert, extract, normalize)."""
        pass
    
    @property
    @abstractmethod
    def operation_type(self) -> str:
        """Тип операции для манифеста."""
        pass
    
    def move_to_exceptions(
        self,
        unit_path: Path,
        reason: str,
        cycle: int,
        exceptions_base: Path,
    ) -> Path:
        """
        Перемещает юнит в директорию Exceptions.
        
        Args:
            unit_path: Путь к юниту
            reason: Причина перемещения (Empty, Special, Ambiguous, ErConvert, ErNormalize, ErExtract)
            cycle: Номер цикла
            exceptions_base: Базовая директория Exceptions
        
        Returns:
            Новый путь к юниту
        """
        unit_id = unit_path.name
        # НОВАЯ СТРУКТУРА v2: Exceptions/Direct для цикла 1, Exceptions/Processing_N для остальных
        if cycle == 1:
            target_dir = exceptions_base / "Direct" / reason / unit_id
        else:
            target_dir = exceptions_base / f"Processing_{cycle}" / reason / unit_id
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would move {unit_id} to {target_dir}")
            return target_dir
        
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        
        if target_dir.exists():
            shutil.rmtree(target_dir)
        
        shutil.move(str(unit_path), str(target_dir))
        logger.warning(f"Moved {unit_id} to Exceptions/{reason}")
        
        # Логируем в audit
        self.audit_logger.log_event(
            unit_id=unit_id,
            event_type="exception",
            operation=self.operation_type,
            details={"reason": reason, "cycle": cycle},
            unit_path=target_dir,
        )
        
        return target_dir
    
    def update_manifest_on_success(
        self,
        unit_path: Path,
        operation_details: Dict[str, Any],
        cycle: int,
    ) -> None:
        """
        Обновляет манифест после успешной операции.
        
        Args:
            unit_path: Путь к юниту
            operation_details: Детали операции (from, to, tool, etc.)
            cycle: Номер цикла
        """
        if self.dry_run:
            return
        
        try:
            manifest = load_manifest(unit_path)
            
            operation = {
                "type": self.operation_type,
                "cycle": cycle,
                "status": "success",
                **operation_details,
            }
            
            manifest = update_manifest_operation(manifest, operation)
            save_manifest(unit_path, manifest)
            
        except Exception as e:
            logger.warning(f"Failed to update manifest for {unit_path.name}: {e}")
    
    def update_manifest_on_failure(
        self,
        unit_path: Path,
        error_message: str,
        cycle: int,
    ) -> None:
        """
        Обновляет манифест после неудачной операции.
        
        Args:
            unit_path: Путь к юниту
            error_message: Сообщение об ошибке
            cycle: Номер цикла
        """
        if self.dry_run:
            return
        
        try:
            manifest = load_manifest(unit_path)
            
            operation = {
                "type": self.operation_type,
                "cycle": cycle,
                "status": "failed",
                "error": error_message,
            }
            
            manifest = update_manifest_operation(manifest, operation)
            save_manifest(unit_path, manifest)
            
        except Exception as e:
            logger.warning(f"Failed to update manifest for {unit_path.name}: {e}")
    
    def log_operation(
        self,
        unit_id: str,
        success: bool,
        details: Optional[Dict[str, Any]] = None,
        unit_path: Optional[Path] = None,
    ) -> None:
        """
        Логирует операцию в audit log.
        
        Args:
            unit_id: ID юнита
            success: Успешность операции
            details: Дополнительные детали
            unit_path: Путь к юниту (опционально)
        """
        self.audit_logger.log_event(
            unit_id=unit_id,
            event_type="operation",
            operation=self.operation_type,
            details={
                "engine": self.engine_name,
                "success": success,
                **(details or {}),
            },
            unit_path=unit_path,
        )
    
    def validate_unit_files(self, unit_path: Path) -> bool:
        """
        Проверяет, что файлы юнита поддерживаются.
        
        Args:
            unit_path: Путь к юниту
        
        Returns:
            True если все файлы поддерживаются
        """
        for file_path in unit_path.iterdir():
            if file_path.is_file() and file_path.name != "manifest.json":
                ext = file_path.suffix.lower().lstrip(".")
                if not is_supported_extension(ext):
                    return False
        return True

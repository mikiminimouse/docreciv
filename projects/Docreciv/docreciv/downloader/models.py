"""
Модели данных для Downloader компонента.

Определяет контракты для запросов загрузки, конфигурации и результатов.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional, List, Dict, Any
from pathlib import Path


@dataclass
class DownloadRequest:
    """
    Запрос на загрузку файлов для протоколов.
    
    Attributes:
        from_date: Начальная дата для фильтрации (опционально)
        to_date: Конечная дата для фильтрации (опционально)
        record_ids: Список конкретных record_id для загрузки (опционально)
        max_units_per_run: Максимальное количество UNIT для обработки (0 = без лимита)
        max_urls_per_unit: Максимальное количество URL на UNIT
        dry_run: Режим тестирования без реальных изменений
        force_reload: Принудительная повторная загрузка существующих UNIT
        skip_existing: Пропускать существующие UNIT директории
        requested_by: Идентификатор пользователя/системы, запросившей загрузку
    """
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    record_ids: Optional[List[str]] = None
    max_units_per_run: int = 0  # 0 = no limit
    max_urls_per_unit: int = 15
    dry_run: bool = False
    force_reload: bool = False
    skip_existing: bool = True
    requested_by: str = "system"
    
    def __post_init__(self):
        """Валидация запроса после инициализации."""
        if self.from_date and self.to_date:
            if self.from_date > self.to_date:
                raise ValueError("from_date must be before or equal to to_date")
        
        if self.max_units_per_run < 0:
            raise ValueError("max_units_per_run must be non-negative")
        
        if self.max_urls_per_unit <= 0:
            raise ValueError("max_urls_per_unit must be positive")


@dataclass
class DownloadConfig:
    """
    Конфигурация для загрузки.
    
    Attributes:
        input_dir: Базовая директория для загрузки (INPUT_DIR)
        max_urls_per_protocol: Максимальное количество URL на протокол
        download_http_timeout: Таймаут HTTP запросов в секундах
        download_concurrency: Количество одновременных загрузок файлов
        protocols_concurrency: Количество одновременных обработок протоколов
    """
    input_dir: Path
    max_urls_per_protocol: int = 15
    download_http_timeout: int = 120
    download_concurrency: int = 20
    protocols_concurrency: int = 20


@dataclass
class DownloadProgressEvent:
    """
    Событие прогресса загрузки.
    
    Attributes:
        run_id: Идентификатор запуска загрузки
        processed_units: Количество обработанных UNIT
        downloaded_files: Количество загруженных файлов
        failed_files: Количество неудачных загрузок
        current_unit: Текущий обрабатываемый UNIT
        timestamp: Время события
    """
    run_id: str
    processed_units: int = 0
    downloaded_files: int = 0
    failed_files: int = 0
    current_unit: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует событие в словарь."""
        return {
            "run_id": self.run_id,
            "processed_units": self.processed_units,
            "downloaded_files": self.downloaded_files,
            "failed_files": self.failed_files,
            "current_unit": self.current_unit,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class DownloadRunResult:
    """
    Результат выполнения загрузки.
    
    Attributes:
        run_id: Идентификатор запуска
        success: Успешно ли завершена загрузка
        started_at: Время начала
        finished_at: Время завершения
        stats: Детальная статистика
        errors: Список ошибок
        warnings: Список предупреждений
    """
    run_id: str
    success: bool
    started_at: datetime
    finished_at: datetime
    stats: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    @property
    def duration_seconds(self) -> float:
        """Длительность выполнения в секундах."""
        return (self.finished_at - self.started_at).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует результат в словарь для MongoDB."""
        return {
            "run_id": self.run_id,
            "success": self.success,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "stats": self.stats,
            "errors": self.errors,
            "warnings": self.warnings,
            "updated_at": datetime.utcnow()
        }


@dataclass
class DownloadRunHandle:
    """
    Дескриптор запуска загрузки.
    
    Attributes:
        run_id: Идентификатор запуска
        status: Текущий статус (pending, running, completed, failed, cancelled)
        request: Исходный запрос загрузки
        created_at: Время создания
    """
    run_id: str
    status: Literal["pending", "running", "completed", "failed", "cancelled"]
    request: DownloadRequest
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует дескриптор в словарь для MongoDB."""
        return {
            "run_id": self.run_id,
            "status": self.status,
            "from_date": self.request.from_date,
            "to_date": self.request.to_date,
            "record_ids": self.request.record_ids,
            "max_units_per_run": self.request.max_units_per_run,
            "max_urls_per_unit": self.request.max_urls_per_unit,
            "dry_run": self.request.dry_run,
            "force_reload": self.request.force_reload,
            "skip_existing": self.request.skip_existing,
            "requested_by": self.request.requested_by,
            "created_at": self.created_at,
            "updated_at": datetime.utcnow()
        }


@dataclass
class DownloadRunStatus:
    """
    Статус запуска загрузки.
    
    Attributes:
        run_id: Идентификатор запуска
        status: Текущий статус
        progress: Текущий прогресс (0-100)
        processed_units: Количество обработанных UNIT
        downloaded_files: Количество загруженных файлов
        failed_files: Количество неудачных загрузок
        current_unit: Текущий обрабатываемый UNIT
        message: Сообщение о текущем состоянии
    """
    run_id: str
    status: Literal["pending", "running", "completed", "failed", "cancelled"]
    progress: float = 0.0  # 0-100
    processed_units: int = 0
    downloaded_files: int = 0
    failed_files: int = 0
    current_unit: Optional[str] = None
    message: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует статус в словарь."""
        return {
            "run_id": self.run_id,
            "status": self.status,
            "progress": self.progress,
            "processed_units": self.processed_units,
            "downloaded_files": self.downloaded_files,
            "failed_files": self.failed_files,
            "current_unit": self.current_unit,
            "message": self.message
        }


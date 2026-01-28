"""
Модели данных для SyncManagerService.

Определяет контракты для запросов синхронизации, состояния курсоров,
событий прогресса и результатов выполнения синхронизации.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional, List, Dict, Any


@dataclass
class SyncRequest:
    """
    Запрос на синхронизацию данных.
    
    Attributes:
        collection: Имя коллекции для синхронизации (только "protocols")
        mode: Режим синхронизации (incremental, range, backfill, replay)
        from_date: Начальная дата для диапазона (опционально)
        to_date: Конечная дата для диапазона (опционально)
        batch_size: Размер пакета для обработки
        dry_run: Режим тестирования без реальных изменений
        write_mode: Режим записи (merge или overwrite)
        requested_by: Идентификатор пользователя/системы, запросившей синхронизацию
    """
    collection: Literal["protocols"]
    mode: Literal["incremental", "range", "backfill", "replay"]
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    batch_size: int = 1000
    dry_run: bool = False
    write_mode: Literal["merge", "overwrite"] = "merge"
    requested_by: str = "system"
    
    def __post_init__(self):
        """Валидация запроса после инициализации."""
        if self.mode in ["range", "backfill", "replay"]:
            if self.from_date is None:
                raise ValueError(f"Mode '{self.mode}' requires from_date")
            # Для backfill to_date может быть None (будет установлен из курсора)
            if self.mode in ["range", "replay"] and self.to_date is None:
                raise ValueError(f"Mode '{self.mode}' requires to_date")
        
        if self.from_date and self.to_date:
            if self.from_date > self.to_date:
                raise ValueError("from_date must be before or equal to to_date")
        
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")


@dataclass
class SyncCursorState:
    """
    Состояние курсора синхронизации.
    
    Хранит информацию о последней успешной синхронизации для
    поддержки incremental режима.
    
    Attributes:
        collection: Имя коллекции
        cursor_field: Поле для курсора (publish_date или updated_at)
        last_cursor_value: Последнее значение курсора
        last_successful_run: Время последнего успешного запуска
    """
    collection: str
    cursor_field: str  # publish_date / updated_at / loadDate
    last_cursor_value: datetime
    last_successful_run: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует состояние курсора в словарь для MongoDB."""
        return {
            "collection": self.collection,
            "cursor_field": self.cursor_field,
            "last_cursor_value": self.last_cursor_value,
            "last_successful_run": self.last_successful_run,
            "updated_at": datetime.utcnow()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SyncCursorState":
        """Создает состояние курсора из словаря MongoDB."""
        return cls(
            collection=data["collection"],
            cursor_field=data["cursor_field"],
            last_cursor_value=data["last_cursor_value"],
            last_successful_run=data["last_successful_run"]
        )


@dataclass
class SyncProgressEvent:
    """
    Событие прогресса синхронизации.
    
    Используется для отслеживания прогресса выполнения синхронизации
    в реальном времени.
    
    Attributes:
        run_id: Идентификатор запуска синхронизации
        processed: Количество обработанных документов
        inserted: Количество вставленных документов
        skipped: Количество пропущенных документов
        errors: Количество ошибок
        current_cursor: Текущее значение курсора
        timestamp: Время события
    """
    run_id: str
    processed: int = 0
    inserted: int = 0
    skipped: int = 0
    errors: int = 0
    current_cursor: Optional[datetime] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует событие в словарь."""
        return {
            "run_id": self.run_id,
            "processed": self.processed,
            "inserted": self.inserted,
            "skipped": self.skipped,
            "errors": self.errors,
            "current_cursor": self.current_cursor,
            "timestamp": self.timestamp
        }


@dataclass
class SyncRunResult:
    """
    Результат выполнения синхронизации.
    
    Содержит полную информацию о завершенной синхронизации,
    включая статистику и ошибки.
    
    Attributes:
        run_id: Идентификатор запуска
        success: Успешно ли завершена синхронизация
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
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SyncRunResult":
        """Создает результат из словаря MongoDB."""
        return cls(
            run_id=data["run_id"],
            success=data["success"],
            started_at=data["started_at"],
            finished_at=data["finished_at"],
            stats=data.get("stats", {}),
            errors=data.get("errors", []),
            warnings=data.get("warnings", [])
        )


@dataclass
class SyncRunHandle:
    """
    Дескриптор запуска синхронизации.
    
    Используется для отслеживания и управления запущенной синхронизацией.
    
    Attributes:
        run_id: Идентификатор запуска
        status: Текущий статус (pending, running, completed, failed, cancelled)
        request: Исходный запрос синхронизации
        created_at: Время создания
    """
    run_id: str
    status: Literal["pending", "running", "completed", "failed", "cancelled"]
    request: SyncRequest
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует дескриптор в словарь для MongoDB."""
        return {
            "run_id": self.run_id,
            "status": self.status,
            "collection": self.request.collection,
            "mode": self.request.mode,
            "from_date": self.request.from_date,
            "to_date": self.request.to_date,
            "batch_size": self.request.batch_size,
            "dry_run": self.request.dry_run,
            "write_mode": self.request.write_mode,
            "requested_by": self.request.requested_by,
            "created_at": self.created_at,
            "updated_at": datetime.utcnow()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SyncRunHandle":
        """Создает дескриптор из словаря MongoDB."""
        request = SyncRequest(
            collection=data["collection"],
            mode=data["mode"],
            from_date=data.get("from_date"),
            to_date=data.get("to_date"),
            batch_size=data.get("batch_size", 1000),
            dry_run=data.get("dry_run", False),
            write_mode=data.get("write_mode", "merge"),
            requested_by=data.get("requested_by", "system")
        )
        return cls(
            run_id=data["run_id"],
            status=data["status"],
            request=request,
            created_at=data.get("created_at", datetime.utcnow())
        )


@dataclass
class SyncRunStatus:
    """
    Статус запуска синхронизации.
    
    Используется для получения текущего состояния синхронизации.
    
    Attributes:
        run_id: Идентификатор запуска
        status: Текущий статус
        progress: Текущий прогресс (0-100)
        processed: Количество обработанных документов
        inserted: Количество вставленных документов
        skipped: Количество пропущенных документов
        errors: Количество ошибок
        current_cursor: Текущее значение курсора
        message: Сообщение о текущем состоянии
    """
    run_id: str
    status: Literal["pending", "running", "completed", "failed", "cancelled"]
    progress: float = 0.0  # 0-100
    processed: int = 0
    inserted: int = 0
    skipped: int = 0
    errors: int = 0
    current_cursor: Optional[datetime] = None
    message: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует статус в словарь."""
        return {
            "run_id": self.run_id,
            "status": self.status,
            "progress": self.progress,
            "processed": self.processed,
            "inserted": self.inserted,
            "skipped": self.skipped,
            "errors": self.errors,
            "current_cursor": self.current_cursor,
            "message": self.message
        }


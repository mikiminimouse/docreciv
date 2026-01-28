"""
Интерфейсы для всех сервисов модуля receiver.

Предоставляет протоколы (Protocol) для type checking и dependency injection.
"""
from typing import Protocol, Optional, List
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass


@dataclass
class SyncResult:
    """Результат синхронизации протоколов."""
    status: str
    message: str
    date: str
    scanned: int = 0
    inserted: int = 0
    skipped_existing: int = 0
    errors_count: int = 0
    duration: float = 0.0
    errors: Optional[List[str]] = None


@dataclass
class DownloadResult:
    """Результат загрузки документов."""
    status: str
    message: str
    processed: int = 0
    downloaded: int = 0
    failed: int = 0
    duration: float = 0.0
    errors: Optional[List[str]] = None


@dataclass
class ProcessResult:
    """Результат обработки файла."""
    status: str
    unit_id: Optional[str]
    file: str
    message: Optional[str] = None


class ISyncService(Protocol):
    """Интерфейс для сервиса синхронизации протоколов."""
    
    def sync_protocols_for_date(
        self,
        target_date: datetime,
        limit: Optional[int] = None
    ) -> SyncResult:
        """Синхронизирует протоколы за указанную дату."""
        ...
    
    def sync_daily_updates(self, limit: Optional[int] = None) -> SyncResult:
        """Выполняет ежедневное обновление (вчерашний день)."""
        ...


class IDownloadService(Protocol):
    """Интерфейс для сервиса загрузки документов."""
    
    def process_pending_protocols(self, limit: int = 200) -> DownloadResult:
        """Обрабатывает ожидающие загрузки протоколы."""
        ...


class IProcessingService(Protocol):
    """Интерфейс для сервиса обработки файлов."""
    
    def process_file(
        self,
        file_path: Path,
        unit_metadata: Optional[dict] = None
    ) -> ProcessResult:
        """Обрабатывает один файл."""
        ...
    
    def process_files(
        self,
        file_paths: List[Path],
        unit_metadata: Optional[dict] = None
    ) -> List[ProcessResult]:
        """Обрабатывает несколько файлов."""
        ...


class IMongoConnector(Protocol):
    """Интерфейс для подключения к MongoDB."""
    
    def get_remote_client(self):
        """Получает клиент для удаленной MongoDB."""
        ...
    
    def get_local_client(self):
        """Получает клиент для локальной MongoDB."""
        ...
    
    def close(self) -> None:
        """Закрывает все подключения."""
        ...


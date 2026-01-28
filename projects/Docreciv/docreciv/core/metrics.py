"""
Базовые классы и интерфейсы для сбора статистики.
Унифицированный подход к сбору метрик в различных компонентах.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class BaseStatistics:
    """Базовый класс для статистики."""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    duration_seconds: float = 0.0
    errors_count: int = 0
    errors: list = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразовать статистику в словарь."""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, list):
                result[key] = value
            else:
                result[key] = value
        return result


@dataclass
class SyncStatistics(BaseStatistics):
    """Статистика синхронизации."""
    scanned: int = 0
    inserted: int = 0
    skipped_existing: int = 0
    skipped_duplicates: int = 0
    
    # Дополнительные метрики
    url_distribution: Dict[str, int] = field(default_factory=dict)
    attachment_types: Dict[str, int] = field(default_factory=dict)
    processing_times: list = field(default_factory=list)
    error_types: Dict[str, int] = field(default_factory=dict)
    
    # Вычисляемые метрики
    @property
    def success_rate(self) -> float:
        """Процент успешных операций."""
        total = self.scanned
        if total == 0:
            return 0.0
        successful = self.inserted + self.skipped_existing
        return (successful / total) * 100
    
    @property
    def average_processing_time(self) -> float:
        """Среднее время обработки."""
        if not self.processing_times:
            return 0.0
        return sum(self.processing_times) / len(self.processing_times)


@dataclass
class DownloadStatistics(BaseStatistics):
    """Статистика загрузки."""
    processed: int = 0
    downloaded: int = 0
    failed: int = 0
    skipped: int = 0
    
    # Дополнительные метрики
    file_sizes: list = field(default_factory=list)
    download_times: list = field(default_factory=list)
    failed_by_domain: Dict[str, int] = field(default_factory=dict)
    
    # Вычисляемые метрики
    @property
    def success_rate(self) -> float:
        """Процент успешных загрузок."""
        total = self.processed
        if total == 0:
            return 0.0
        return (self.downloaded / total) * 100
    
    @property
    def total_size_bytes(self) -> int:
        """Общий размер загруженных файлов."""
        return sum(self.file_sizes) if self.file_sizes else 0
    
    @property
    def average_file_size(self) -> float:
        """Средний размер файла."""
        if not self.file_sizes:
            return 0.0
        return sum(self.file_sizes) / len(self.file_sizes)
    
    @property
    def average_download_time(self) -> float:
        """Среднее время загрузки."""
        if not self.download_times:
            return 0.0
        return sum(self.download_times) / len(self.download_times)


class BaseStatisticsCollector(ABC):
    """Абстрактный базовый класс для сбора статистики."""
    
    def __init__(self):
        """Инициализация коллектора статистики."""
        self.counters: Dict[str, Any] = {}
        self._initialize_counters()
    
    @abstractmethod
    def _initialize_counters(self) -> None:
        """Инициализировать счетчики статистики."""
        pass
    
    @abstractmethod
    def collect(self) -> BaseStatistics:
        """Собрать статистику из счетчиков."""
        pass
    
    def reset(self) -> None:
        """Сбросить все счетчики."""
        self._initialize_counters()


class SyncStatisticsCollector(BaseStatisticsCollector):
    """Коллектор статистики для синхронизации."""
    
    def _initialize_counters(self) -> None:
        """Инициализировать счетчики статистики синхронизации."""
        self.counters = {
            "scanned": 0,
            "inserted": 0,
            "skipped_existing": 0,
            "skipped_duplicates": 0,
            "errors": 0,
            "error_list": [],
            "single_url": 0,
            "multi_url": 0,
            "no_url": 0,
            "attachment_types": {},
            "processing_times": [],
            "error_types": {}
        }
    
    def collect(self) -> SyncStatistics:
        """Собрать статистику синхронизации."""
        stats = SyncStatistics(
            scanned=self.counters.get("scanned", 0),
            inserted=self.counters.get("inserted", 0),
            skipped_existing=self.counters.get("skipped_existing", 0),
            skipped_duplicates=self.counters.get("skipped_duplicates", 0),
            errors_count=self.counters.get("errors", 0),
            errors=self.counters.get("error_list", []).copy(),
            url_distribution={
                "single_url": self.counters.get("single_url", 0),
                "multi_url": self.counters.get("multi_url", 0),
                "no_url": self.counters.get("no_url", 0)
            },
            attachment_types=self.counters.get("attachment_types", {}).copy(),
            processing_times=self.counters.get("processing_times", []).copy(),
            error_types=self.counters.get("error_types", {}).copy()
        )
        return stats


class DownloadStatisticsCollector(BaseStatisticsCollector):
    """Коллектор статистики для загрузки."""
    
    def _initialize_counters(self) -> None:
        """Инициализировать счетчики статистики загрузки."""
        self.counters = {
            "processed": 0,
            "downloaded": 0,
            "failed": 0,
            "skipped": 0,
            "errors": 0,
            "error_list": [],
            "file_sizes": [],
            "download_times": [],
            "failed_by_domain": {}
        }
    
    def collect(self) -> DownloadStatistics:
        """Собрать статистику загрузки."""
        stats = DownloadStatistics(
            processed=self.counters.get("processed", 0),
            downloaded=self.counters.get("downloaded", 0),
            failed=self.counters.get("failed", 0),
            skipped=self.counters.get("skipped", 0),
            errors_count=self.counters.get("errors", 0),
            errors=self.counters.get("error_list", []).copy(),
            file_sizes=self.counters.get("file_sizes", []).copy(),
            download_times=self.counters.get("download_times", []).copy(),
            failed_by_domain=self.counters.get("failed_by_domain", {}).copy()
        )
        return stats


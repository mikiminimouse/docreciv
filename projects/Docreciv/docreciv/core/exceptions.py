"""
Кастомные исключения для модуля receiver.

Предоставляет иерархию исключений с контекстной информацией для лучшей обработки ошибок.
"""
from typing import Optional, Dict, Any


class PreprocessingError(Exception):
    """
    Базовое исключение для всех ошибок препроцессинга.
    
    Attributes:
        message: Сообщение об ошибке
        context: Дополнительный контекст (например, unit_id, file_path)
        original_error: Исходное исключение, если было перехвачено
    """
    
    def __init__(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None
    ):
        super().__init__(message)
        self.message = message
        self.context = context or {}
        self.original_error = original_error
    
    def __str__(self) -> str:
        parts = [self.message]
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"Context: {context_str}")
        if self.original_error:
            parts.append(f"Original error: {self.original_error}")
        return " | ".join(parts)


class ConfigurationError(PreprocessingError):
    """Ошибка конфигурации (невалидные настройки, отсутствующие переменные окружения)."""
    pass


class SyncError(PreprocessingError):
    """Ошибка синхронизации протоколов из удаленной MongoDB."""
    pass


class ConnectionError(PreprocessingError):
    """Ошибка подключения к внешнему сервису (MongoDB, HTTP)."""
    pass


class DownloadError(PreprocessingError):
    """Ошибка загрузки файлов с zakupki.gov.ru."""
    pass


class ProcessingError(PreprocessingError):
    """Ошибка обработки файла/unit в router."""
    pass


class FileDetectionError(ProcessingError):
    """Ошибка определения типа файла."""
    pass


class ArchiveExtractionError(ProcessingError):
    """Ошибка распаковки архива."""
    pass


class ConversionError(ProcessingError):
    """Ошибка конвертации файла (например, DOC -> DOCX)."""
    pass


class ClassificationError(ProcessingError):
    """Ошибка классификации файла по категориям."""
    pass


class DistributionError(ProcessingError):
    """Ошибка распределения unit по директориям."""
    pass


class MetricsError(PreprocessingError):
    """Ошибка записи/чтения метрик."""
    pass


class ManifestError(PreprocessingError):
    """Ошибка создания/чтения manifest."""
    pass


"""
Модели данных для отслеживания pipeline метаданных.

Этот модуль определяет структуры данных для управления документами
через весь pipeline обработки:
Download → DocPrep → DoclingProc → LLM_qaenrich
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional, Dict, Any, List
from enum import Enum


class StageStatus(str, Enum):
    """Статусы этапа pipeline."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class PipelineStage(str, Enum):
    """Этапы pipeline."""
    DOWNLOAD = "download"
    DOCPREP = "docprep"
    DOCLING = "docling"
    LLM_QAENRICH = "llm_qaenrich"


@dataclass
class PipelineStageInfo:
    """
    Информация об этапе pipeline.

    Attributes:
        status: Текущий статус этапа
        started_at: Время начала этапа
        completed_at: Время завершения этапа
        metrics: Метрики этапа (зависит от типа этапа)
        error: Сообщение об ошибке, если статус failed
    """
    status: StageStatus = StageStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        """Длительность этапа в секундах."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Преобразует в словарь для MongoDB/JSON."""
        return {
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "metrics": self.metrics,
            "error": self.error
        }


@dataclass
class PipelineDocument:
    """
    Документ в pipeline с отслеживанием статусов по этапам.

    Attributes:
        unit_id: Уникальный идентификатор UNIT
        record_id: ID записи в MongoDB protocols
        purchase_notice_number: Номер госзакупки
        publication_date: Дата публикации (legacy, uses loadDate)
        load_date: Дата загрузки (loadDate) - основная дата для фильтрации
        input_dir: Путь к директории с файлами
        stages: Статусы по каждому этапу
        files: Список файлов в UNIT
    """
    unit_id: str
    record_id: str
    purchase_notice_number: str
    publication_date: datetime
    load_date: Optional[datetime] = None
    input_dir: Optional[str] = None
    stages: Dict[str, str] = field(default_factory=dict)
    files: List[str] = field(default_factory=list)

    def __post_init__(self):
        """Инициализирует статусы этапов как pending."""
        if not self.stages:
            for stage in PipelineStage:
                self.stages[stage.value] = StageStatus.PENDING.value

    def update_stage(self, stage: str, status: str) -> None:
        """Обновляет статус этапа."""
        self.stages[stage] = status

    def get_stage_status(self, stage: str) -> str:
        """Возвращает статус этапа."""
        return self.stages.get(stage, StageStatus.PENDING.value)

    def to_dict(self) -> Dict[str, Any]:
        """Преобразует в словарь для MongoDB/JSON."""
        return {
            "unit_id": self.unit_id,
            "record_id": self.record_id,
            "purchase_notice_number": self.purchase_notice_number,
            "publication_date": self.publication_date.isoformat() if self.publication_date else None,
            "load_date": self.load_date.isoformat() if self.load_date else None,
            "input_dir": self.input_dir,
            "stages": self.stages,
            "files": self.files
        }


@dataclass
class PipelineRun:
    """
    Запуск pipeline для обработки пакета документов.

    Attributes:
        run_id: Уникальный идентификатор запуска
        batch_date: Дата обрабатываемой партии (YYYY-MM-DD)
        stages: Информация о каждом этапе
        documents: Список документов в pipeline
        created_at: Время создания
        updated_at: Время последнего обновления
        contract_version: Версия формата contract
    """
    run_id: str
    batch_date: str
    stages: Dict[str, PipelineStageInfo] = field(default_factory=dict)
    documents: List[PipelineDocument] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    contract_version: str = "1.0"

    def __post_init__(self):
        """Инициализирует этапы pipeline."""
        if not self.stages:
            for stage in PipelineStage:
                self.stages[stage.value] = PipelineStageInfo()

    def get_stage(self, stage: str) -> PipelineStageInfo:
        """Возвращает информацию об этапе."""
        return self.stages.get(stage, PipelineStageInfo())

    def update_stage(
        self,
        stage: str,
        status: StageStatus,
        metrics: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> None:
        """
        Обновляет статус этапа.

        Args:
            stage: Имя этапа (download, docprep, docling, llm_qaenrich)
            status: Новый статус
            metrics: Метрики этапа
            error: Сообщение об ошибке
        """
        stage_info = self.stages.get(stage)
        if not stage_info:
            stage_info = PipelineStageInfo()
            self.stages[stage] = stage_info

        old_status = stage_info.status
        stage_info.status = status

        if status == StageStatus.RUNNING and old_status != StageStatus.RUNNING:
            stage_info.started_at = datetime.utcnow()
        elif status in (StageStatus.COMPLETED, StageStatus.FAILED):
            if not stage_info.started_at:
                stage_info.started_at = datetime.utcnow()
            stage_info.completed_at = datetime.utcnow()

        if metrics:
            stage_info.metrics.update(metrics)

        if error:
            stage_info.error = error

        self.updated_at = datetime.utcnow()

    def add_documents(self, documents: List[PipelineDocument]) -> None:
        """Добавляет документы в pipeline."""
        self.documents.extend(documents)
        self.updated_at = datetime.utcnow()

    def update_document_stage(self, unit_id: str, stage: str, status: str) -> None:
        """Обновляет статус этапа для конкретного документа."""
        for doc in self.documents:
            if doc.unit_id == unit_id:
                doc.update_stage(stage, status)
                self.updated_at = datetime.utcnow()
                return

    def get_document(self, unit_id: str) -> Optional[PipelineDocument]:
        """Возвращает документ по unit_id."""
        for doc in self.documents:
            if doc.unit_id == unit_id:
                return doc
        return None

    @property
    def total_documents(self) -> int:
        """Общее количество документов."""
        return len(self.documents)

    @property
    def completed_documents(self) -> int:
        """Количество завершённых документов (все этапы completed)."""
        count = 0
        for doc in self.documents:
            if all(s == StageStatus.COMPLETED.value for s in doc.stages.values()):
                count += 1
        return count

    @property
    def is_complete(self) -> bool:
        """Завершён ли весь pipeline."""
        return all(
            s.status == StageStatus.COMPLETED
            for s in self.stages.values()
        )

    def to_dict(self) -> Dict[str, Any]:
        """Преобразует в словарь для MongoDB/JSON."""
        return {
            "run_id": self.run_id,
            "batch_date": self.batch_date,
            "contract_version": self.contract_version,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "stages": {
                stage: info.to_dict()
                for stage, info in self.stages.items()
            },
            "documents": [doc.to_dict() for doc in self.documents]
        }

    def to_contract_json(self) -> Dict[str, Any]:
        """
        Преобразует в JSON contract для записи в файл.

        Контракт используется downstream компонентами для
        получения информации о пакете документов.
        """
        return {
            "contract_version": self.contract_version,
            "run_id": self.run_id,
            "batch_date": self.batch_date,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "total_documents": self.total_documents,
            "completed_documents": self.completed_documents,
            "is_complete": self.is_complete,
            "stages": {
                stage: {
                    "status": info.status.value,
                    "started_at": info.started_at.isoformat() if info.started_at else None,
                    "completed_at": info.completed_at.isoformat() if info.completed_at else None,
                    "metrics": info.metrics
                }
                for stage, info in self.stages.items()
            },
            "documents": [
                {
                    "unit_id": doc.unit_id,
                    "input_dir": doc.input_dir,
                    "purchase_notice_number": doc.purchase_notice_number,
                    "publication_date": doc.publication_date.isoformat() if doc.publication_date else None,
                    "files": doc.files,
                    "stages": doc.stages
                }
                for doc in self.documents
            ]
        }


@dataclass
class ProtocolMetrics:
    """
    Метрики по протоколам за день.

    Attributes:
        date: Дата в формате YYYY-MM-DD
        total_protocols: Общее количество протоколов
        pending_protocols: Ожидающих скачивания
        downloaded_protocols: Скачанных
        with_urls: С URL для скачивания
        without_urls: Без URL
        multi_url_protocols: С множественными URL
    """
    date: str
    total_protocols: int = 0
    pending_protocols: int = 0
    downloaded_protocols: int = 0
    with_urls: int = 0
    without_urls: int = 0
    multi_url_protocols: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Преобразует в словарь."""
        return {
            "date": self.date,
            "total_protocols": self.total_protocols,
            "pending_protocols": self.pending_protocols,
            "downloaded_protocols": self.downloaded_protocols,
            "with_urls": self.with_urls,
            "without_urls": self.without_urls,
            "multi_url_protocols": self.multi_url_protocols
        }


@dataclass
class ConcurrencyTestResult:
    """
    Результаты тестирования конкурентности.

    Attributes:
        concurrency_level: Уровень конкурентности (потоков)
        protocols_processed: Обработано протоколов
        files_downloaded: Скачано файлов
        files_failed: Неудачных скачиваний
        duration_seconds: Длительность теста
        throughput_protocols_per_sec: Протоколов в секунду
        throughput_files_per_sec: Файлов в секунду
        success_rate: Процент успеха
        error_types: Распределение ошибок по типам
    """
    concurrency_level: int
    protocols_processed: int = 0
    files_downloaded: int = 0
    files_failed: int = 0
    duration_seconds: float = 0.0
    throughput_protocols_per_sec: float = 0.0
    throughput_files_per_sec: float = 0.0
    success_rate: float = 0.0
    error_types: Dict[str, int] = field(default_factory=dict)

    @property
    def total_files(self) -> int:
        """Общее количество файлов."""
        return self.files_downloaded + self.files_failed

    def to_dict(self) -> Dict[str, Any]:
        """Преобразует в словарь."""
        return {
            "concurrency_level": self.concurrency_level,
            "protocols_processed": self.protocols_processed,
            "files_downloaded": self.files_downloaded,
            "files_failed": self.files_failed,
            "total_files": self.total_files,
            "duration_seconds": self.duration_seconds,
            "throughput_protocols_per_sec": self.throughput_protocols_per_sec,
            "throughput_files_per_sec": self.throughput_files_per_sec,
            "success_rate": self.success_rate,
            "error_types": self.error_types
        }

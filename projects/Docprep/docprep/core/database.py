"""
MongoDB интеграция для DocPrep.

Позволяет хранить состояния, метаданные и метрики в MongoDB для:
- Pipeline runs (запуски обработки)
- Unit states (состояния UNIT)
- Document metadata (метаданные файлов)
- Processing metrics (метрики операций)

MongoDB является опциональным компонентом — система работает без неё.
При недоступности MongoDB происходит fallback на файловый режим.

Использование:
    from docprep.core.database import get_database, DocPrepDatabase

    # Автоматическое создание из переменных окружения
    db = get_database()

    # Явное создание
    db = DocPrepDatabase(
        connection_string="mongodb://localhost:27018",  # Локальный MongoDB
        db_name="docling_metadata"  # База данных Docreciv
    )

    # Запись данных
    pipeline_id = db.start_pipeline({
        "input_dir": "/path/to/input",
        "output_dir": "/path/to/output",
        "max_cycles": 3
    })
"""
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import uuid

logger = logging.getLogger(__name__)

# Глобальный экземпляр базы данных
_database_instance: Optional["DocPrepDatabase"] = None


class DocPrepDatabase:
    """
    Клиент для MongoDB интеграции с DocPrep.

    Управляет подключением к MongoDB и предоставляет методы
    для записи состояний UNIT, метаданных документов и метрик pipeline.

    Attributes:
        client: MongoClient PyMongo
        db: База данных MongoDB
        pipeline_runs: Коллекция pipeline_runs
        unit_states: Коллекция unit_states
        document_metadata: Коллекция document_metadata
        processing_metrics: Коллекция processing_metrics
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        db_name: str = "docling_metadata",  # Унификация с Docreciv
        enabled: bool = True,
    ):
        """
        Инициализирует подключение к MongoDB.

        Args:
            connection_string: Строка подключения MongoDB (по умолчанию из MONGODB_URI)
            db_name: Имя базы данных (по умолчанию "docling_metadata" - unified с Docreciv)
            enabled: Если False, создаёт отключённый экземпляр
        """
        # Проверяем MONGODB_ENABLED: пустая строка = true по умолчанию
        mongo_enabled_env = os.getenv("MONGODB_ENABLED", "true").strip().lower()
        self.enabled = bool(enabled) and (mongo_enabled_env == "true" or mongo_enabled_env == "")

        if not self.enabled:
            logger.info("MongoDB integration disabled")
            self.client = None
            self.db = None
            self._collections = {}
            return

        try:
            import pymongo
            from pymongo import MongoClient, ASCENDING, DESCENDING
            self._pymongo = pymongo
            self._ASCENDING = ASCENDING
            self._DESCENDING = DESCENDING
        except ImportError:
            logger.warning(
                "pymongo not installed. MongoDB integration disabled. "
                "Install with: pip install pymongo"
            )
            self.enabled = False
            self.client = None
            self.db = None
            self._collections = {}
            return

        connection_string = connection_string or os.getenv(
            "MONGODB_URI", "mongodb://admin:password@localhost:27018/docling_metadata?authSource=admin"
        )

        try:
            self.client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000,  # 5 сек таймаут
                connectTimeoutMS=5000,
            )
            # Проверяем подключение
            self.client.admin.command('ping')
            self.db = self.client[db_name]

            # Инициализируем коллекции DocPrep
            self.pipeline_runs = self.db.pipeline_runs
            self.unit_states = self.db.unit_states
            self.document_metadata = self.db.document_metadata
            self.processing_metrics = self.db.processing_metrics

            # Статистика по этапам и trace операций
            self.stage_stats = self.db.stage_stats
            self.unit_traces = self.db.unit_traces

            # Коллекция protocols из docling_metadata (создаётся Docreciv)
            self.protocols = self.db.protocols

            self._collections = {
                "pipeline_runs": self.pipeline_runs,
                "unit_states": self.unit_states,
                "document_metadata": self.document_metadata,
                "processing_metrics": self.processing_metrics,
                "stage_stats": self.stage_stats,
                "unit_traces": self.unit_traces,
                "protocols": self.protocols,
            }

            # Создаём индексы
            self._create_indexes()

            logger.info(f"MongoDB connected: {db_name} at {connection_string}")

        except Exception as e:
            logger.warning(f"MongoDB connection failed: {e}. Using file-only mode.")
            self.enabled = False
            self.client = None
            self.db = None
            self._collections = {}

    def _create_indexes(self) -> None:
        """Создаёт индексы для производительности запросов."""
        if not self.enabled or self.db is None:
            return

        try:
            # pipeline_runs
            self.pipeline_runs.create_index([("start_time", self._DESCENDING)])
            self.pipeline_runs.create_index([("protocol_date", self._ASCENDING)])
            self.pipeline_runs.create_index([("status", self._ASCENDING)])

            # unit_states
            self.unit_states.create_index([("unit_id", self._ASCENDING)], unique=True)
            self.unit_states.create_index([("pipeline_id", self._ASCENDING)])
            self.unit_states.create_index([("current_state", self._ASCENDING)])
            self.unit_states.create_index([("protocol_date", self._ASCENDING)])

            # document_metadata
            self.document_metadata.create_index([("unit_id", self._ASCENDING)])
            self.document_metadata.create_index([("detected_type", self._ASCENDING)])

            # processing_metrics
            self.processing_metrics.create_index([("pipeline_id", self._ASCENDING)])
            self.processing_metrics.create_index([("timestamp", self._DESCENDING)])
            self.processing_metrics.create_index([("operation_type", self._ASCENDING)])

            # stage_stats
            self.stage_stats.create_index([("pipeline_id", self._ASCENDING), ("cycle", self._ASCENDING), ("stage", self._ASCENDING)])
            self.stage_stats.create_index([("timestamp", self._DESCENDING)])

            # unit_traces
            self.unit_traces.create_index([("pipeline_id", self._ASCENDING), ("unit_id", self._ASCENDING)])
            self.unit_traces.create_index([("unit_id", self._ASCENDING)])
            self.unit_traces.create_index([("timestamp", self._DESCENDING)])
            self.unit_traces.create_index([("status", self._ASCENDING)])

            # ★ protocols (из docling_metadata - создаётся Docreciv)
            # Индексы для trace системы и быстрого поиска
            try:
                self.protocols.create_index([("registrationNumber", self._ASCENDING)], name="registration_number_idx")
                self.protocols.create_index([("unit_id", self._ASCENDING)], name="unit_id_idx", unique=True)
                self.protocols.create_index([("purchaseNoticeNumber", self._ASCENDING)], name="purchase_notice_idx")
                # Индекс для trace.docprep оптимизации
                self.protocols.create_index([("trace.docreciv.unit_id", self._ASCENDING)], name="trace_docreciv_unit_idx")
            except Exception as idx_err:
                # Индексы могут уже существовать - игнорируем ошибку
                logger.debug(f"Protocols index creation note: {idx_err}")

            logger.debug("MongoDB indexes created")

        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")

    def is_connected(self) -> bool:
        """Проверяет, активно ли подключение к MongoDB."""
        return self.enabled and self.client is not None

    # ========================================================================
    # Pipeline runs
    # ========================================================================

    def start_pipeline(
        self,
        input_dir: str,
        output_dir: str,
        protocol_date: Optional[str] = None,
        max_cycles: int = 3,
        config: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Регистрирует начало pipeline run.

        Args:
            input_dir: Входная директория
            output_dir: Выходная директория
            protocol_date: Дата протокола (из имени директории)
            max_cycles: Максимальное количество циклов
            config: Дополнительная конфигурация

        Returns:
            pipeline_id: Уникальный идентификатор запуска
        """
        if not self.is_connected():
            return self._generate_pipeline_id()

        # Извлекаем protocol_date из input_dir если не указан
        if not protocol_date:
            protocol_date = self._extract_protocol_date(input_dir)

        pipeline_id = self._generate_pipeline_id()

        document = {
            "_id": pipeline_id,
            "start_time": datetime.now(timezone.utc),
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "protocol_date": protocol_date,
            "max_cycles": max_cycles,
            "config": config or {},
            "units_total": 0,
            "units_success": 0,
            "units_failed": 0,
            "status": "running",
            "metrics": {
                "files_by_category": {},
                "success_rate": 0.0,
                "exceptions_by_type": {},
            },
        }

        try:
            self.pipeline_runs.insert_one(document)
            logger.debug(f"Pipeline started: {pipeline_id}")
        except Exception as e:
            logger.warning(f"Failed to start pipeline in MongoDB: {e}")

        return pipeline_id

    def update_pipeline_metrics(
        self,
        pipeline_id: str,
        metrics: Dict[str, Any],
    ) -> None:
        """
        Обновляет метрики pipeline.

        Args:
            pipeline_id: Идентификатор pipeline
            metrics: Словарь с метриками (units_total, units_success, etc.)
        """
        if not self.is_connected():
            return

        try:
            # Вычисляем success_rate
            units_total = metrics.get("units_total", 0)
            units_success = metrics.get("units_success", 0)
            success_rate = units_success / units_total if units_total > 0 else 0.0

            update = {
                "$set": {
                    **{f"metrics.{k}": v for k, v in metrics.items()},
                    "metrics.success_rate": success_rate,
                }
            }

            if "units_total" in metrics:
                update["$set"]["units_total"] = metrics["units_total"]
            if "units_success" in metrics:
                update["$set"]["units_success"] = metrics["units_success"]
            if "units_failed" in metrics:
                update["$set"]["units_failed"] = metrics["units_failed"]

            self.pipeline_runs.update_one({"_id": pipeline_id}, update)
        except Exception as e:
            logger.warning(f"Failed to update pipeline metrics: {e}")

    def end_pipeline(
        self,
        pipeline_id: str,
        status: str = "completed",
        errors: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Завершает pipeline run.

        Args:
            pipeline_id: Идентификатор pipeline
            status: Статус завершения (completed, failed, partial)
            errors: Список ошибок (если есть)
        """
        if not self.is_connected():
            return

        try:
            end_time = datetime.now(timezone.utc)

            # Вычисляем длительность
            pipeline_doc = self.pipeline_runs.find_one({"_id": pipeline_id})
            duration_seconds = None
            if pipeline_doc and "start_time" in pipeline_doc:
                duration_seconds = (end_time - pipeline_doc["start_time"]).total_seconds()

            update = {
                "$set": {
                    "end_time": end_time,
                    "status": status,
                }
            }

            if duration_seconds is not None:
                update["$set"]["duration_seconds"] = duration_seconds

            if errors:
                update["$set"]["errors"] = errors

            self.pipeline_runs.update_one({"_id": pipeline_id}, update)
            logger.debug(f"Pipeline ended: {pipeline_id} with status {status}")
        except Exception as e:
            logger.warning(f"Failed to end pipeline: {e}")

    def get_pipeline(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """
        Получает информацию о pipeline run.

        Args:
            pipeline_id: Идентификатор pipeline

        Returns:
            Документ pipeline или None
        """
        if not self.is_connected():
            return None

        try:
            return self.pipeline_runs.find_one({"_id": pipeline_id})
        except Exception as e:
            logger.warning(f"Failed to get pipeline: {e}")
            return None

    def list_pipelines(
        self,
        protocol_date: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Возвращает список pipeline runs.

        Args:
            protocol_date: Фильтр по дате протокола
            status: Фильтр по статусу
            limit: Максимальное количество результатов

        Returns:
            Список pipeline документов
        """
        if not self.is_connected():
            return []

        try:
            query = {}
            if protocol_date:
                query["protocol_date"] = protocol_date
            if status:
                query["status"] = status

            cursor = self.pipeline_runs.find(query).sort("start_time", -1).limit(limit)
            return list(cursor)
        except Exception as e:
            logger.warning(f"Failed to list pipelines: {e}")
            return []

    # ========================================================================
    # Unit states
    # ========================================================================

    def write_unit_state(self, manifest: Dict[str, Any]) -> None:
        """
        Записывает или обновляет состояние UNIT из manifest.

        Args:
            manifest: Словарь manifest.json
        """
        if not self.is_connected():
            return

        try:
            unit_id = manifest.get("unit_id")
            if not unit_id:
                return

            processing = manifest.get("processing", {})
            state_machine = manifest.get("state_machine", {})

            document = {
                "unit_id": unit_id,
                "current_state": state_machine.get("current_state", "UNKNOWN"),
                "state_trace": state_machine.get("state_trace", []),
                "processing_cycle": processing.get("current_cycle", 0),
                "final_cluster": processing.get("final_cluster"),
                "route": processing.get("route"),
                "file_count": len(manifest.get("files", [])),
                "updated_at": datetime.now(timezone.utc),
            }

            # Добавляем pipeline_id если есть в applied_operations
            applied_ops = manifest.get("applied_operations", [])
            if applied_ops:
                # Берём pipeline_id из последней операции
                last_op = applied_ops[-1]
                document["pipeline_id"] = last_op.get("pipeline_id")

            # Добавляем protocol_date если есть
            if "protocol_date" in manifest:
                document["protocol_date"] = manifest["protocol_date"]

            # Upsert по unit_id
            self.unit_states.update_one(
                {"unit_id": unit_id},
                {"$set": document, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
                upsert=True,
            )

        except Exception as e:
            logger.warning(f"Failed to write unit state for {manifest.get('unit_id')}: {e}")

    def get_unit_state(self, unit_id: str) -> Optional[Dict[str, Any]]:
        """
        Получает состояние UNIT.

        Args:
            unit_id: Идентификатор UNIT

        Returns:
            Документ состояния UNIT или None
        """
        if not self.is_connected():
            return None

        try:
            return self.unit_states.find_one({"unit_id": unit_id})
        except Exception as e:
            logger.warning(f"Failed to get unit state: {e}")
            return None

    def get_units_by_pipeline(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """
        Возвращает все UNIT для pipeline.

        Args:
            pipeline_id: Идентификатор pipeline

        Returns:
            Список документов UNIT
        """
        if not self.is_connected():
            return []

        try:
            return list(self.unit_states.find({"pipeline_id": pipeline_id}))
        except Exception as e:
            logger.warning(f"Failed to get units by pipeline: {e}")
            return []

    # ========================================================================
    # Document metadata
    # ========================================================================

    def write_document_metadata(
        self,
        unit_id: str,
        files: List[Dict[str, Any]],
        pipeline_id: Optional[str] = None,
    ) -> None:
        """
        Записывает метаданные файлов UNIT.

        Args:
            unit_id: Идентификатор UNIT
            files: Список файлов из manifest.json
            pipeline_id: Идентификатор pipeline (опционально)
        """
        if not self.is_connected():
            return

        try:
            # Удаляем старые записи для этого UNIT
            self.document_metadata.delete_many({"unit_id": unit_id})

            # Вставляем новые записи
            documents = []
            for idx, file_info in enumerate(files):
                doc = {
                    "unit_id": unit_id,
                    "file_index": idx,
                    "original_name": file_info.get("original_name"),
                    "current_name": file_info.get("current_name"),
                    "detected_type": file_info.get("detected_type"),
                    "needs_ocr": file_info.get("needs_ocr", False),
                    "mime_type": file_info.get("mime_type"),
                    "size_bytes": file_info.get("size_bytes"),
                    "checksum_sha256": file_info.get("checksum_sha256"),
                    "transformations": file_info.get("transformations", []),
                }

                if pipeline_id:
                    doc["pipeline_id"] = pipeline_id

                documents.append(doc)

            if documents:
                self.document_metadata.insert_many(documents)

        except Exception as e:
            logger.warning(f"Failed to write document metadata for {unit_id}: {e}")

    def get_documents_by_unit(self, unit_id: str) -> List[Dict[str, Any]]:
        """
        Возвращает метаданные файлов для UNIT.

        Args:
            unit_id: Идентификатор UNIT

        Returns:
            Список документов файлов
        """
        if not self.is_connected():
            return []

        try:
            return list(
                self.document_metadata.find({"unit_id": unit_id}).sort("file_index", 1)
            )
        except Exception as e:
            logger.warning(f"Failed to get documents for unit: {e}")
            return []

    # ========================================================================
    # Processing metrics
    # ========================================================================

    def write_processing_metric(
        self,
        pipeline_id: str,
        operation_type: str,
        cycle: int,
        duration_ms: Optional[int] = None,
        units_processed: int = 0,
        units_failed: int = 0,
        worker_count: Optional[int] = None,
        additional_metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Записывает метрику операции обработки.

        Args:
            pipeline_id: Идентификатор pipeline
            operation_type: Тип операции (classify, convert, extract, normalize, merge)
            cycle: Номер цикла
            duration_ms: Длительность в миллисекундах
            units_processed: Количество обработанных UNIT
            units_failed: Количество_failed UNIT
            worker_count: Количество использованных workers
            additional_metrics: Дополнительные метрики
        """
        if not self.is_connected():
            return

        try:
            document = {
                "pipeline_id": pipeline_id,
                "operation_type": operation_type,
                "cycle": cycle,
                "timestamp": datetime.now(timezone.utc),
                "duration_ms": duration_ms,
                "units_processed": units_processed,
                "units_failed": units_failed,
                "worker_count": worker_count,
            }

            if additional_metrics:
                document["additional_metrics"] = additional_metrics

            self.processing_metrics.insert_one(document)

        except Exception as e:
            logger.warning(f"Failed to write processing metric: {e}")

    def get_metrics_by_pipeline(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """
        Возвращает все метрики для pipeline.

        Args:
            pipeline_id: Идентификатор pipeline

        Returns:
            Список метрик
        """
        if not self.is_connected():
            return []

        try:
            return list(
                self.processing_metrics.find({"pipeline_id": pipeline_id}).sort("timestamp", 1)
            )
        except Exception as e:
            logger.warning(f"Failed to get metrics for pipeline: {e}")
            return []

    # ========================================================================
    # Агрегация и статистика
    # ========================================================================

    def get_pipeline_summary(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """
        Возвращает сводную информацию о pipeline с метриками.

        Args:
            pipeline_id: Идентификатор pipeline

        Returns:
            Сводная информация или None
        """
        if not self.is_connected():
            return None

        try:
            pipeline = self.get_pipeline(pipeline_id)
            if not pipeline:
                return None

            # Получаем статистику по состояниям UNIT
            unit_stats = list(
                self.unit_states.aggregate([
                    {"$match": {"pipeline_id": pipeline_id}},
                    {"$group": {
                        "_id": "$current_state",
                        "count": {"$sum": 1}
                    }}
                ])
            )

            # Получаем метрики по операциям
            operation_metrics = list(
                self.processing_metrics.aggregate([
                    {"$match": {"pipeline_id": pipeline_id}},
                    {"$group": {
                        "_id": "$operation_type",
                        "total_units": {"$sum": "$units_processed"},
                        "failed_units": {"$sum": "$units_failed"},
                        "total_duration_ms": {"$sum": "$duration_ms"},
                        "count": {"$sum": 1}
                    }}
                ])
            )

            return {
                "pipeline": pipeline,
                "unit_states": {s["_id"]: s["count"] for s in unit_stats},
                "operation_metrics": operation_metrics,
            }

        except Exception as e:
            logger.warning(f"Failed to get pipeline summary: {e}")
            return None

    def compare_pipelines(
        self,
        pipeline_id1: str,
        pipeline_id2: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Сравнивает два pipeline run.

        Args:
            pipeline_id1: Первый pipeline
            pipeline_id2: Второй pipeline

        Returns:
            Сравнительная информация или None
        """
        if not self.is_connected():
            return None

        try:
            p1 = self.get_pipeline(pipeline_id1)
            p2 = self.get_pipeline(pipeline_id2)

            if not p1 or not p2:
                return None

            return {
                "pipeline1": {
                    "id": pipeline_id1,
                    "protocol_date": p1.get("protocol_date"),
                    "duration_seconds": p1.get("duration_seconds"),
                    "units_total": p1.get("units_total"),
                    "success_rate": p1.get("metrics", {}).get("success_rate", 0),
                },
                "pipeline2": {
                    "id": pipeline_id2,
                    "protocol_date": p2.get("protocol_date"),
                    "duration_seconds": p2.get("duration_seconds"),
                    "units_total": p2.get("units_total"),
                    "success_rate": p2.get("metrics", {}).get("success_rate", 0),
                },
                "diff": {
                    "duration_seconds": (
                        (p2.get("duration_seconds") or 0) - (p1.get("duration_seconds") or 0)
                    ),
                    "units_total": (p2.get("units_total") or 0) - (p1.get("units_total") or 0),
                    "success_rate": (
                        p2.get("metrics", {}).get("success_rate", 0) -
                        p1.get("metrics", {}).get("success_rate", 0)
                    ),
                },
            }

        except Exception as e:
            logger.warning(f"Failed to compare pipelines: {e}")
            return None

    # ========================================================================
    # Управление данными
    # ========================================================================

    def delete_pipeline(self, pipeline_id: str) -> bool:
        """
        Удаляет pipeline и все связанные данные.

        Args:
            pipeline_id: Идентификатор pipeline

        Returns:
            True если успешно удалён
        """
        if not self.is_connected():
            return False

        try:
            # Удаляем pipeline
            self.pipeline_runs.delete_one({"_id": pipeline_id})
            # Удаляем связанные UNIT states
            self.unit_states.delete_many({"pipeline_id": pipeline_id})
            # Удаляем связанные метрики
            self.processing_metrics.delete_many({"pipeline_id": pipeline_id})

            logger.info(f"Pipeline {pipeline_id} deleted")
            return True

        except Exception as e:
            logger.warning(f"Failed to delete pipeline: {e}")
            return False

    def cleanup_old_records(self, days: int = 90) -> int:
        """
        Удаляет старые записи (retention policy).

        Args:
            days: Удалить записи старше указанного количества дней

        Returns:
            Количество удалённых pipeline
        """
        if not self.is_connected():
            return 0

        try:
            from datetime import timedelta

            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

            # Находим старые pipeline
            old_pipeline_ids = [
                p["_id"] for p in self.pipeline_runs.find(
                    {"start_time": {"$lt": cutoff_date}},
                    {"_id": 1}
                )
            ]

            count = len(old_pipeline_ids)
            for pipeline_id in old_pipeline_ids:
                self.delete_pipeline(pipeline_id)

            logger.info(f"Cleaned up {count} old pipelines (older than {days} days)")
            return count

        except Exception as e:
            logger.warning(f"Failed to cleanup old records: {e}")
            return 0

    # ========================================================================
    # Связь с docling_metadata.protocols (Docreciv)
    # ========================================================================

    def get_protocol_by_unit_id(self, unit_id: str) -> Optional[Dict[str, Any]]:
        """
        Получает запись протокола из docling_metadata.protocols по unit_id.

        Args:
            unit_id: Идентификатор UNIT (записан Docreciv)

        Returns:
            Документ протокола или None
        """
        if not self.is_connected():
            return None

        try:
            return self.protocols.find_one({"unit_id": unit_id})
        except Exception as e:
            logger.warning(f"Failed to get protocol for {unit_id}: {e}")
            return None

    def link_to_protocol_collection(
        self,
        unit_id: str,
        manifest: Dict[str, Any],
        pipeline_id: Optional[str] = None,
    ) -> bool:
        """
        Создаёт/обновляет связь с docling_metadata.protocols.

        Обновляет запись протокола (созданную Docreciv) с информацией
        об обработке в DocPrep, обеспечивая двустороннюю связь и сквозной трейсинг.

        Args:
            unit_id: Идентификатор UNIT
            manifest: Словарь manifest.json
            pipeline_id: Опциональный ID текущего pipeline

        Returns:
            True если обновление успешно
        """
        if not self.is_connected():
            return False

        try:
            # ★ Извлекаем registrationNumber из manifest для trace системы
            registration_number = manifest.get("registration_number", "")

            # Подготавливаем данные для обновления
            update_data = {
                "$set": {
                    "docprep_processed": True,
                    "docprep_state": manifest.get("state_machine", {}).get("current_state"),
                    "docprep_route": manifest.get("processing", {}).get("route"),
                    "docprep_updated_at": datetime.now(timezone.utc),
                    "docprep_schema_version": manifest.get("schema_version"),
                    "protocol_date": manifest.get("protocol_date"),
                    "file_count": len(manifest.get("files", [])),
                }
            }

            # ★ Добавляем registrationNumber если есть (для trace системы)
            if registration_number:
                update_data["$set"]["registrationNumber"] = registration_number

            # ★ Добавляем trace.docprep для сквозного трейсинга
            update_data["$set"]["trace.docprep"] = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "state": manifest.get("state_machine", {}).get("current_state"),
                "route": manifest.get("processing", {}).get("route"),
                "cycle": manifest.get("processing", {}).get("current_cycle", 1),
                "schema_version": manifest.get("schema_version"),
            }

            if pipeline_id:
                update_data["$set"]["docprep_pipeline_id"] = pipeline_id

            # ★ Добавляем запись в history для хронологии событий
            update_data["$push"] = {
                "history": {
                    "component": "docprep",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "event": f"processed_{manifest.get('state_machine', {}).get('current_state', 'unknown')}",
                    "pipeline_id": pipeline_id,
                }
            }

            # Обновляем существующую запись (созданную Docreciv)
            result = self.protocols.update_one(
                {"unit_id": unit_id},
                update_data
            )

            if result.matched_count > 0:
                logger.debug(f"Linked {unit_id} to protocols collection with trace info")
                return True
            else:
                logger.warning(f"No protocol found for unit_id {unit_id}")
                return False

        except Exception as e:
            logger.warning(f"Failed to link {unit_id} to protocols: {e}")
            return False

    # ========================================================================
    # Статистика по этапам обработки
    # ========================================================================

    def write_stage_stats(
        self,
        pipeline_id: str,
        cycle: int,
        stage: str,
        stats: Dict[str, Any]
    ) -> None:
        """
        Записывает статистику этапа обработки.

        Args:
            pipeline_id: ID pipeline
            cycle: Номер цикла (1, 2, 3)
            stage: Название этапа (classifier, convert, extract, normalize, merge)
            stats: Словарь со статистикой этапа
        """
        if not self.is_connected():
            return

        try:
            document = {
                "pipeline_id": pipeline_id,
                "cycle": cycle,
                "stage": stage,
                "timestamp": datetime.now(timezone.utc),
                **stats
            }

            self.stage_stats.insert_one(document)
            logger.debug(f"Written stage stats for {stage} (cycle {cycle})")
        except Exception as e:
            logger.warning(f"Failed to write stage stats for {stage}: {e}")

    def write_unit_trace(
        self,
        unit_id: str,
        pipeline_id: str,
        cycle: int,
        stage: str,
        operation: str,
        duration_ms: int,
        status: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Записывает trace операции над UNIT.

        Args:
            unit_id: ID UNIT
            pipeline_id: ID pipeline
            cycle: Номер цикла
            stage: Название этапа
            operation: Операция (classify, convert, extract, normalize, merge)
            duration_ms: Длительность в миллисекундах
            status: Статус (success, failed, skipped)
            metadata: Дополнительные метаданные
        """
        if not self.is_connected():
            return

        try:
            document = {
                "unit_id": unit_id,
                "pipeline_id": pipeline_id,
                "cycle": cycle,
                "stage": stage,
                "operation": operation,
                "duration_ms": duration_ms,
                "status": status,
                "timestamp": datetime.now(timezone.utc),
                "metadata": metadata or {}
            }

            self.unit_traces.insert_one(document)
            logger.debug(f"Written unit trace for {unit_id}: {operation}")
        except Exception as e:
            logger.warning(f"Failed to write unit trace for {unit_id}: {e}")

    def get_pipeline_full_report(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Возвращает полный отчёт по pipeline со всеми стадиями.

        Args:
            pipeline_id: ID pipeline

        Returns:
            Словарь с полной статистикой pipeline
        """
        if not self.is_connected():
            return {"error": "Not connected to MongoDB"}

        try:
            # Получаем информацию о pipeline
            pipeline = self.pipeline_runs.find_one({"pipeline_id": pipeline_id})
            if not pipeline:
                return {"error": "Pipeline not found"}

            # Получаем статистику по этапам
            stage_stats = list(self.stage_stats.find({"pipeline_id": pipeline_id}).sort("cycle"))

            # Группируем по циклам
            cycles = {}
            for stat in stage_stats:
                cycle_num = stat.get("cycle")
                if cycle_num not in cycles:
                    cycles[cycle_num] = {"cycle": cycle_num, "stages": {}}

                stage_name = stat.get("stage")
                # Удаляем служебные поля
                stat_copy = {k: v for k, v in stat.items()
                            if k not in ["pipeline_id", "_id", "timestamp"]}
                cycles[cycle_num]["stages"][stage_name] = stat_copy

            # Получаем trace операций (опционально, только агрегированные данные)
            total_traces = self.unit_traces.count_documents({"pipeline_id": pipeline_id})
            success_traces = self.unit_traces.count_documents({"pipeline_id": pipeline_id, "status": "success"})
            failed_traces = self.unit_traces.count_documents({"pipeline_id": pipeline_id, "status": "failed"})

            # Формируем итоговую статистику
            report = {
                "pipeline_id": pipeline_id,
                "input_dir": pipeline.get("input_dir"),
                "output_dir": pipeline.get("output_dir"),
                "protocol_date": pipeline.get("protocol_date"),
                "status": pipeline.get("status"),
                "started_at": pipeline.get("started_at"),
                "completed_at": pipeline.get("completed_at"),
                "max_cycles": pipeline.get("max_cycles"),
                "cycles": sorted(cycles.values(), key=lambda x: x.get("cycle", 0)),
                "traces": {
                    "total": total_traces,
                    "success": success_traces,
                    "failed": failed_traces
                },
                "metrics": pipeline.get("metrics", {})
            }

            return report
        except Exception as e:
            logger.error(f"Failed to get pipeline report: {e}")
            return {"error": str(e)}

    def get_unit_traces(self, pipeline_id: str, unit_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Возвращает trace операций для UNIT или всего pipeline.

        Args:
            pipeline_id: ID pipeline
            unit_id: Опциональный ID UNIT для фильтрации

        Returns:
            Список trace записей
        """
        if not self.is_connected():
            return []

        try:
            query = {"pipeline_id": pipeline_id}
            if unit_id:
                query["unit_id"] = unit_id

            traces = list(self.unit_traces.find(query).sort("timestamp"))
            # Конвертируем ObjectId в строку
            for trace in traces:
                if "_id" in trace:
                    trace["_id"] = str(trace["_id"])
            return traces
        except Exception as e:
            logger.error(f"Failed to get unit traces: {e}")
            return []

    # ========================================================================
    # PipelineTracker integration (Docreciv)
    # ========================================================================

    def create_pipeline_run(
        self,
        batch_date: str,
        stage: Any,  # docreciv.pipeline.events.Stage
        config: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Создаёт PipelineRun через Docreciv PipelineTracker.

        Args:
            batch_date: Дата партии в формате YYYY-MM-DD
            stage: Stage из docreciv.pipeline.events (Stage.DOCPREP)
            config: Конфигурация pipeline

        Returns:
            run_id или None если не подключён к MongoDB
        """
        if not self.is_connected():
            return None

        try:
            from docreciv.pipeline.tracker import get_tracker

            tracker = get_tracker()
            run = tracker.create_run(
                batch_date=batch_date,
                stage=stage,
                config=config or {}
            )
            return run.run_id if run else None

        except ImportError:
            logger.warning("Docreciv PipelineTracker not available")
            return None
        except Exception as e:
            logger.warning(f"Failed to create pipeline run: {e}")
            return None


    def record_unit_event(
        self,
        unit_id: str,
        run_id: str,
        registration_number: str,
        event_type: Any,  # docreciv.pipeline.events.EventType
        stage: Any,  # docreciv.pipeline.events.Stage
        status: Any,  # docreciv.pipeline.events.EventStatus
        metrics: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        duration_ms: Optional[int] = None
    ) -> Optional[Any]:  # ObjectId
        """
        Записывает UnitEvent через Docreciv PipelineTracker.

        Args:
            unit_id: Идентификатор UNIT
            run_id: PipelineRun ID
            registration_number: Регистрационный номер (primary trace ID)
            event_type: EventType из docreciv.pipeline.events
            stage: Stage из docreciv.pipeline.events
            status: EventStatus из docreciv.pipeline.events
            metrics: Метрики операции
            error: Текст ошибки (если есть)
            duration_ms: Длительность в миллисекундах

        Returns:
            ObjectId события или None
        """
        if not self.is_connected():
            return None

        try:
            from docreciv.pipeline.tracker import get_tracker

            tracker = get_tracker()
            return tracker.record_event(
                unit_id=unit_id,
                run_id=run_id,
                registrationNumber=registration_number,
                event_type=event_type,
                stage=stage,
                status=status,
                metrics=metrics,
                error=error,
                duration_ms=duration_ms
            )

        except ImportError:
            logger.warning("Docreciv PipelineTracker not available")
            return None
        except Exception as e:
            logger.warning(f"Failed to record unit event: {e}")
            return None


    def update_pipeline_run(
        self,
        run_id: str,
        status: Any = None,  # Optional[docreciv.pipeline.events.RunStatus]
        metrics: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> bool:
        """
        Обновляет PipelineRun через Docreciv PipelineTracker.

        Args:
            run_id: PipelineRun ID
            status: Новый статус (RunStatus)
            metrics: Метрики для обновления
            error: Текст ошибки (если есть)

        Returns:
            True если обновление успешно
        """
        if not self.is_connected():
            return False

        try:
            from docreciv.pipeline.tracker import get_tracker

            tracker = get_tracker()
            result = tracker.update_run(
                run_id=run_id,
                status=status,
                metrics=metrics,
                error=error
            )
            return result is not None

        except ImportError:
            logger.warning("Docreciv PipelineTracker not available")
            return False
        except Exception as e:
            logger.warning(f"Failed to update pipeline run: {e}")
            return False


    def get_pipeline_run_status(
        self,
        run_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Получает статус PipelineRun через Docreciv PipelineTracker.

        Args:
            run_id: PipelineRun ID

        Returns:
            Словарь со статусом или None
        """
        if not self.is_connected():
            return None

        try:
            from docreciv.pipeline.tracker import get_tracker

            tracker = get_tracker()
            return tracker.get_run_status(run_id)

        except ImportError:
            logger.warning("Docreciv PipelineTracker not available")
            return None
        except Exception as e:
            logger.warning(f"Failed to get pipeline run status: {e}")
            return None

    # ========================================================================
    # Вспомогательные методы
    # ========================================================================

    def _generate_pipeline_id(self) -> str:
        """Генерирует уникальный идентификатор pipeline."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique = uuid.uuid4().hex[:8]
        return f"run_{timestamp}_{unique}"

    def _extract_protocol_date(self, path_str: str) -> Optional[str]:
        """Извлекает дату протокола из пути (формат YYYY-MM-DD)."""
        import re
        # Ищем паттерн даты в пути
        match = re.search(r'\d{4}-\d{2}-\d{2}', path_str)
        return match.group(0) if match else None

    def close(self) -> None:
        """Закрывает подключение к MongoDB."""
        if self.client:
            self.client.close()
            logger.debug("MongoDB connection closed")

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support."""
        self.close()


# ========================================================================
# Глобальные функции доступа
# ========================================================================

def get_database(
    connection_string: Optional[str] = None,
    db_name: str = "docling_metadata",  # Унификация с Docreciv
    force_new: bool = False,
) -> DocPrepDatabase:
    """
    Возвращает глобальный экземпляр DocPrepDatabase.

    Args:
        connection_string: Строка подключения MongoDB
        db_name: Имя базы данных
        force_new: Создать новый экземпляр

    Returns:
        Экземпляр DocPrepDatabase
    """
    global _database_instance

    if force_new or _database_instance is None:
        _database_instance = DocPrepDatabase(
            connection_string=connection_string,
            db_name=db_name,
        )

    return _database_instance


def set_database(db: DocPrepDatabase) -> None:
    """
    Устанавливает глобальный экземпляр DocPrepDatabase.

    Args:
        db: Экземпляр DocPrepDatabase
    """
    global _database_instance
    _database_instance = db


def is_mongobd_enabled() -> bool:
    """
    Проверяет, включена ли MongoDB интеграция.

    Returns:
        True если MongoDB доступна и включена
    """
    db = get_database()
    return db.is_connected()

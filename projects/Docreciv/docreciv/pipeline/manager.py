"""
Менеджер для управления pipeline метаданными.

Отвечает за:
- Создание и обновление pipeline runs в MongoDB
- Запись JSON contract файлов
- Отслеживание статусов документов через этапы обработки
"""

import os
import json
import uuid
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

from pymongo import MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError

from .models import (
    PipelineRun,
    PipelineDocument,
    StageStatus,
    PipelineStage
)

logger = logging.getLogger(__name__)


class PipelineManager:
    """
    Менеджер для управления pipeline метаданными.

    Хранит метаданные в MongoDB (collection: pipeline_runs)
    и создаёт JSON contract файлы для использования downstream компонентами.
    """

    COLLECTION_NAME = "pipeline_runs"

    # Индексы для коллекции
    INDEXES = [
        ([("run_id", 1)], {"unique": True}, "run_id_idx"),
        ([("batch_date", -1)], {}, "batch_date_idx"),
        ([("stages.download.status", 1)], {}, "download_status_idx"),
        ([("documents.unit_id", 1)], {}, "document_unit_id_idx"),
        ([("created_at", -1)], {}, "created_at_idx"),
    ]

    def __init__(
        self,
        mongo_client: MongoClient,
        db_name: str = "docling_metadata",
        base_dir: Path = Path("/home/pak/Processing data")
    ):
        """
        Инициализация менеджера.

        Args:
            mongo_client: Клиент MongoDB
            db_name: Имя базы данных
            base_dir: Базовая директория для Processing data
        """
        self.mongo = mongo_client
        self.db = mongo_client[db_name]
        self.collection = self.db[self.COLLECTION_NAME]
        self.base_dir = Path(base_dir)
        self.logger = logger

        # Создаём индексы при инициализации
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        """Создаёт индексы в коллекции."""
        for index_spec in self.INDEXES:
            try:
                keys, options, name = index_spec
                options["name"] = name
                self.collection.create_index(keys, **options)
                self.logger.debug(f"Created index: {name}")
            except Exception as e:
                self.logger.warning(f"Could not create index {index_spec[2]}: {e}")

    def generate_run_id(self, batch_date: str) -> str:
        """
        Генерирует уникальный run_id.

        Args:
            batch_date: Дата партии в формате YYYY-MM-DD

        Returns:
            run_id в формате RUN_YYYYMMDD_<uuid>
        """
        date_part = batch_date.replace("-", "")
        unique_part = uuid.uuid4().hex[:8]
        return f"RUN_{date_part}_{unique_part}"

    def create_run(
        self,
        batch_date: str,
        run_id: Optional[str] = None
    ) -> PipelineRun:
        """
        Создаёт новый pipeline run.

        Args:
            batch_date: Дата обрабатываемой партии (YYYY-MM-DD)
            run_id: Опциональный run_id (генерируется если не указан)

        Returns:
            Созданный PipelineRun
        """
        if run_id is None:
            run_id = self.generate_run_id(batch_date)

        run = PipelineRun(
            run_id=run_id,
            batch_date=batch_date
        )

        # Сохраняем в MongoDB
        try:
            self.collection.insert_one(run.to_dict())
            self.logger.info(f"Created pipeline run: {run_id} for date {batch_date}")
        except DuplicateKeyError:
            self.logger.warning(f"Pipeline run {run_id} already exists")

        return run

    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        """
        Загружает pipeline run из MongoDB.

        Args:
            run_id: Идентификатор запуска

        Returns:
            PipelineRun или None если не найден
        """
        doc = self.collection.find_one({"run_id": run_id})
        if not doc:
            return None

        return self._dict_to_run(doc)

    def _dict_to_run(self, doc: Dict[str, Any]) -> PipelineRun:
        """Преобразует документ из MongoDB в PipelineRun."""
        from .models import PipelineStageInfo, PipelineDocument

        # Парсим stages
        stages = {}
        for stage_name, stage_data in doc.get("stages", {}).items():
            info = PipelineStageInfo(
                status=StageStatus(stage_data.get("status", "pending")),
                started_at=datetime.fromisoformat(stage_data["started_at"]) if stage_data.get("started_at") else None,
                completed_at=datetime.fromisoformat(stage_data["completed_at"]) if stage_data.get("completed_at") else None,
                metrics=stage_data.get("metrics", {}),
                error=stage_data.get("error")
            )
            stages[stage_name] = info

        # Парсим documents
        documents = []
        for doc_data in doc.get("documents", []):
            pub_date = datetime.fromisoformat(doc_data["publication_date"]) if doc_data.get("publication_date") else None
            load_date = datetime.fromisoformat(doc_data["load_date"]) if doc_data.get("load_date") else None

            document = PipelineDocument(
                unit_id=doc_data["unit_id"],
                record_id=doc_data["record_id"],
                purchase_notice_number=doc_data["purchase_notice_number"],
                publication_date=pub_date,
                load_date=load_date,
                input_dir=doc_data.get("input_dir"),
                stages=doc_data.get("stages", {}),
                files=doc_data.get("files", [])
            )
            documents.append(document)

        run = PipelineRun(
            run_id=doc["run_id"],
            batch_date=doc["batch_date"],
            stages=stages,
            documents=documents,
            created_at=datetime.fromisoformat(doc["created_at"]),
            updated_at=datetime.fromisoformat(doc["updated_at"]),
            contract_version=doc.get("contract_version", "1.0")
        )

        return run

    def update_stage(
        self,
        run_id: str,
        stage: str,
        status: str,
        metrics: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> bool:
        """
        Обновляет статус этапа в MongoDB.

        Args:
            run_id: Идентификатор запуска
            stage: Имя этапа (download, docprep, docling, llm_qaenrich)
            status: Новый статус
            metrics: Метрики этапа
            error: Сообщение об ошибке

        Returns:
            True если обновление успешно
        """
        run = self.get_run(run_id)
        if not run:
            self.logger.error(f"Pipeline run {run_id} not found")
            return False

        run.update_stage(stage, StageStatus(status), metrics, error)

        # Обновляем в MongoDB
        result = self.collection.update_one(
            {"run_id": run_id},
            {
                "$set": {
                    f"stages.{stage}": run.get_stage(stage).to_dict(),
                    "updated_at": datetime.utcnow().isoformat()
                }
            }
        )

        # Обновляем JSON contract файл
        self.write_contract(run_id)

        return result.modified_count > 0

    def add_documents(
        self,
        run_id: str,
        documents: List[PipelineDocument]
    ) -> bool:
        """
        Добавляет документы в pipeline run.

        Args:
            run_id: Идентификатор запуска
            documents: Список документов для добавления

        Returns:
            True если добавление успешно
        """
        run = self.get_run(run_id)
        if not run:
            self.logger.error(f"Pipeline run {run_id} not found")
            return False

        run.add_documents(documents)

        # Подготавливаем документы для MongoDB
        docs_to_add = [doc.to_dict() for doc in documents]

        # Обновляем в MongoDB
        result = self.collection.update_one(
            {"run_id": run_id},
            {
                "$push": {"documents": {"$each": docs_to_add}},
                "$set": {"updated_at": datetime.utcnow().isoformat()}
            }
        )

        return result.modified_count > 0

    def update_document_stage(
        self,
        run_id: str,
        unit_id: str,
        stage: str,
        status: str
    ) -> bool:
        """
        Обновляет статус этапа для конкретного документа.

        Args:
            run_id: Идентификатор запуска
            unit_id: Идентификатор документа
            stage: Имя этапа
            status: Новый статус

        Returns:
            True если обновление успешно
        """
        run = self.get_run(run_id)
        if not run:
            self.logger.error(f"Pipeline run {run_id} not found")
            return False

        run.update_document_stage(unit_id, stage, status)

        # Обновляем в MongoDB
        result = self.collection.update_one(
            {"run_id": run_id, "documents.unit_id": unit_id},
            {
                "$set": {
                    f"documents.$.{stage}": status,
                    "updated_at": datetime.utcnow().isoformat()
                }
            }
        )

        return result.modified_count > 0

    def write_contract(self, run_id: str) -> Optional[Path]:
        """
        Записывает JSON contract в директорию обработки.

        Args:
            run_id: Идентификатор запуска

        Returns:
            Путь к созданному файлу или None
        """
        run = self.get_run(run_id)
        if not run:
            self.logger.error(f"Pipeline run {run_id} not found")
            return None

        # Формируем путь к файлу contract
        contract_dir = self.base_dir / run.batch_date / "Input"
        contract_dir.mkdir(parents=True, exist_ok=True)

        contract_file = contract_dir / "pipeline.contract.json"

        # Записываем JSON
        try:
            with open(contract_file, "w", encoding="utf-8") as f:
                json.dump(run.to_contract_json(), f, ensure_ascii=False, indent=2)

            self.logger.info(f"Written contract file: {contract_file}")
            return contract_file

        except Exception as e:
            self.logger.error(f"Failed to write contract file: {e}")
            return None

    def create_run_from_protocols(
        self,
        batch_date: str,
        protocols: List[Dict[str, Any]],
        run_id: Optional[str] = None
    ) -> PipelineRun:
        """
        Создаёт pipeline run из списка протоколов.

        Args:
            batch_date: Дата партии
            protocols: Список протоколов из MongoDB
            run_id: Опциональный run_id

        Returns:
            Созданный PipelineRun
        """
        if run_id is None:
            run_id = self.generate_run_id(batch_date)

        run = PipelineRun(
            run_id=run_id,
            batch_date=batch_date
        )

        # Преобразуем протоколы в PipelineDocument
        documents = []
        for protocol in protocols:
            # Используем loadDate как основную дату для всех операций
            load_date = protocol.get("loadDate")
            pub_date = load_date  # Для совместимости с publication_date

            doc = PipelineDocument(
                unit_id=protocol.get("unit_id", ""),
                record_id=str(protocol.get("_id", "")),
                purchase_notice_number=protocol.get("purchaseInfo", {}).get("purchaseNoticeNumber", ""),
                publication_date=pub_date if isinstance(pub_date, datetime) else None,
                load_date=load_date if isinstance(load_date, datetime) else None,
                input_dir=None,  # Заполнится после скачивания
                files=[],
                stages={}
            )
            documents.append(doc)

        run.add_documents(documents)

        # Сохраняем в MongoDB
        try:
            self.collection.insert_one(run.to_dict())
            self.logger.info(f"Created pipeline run {run_id} with {len(documents)} documents")
        except DuplicateKeyError:
            self.logger.warning(f"Pipeline run {run_id} already exists, updating...")
            self.collection.update_one(
                {"run_id": run_id},
                {"$set": run.to_dict()}
            )

        return run

    def list_runs(
        self,
        batch_date: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Возвращает список pipeline runs с фильтрацией.

        Args:
            batch_date: Фильтр по дате партии
            status: Фильтр по статусу этапа download
            limit: Максимум записей

        Returns:
            Список pipeline runs
        """
        query = {}
        if batch_date:
            query["batch_date"] = batch_date
        if status:
            query[f"stages.{PipelineStage.DOWNLOAD.value}.status"] = status

        cursor = self.collection.find(query).sort("created_at", -1).limit(limit)

        return list(cursor)

    def get_latest_run(self, batch_date: str) -> Optional[PipelineRun]:
        """
        Возвращает последний pipeline run для указанной даты.

        Args:
            batch_date: Дата партии

        Returns:
            PipelineRun или None
        """
        doc = self.collection.find_one(
            {"batch_date": batch_date}
        ).sort("created_at", -1)

        if not doc:
            return None

        return self._dict_to_run(doc)

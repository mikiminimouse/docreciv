"""
Расширенная система метрик для Docprep.

Позволяет собирать детальную статистику по всем этапам обработки,
используя отдельные тестовые коллекции в MongoDB для интеграционных тестов.

Использование:
    from docprep.core.metrics import MetricsCollector, ERROR_CATEGORIES

    # Создаём сборщик метрик
    metrics = MetricsCollector(test_run_id="test_2025_12_02")

    # Регистрируем начало теста
    metrics.start_test_run(
        input_dir=Path("/path/to/input"),
        dataset_name="2025-12-02",
        total_units=2250,
        total_files=2366,
    )

    # Записываем операции
    metrics.record_operation(
        operation_type="classify",
        cycle=1,
        stage="classifier",
        unit_id="UNIT_001",
        status="success",
        duration_ms=150,
    )

    # Записываем ошибки с категоризацией
    metrics.record_error(
        unit_id="UNIT_002",
        operation_type="extract",
        error_category="missing_dependency",
        error_message="unrar not found",
    )

    # Завершаем тест
    metrics.end_test_run(
        units_success=2200,
        units_failed=50,
        units_total=2250,
        final_stats={"ready_units": 2200},
    )
"""
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .database import get_database

logger = logging.getLogger(__name__)


# Категории ошибок для классификации
ERROR_CATEGORIES: Dict[str, str] = {
    "missing_dependency": "Отсутствует системная зависимость (LibreOffice, Xvfb, unrar, etc.)",
    "corrupted_file": "Повреждённый файл (невалидный архив, документ)",
    "timeout": "Таймаут операции",
    "permission_denied": "Нет прав доступа",
    "disk_space": "Недостаточно места на диске",
    "unsupported_format": "Неподдерживаемый формат файла",
    "empty_unit": "Пустой UNIT (нет файлов)",
    "ambiguous_type": "Не удалось определить тип файла",
    "conversion_failed": "Ошибка конвертации документа",
    "extraction_failed": "Ошибка распаковки архива",
    "validation_failed": "Ошибка валидации файла",
    "pipeline_error": "Критическая ошибка pipeline",
}


class MetricsCollector:
    """
    Сборщик метрик для интеграционных тестов pipeline.

    Создаёт отдельные тестовые коллекции в MongoDB для детальной
    статистики по всем операциям, ошибкам и файлам.

    Коллекции:
    - test_results: итоговые результаты теста
    - test_operation_stats: статистика по операциям
    - test_errors: ошибки с категоризацией
    - test_file_stats: статистика по файлам
    - test_unit_states: состояния UNIT

    Graceful degradation: при отсутствии MongoDB работает в offline режиме,
    логируя операции без записи в базу.
    """

    def __init__(
        self,
        test_run_id: Optional[str] = None,
        db_name: Optional[str] = None,
    ):
        """
        Инициализирует сборщик метрик.

        Args:
            test_run_id: Уникальный ID тестового запуска (автогенерация если None)
            db_name: Имя базы данных MongoDB (по умолчанию из MONGODB_URI)
        """
        self.test_run_id = test_run_id or self._generate_id()
        self.start_time = None
        self.db_name = db_name
        self.db = get_database()

        # Инициализируем атрибуты для offline режима
        self.client = None
        self.test_db = None
        self.test_results = None
        self.test_operation_stats = None
        self.test_errors = None
        self.test_file_stats = None
        self.test_unit_states = None

        # Инициализируем тестовые коллекции
        self._init_test_collections()

    def _generate_id(self) -> str:
        """Генерирует уникальный ID тестового запуска."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique = uuid.uuid4().hex[:8]
        return f"test_{timestamp}_{unique}"

    def _init_test_collections(self) -> None:
        """Создаёт тестовые коллекции и индексы в MongoDB."""
        if not self.db.is_connected():
            logger.debug("MongoDB not connected - working in offline mode")
            return

        try:
            # Определяем имя базы данных
            if self.db_name:
                # Используем указанную базу данных
                from pymongo import MongoClient
                connection_string = os.getenv(
                    "MONGODB_URI",
                    "mongodb://admin:password@localhost:27018/docling_metadata?authSource=admin"
                )
                # Извлекаем хост из connection string
                import re
                match = re.search(r'mongodb://[^@]+@([^/]+)', connection_string)
                if match:
                    host = match.group(1)
                else:
                    host = "localhost:27018"

                self.client = MongoClient(f"mongodb://{host}")
                self.test_db = self.client[self.db_name]
            else:
                # Используем текущую базу данных
                self.client = self.db.client
                self.test_db = self.db.db

            # Создаём тестовые коллекции
            self.test_results = self.test_db.test_results
            self.test_operation_stats = self.test_db.test_operation_stats
            self.test_errors = self.test_db.test_errors
            self.test_file_stats = self.test_db.test_file_stats
            self.test_unit_states = self.test_db.test_unit_states

            # Создаём индексы
            self._create_indexes()

            logger.debug(f"Test collections initialized in {self.db_name or self.db.db_name}")

        except Exception as e:
            logger.warning(f"Failed to initialize test collections: {e}")
            self.client = None
            self.test_db = None

    def _create_indexes(self) -> None:
        """Создаёт индексы для тестовых коллекций."""
        if not self.test_db:
            return

        try:
            from pymongo import ASCENDING, DESCENDING

            # test_results
            self.test_results.create_index([("test_run_id", ASCENDING)], unique=True)
            self.test_results.create_index([("start_time", DESCENDING)])
            self.test_results.create_index([("dataset_name", ASCENDING)])
            self.test_results.create_index([("status", ASCENDING)])

            # test_operation_stats
            self.test_operation_stats.create_index([("test_run_id", ASCENDING), ("operation_type", ASCENDING)])
            self.test_operation_stats.create_index([("cycle", ASCENDING), ("stage", ASCENDING)])
            self.test_operation_stats.create_index([("unit_id", ASCENDING)])
            self.test_operation_stats.create_index([("status", ASCENDING)])
            self.test_operation_stats.create_index([("timestamp", DESCENDING)])

            # test_errors
            self.test_errors.create_index([("test_run_id", ASCENDING)])
            self.test_errors.create_index([("unit_id", ASCENDING)])
            self.test_errors.create_index([("operation_type", ASCENDING)])
            self.test_errors.create_index([("error_category", ASCENDING)])
            self.test_errors.create_index([("timestamp", DESCENDING)])

            # test_file_stats
            self.test_file_stats.create_index([("test_run_id", ASCENDING)])
            self.test_file_stats.create_index([("extension", ASCENDING)])
            self.test_file_stats.create_index([("detected_type", ASCENDING)])

            # test_unit_states
            self.test_unit_states.create_index([("test_run_id", ASCENDING), ("unit_id", ASCENDING)], unique=True)
            self.test_unit_states.create_index([("current_state", ASCENDING)])
            self.test_unit_states.create_index([("processing_cycle", ASCENDING)])

            logger.debug("Test collection indexes created")

        except Exception as e:
            logger.warning(f"Failed to create test collection indexes: {e}")

    # ========================================================================
    # Управление тестовым запуском
    # ========================================================================

    def start_test_run(
        self,
        input_dir: Path,
        dataset_name: str,
        total_units: int,
        total_files: int,
        config: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Регистрирует начало тестового запуска.

        Args:
            input_dir: Входная директория
            dataset_name: Имя датасета (например, "2025-12-02")
            total_units: Общее количество UNIT
            total_files: Общее количество файлов
            config: Дополнительная конфигурация теста

        Returns:
            test_run_id: ID тестового запуска
        """
        self.start_time = datetime.now(timezone.utc)

        test_run_doc = {
            "_id": self.test_run_id,
            "test_run_id": self.test_run_id,
            "dataset_name": dataset_name,
            "start_time": self.start_time,
            "input_dir": str(input_dir),
            "initial_units": total_units,
            "initial_files": total_files,
            "config": config or {},
            "status": "running",
        }

        if self._is_connected():
            try:
                self.test_results.insert_one(test_run_doc)
                logger.info(f"Test run started: {self.test_run_id}")
            except Exception as e:
                logger.warning(f"Failed to start test run in MongoDB: {e}")
        else:
            logger.info(f"Test run started (offline): {self.test_run_id}")

        return self.test_run_id

    def end_test_run(
        self,
        units_success: int,
        units_failed: int,
        units_total: int,
        final_stats: Dict[str, Any],
    ) -> None:
        """
        Завершает тестовый запуск и записывает итоговые результаты.

        Args:
            units_success: Количество успешно обработанных UNIT
            units_failed: Количество UNIT с ошибками
            units_total: Общее количество UNIT
            final_stats: Финальная статистика (словарь с произвольными данными)
        """
        end_time = datetime.now(timezone.utc)
        duration_seconds = (end_time - self.start_time).total_seconds() if self.start_time else 0

        doc = {
            "end_time": end_time,
            "duration_seconds": duration_seconds,
            "status": "completed",
            "units_success": units_success,
            "units_failed": units_failed,
            "units_total": units_total,
            "success_rate": units_success / units_total if units_total > 0 else 0.0,
            "final_stats": final_stats,
        }

        if self._is_connected():
            try:
                self.test_results.update_one(
                    {"_id": self.test_run_id},
                    {"$set": doc}
                )
                logger.info(f"Test run completed: {self.test_run_id} ({duration_seconds:.1f}s)")
            except Exception as e:
                logger.warning(f"Failed to end test run in MongoDB: {e}")
        else:
            logger.info(f"Test run completed (offline): {self.test_run_id} ({duration_seconds:.1f}s)")

    # ========================================================================
    # Запись операций
    # ========================================================================

    def record_operation(
        self,
        operation_type: str,
        cycle: int,
        stage: str,
        unit_id: str,
        status: str,
        duration_ms: int = 0,
        details: Optional[Dict[str, Any]] = None,
        error: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Записывает метрику операции.

        Args:
            operation_type: Тип операции (classify, extract, convert, normalize, merge)
            cycle: Номер цикла (1, 2, 3)
            stage: Название этапа
            unit_id: ID UNIT
            status: Статус (success, failed, skipped)
            duration_ms: Длительность в миллисекундах
            details: Дополнительная информация об операции
            error: Детали ошибки (если status="failed")
        """
        if not self._is_connected():
            logger.debug(f"[offline] Operation: {operation_type} {unit_id} -> {status}")
            return

        doc = {
            "test_run_id": self.test_run_id,
            "operation_type": operation_type,
            "cycle": cycle,
            "stage": stage,
            "unit_id": unit_id,
            "status": status,
            "duration_ms": duration_ms,
            "timestamp": datetime.now(timezone.utc),
            "details": details or {},
        }

        if error:
            doc["error"] = error

        try:
            self.test_operation_stats.insert_one(doc)
        except Exception as e:
            logger.warning(f"Failed to record operation: {e}")

    def record_unit_state(
        self,
        unit_id: str,
        current_state: str,
        processing_cycle: int,
        route: Optional[str] = None,
        file_count: int = 0,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Записывает состояние UNIT.

        Args:
            unit_id: ID UNIT
            current_state: Текущее состояние (RAW, CLASSIFIED_N, etc.)
            processing_cycle: Номер цикла обработки
            route: Маршрут обработки
            file_count: Количество файлов
            details: Дополнительная информация
        """
        if not self._is_connected():
            return

        doc = {
            "test_run_id": self.test_run_id,
            "unit_id": unit_id,
            "current_state": current_state,
            "processing_cycle": processing_cycle,
            "route": route,
            "file_count": file_count,
            "details": details or {},
            "updated_at": datetime.now(timezone.utc),
        }

        try:
            self.test_unit_states.update_one(
                {"test_run_id": self.test_run_id, "unit_id": unit_id},
                {"$set": doc, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
                upsert=True,
            )
        except Exception as e:
            logger.warning(f"Failed to record unit state: {e}")

    def record_file_stats(
        self,
        unit_id: str,
        files: List[Dict[str, Any]],
    ) -> None:
        """
        Записывает статистику по файлам UNIT.

        Args:
            unit_id: ID UNIT
            files: Список файлов (из manifest.files)
        """
        if not self._is_connected():
            return

        documents = []
        for file_info in files:
            doc = {
                "test_run_id": self.test_run_id,
                "unit_id": unit_id,
                "original_name": file_info.get("original_name"),
                "current_name": file_info.get("current_name"),
                "extension": file_info.get("extension"),
                "detected_type": file_info.get("detected_type"),
                "mime_type": file_info.get("mime_type"),
                "size_bytes": file_info.get("size_bytes"),
                "needs_ocr": file_info.get("needs_ocr", False),
                "timestamp": datetime.now(timezone.utc),
            }
            documents.append(doc)

        try:
            if documents:
                self.test_file_stats.insert_many(documents, ordered=False)
        except Exception as e:
            logger.warning(f"Failed to record file stats: {e}")

    # ========================================================================
    # Запись ошибок
    # ========================================================================

    def record_error(
        self,
        unit_id: str,
        operation_type: str,
        error_category: str,
        error_message: str,
        details: Optional[Dict[str, Any]] = None,
        cycle: Optional[int] = None,
    ) -> None:
        """
        Записывает ошибку с категоризацией.

        Args:
            unit_id: ID UNIT (или "pipeline" для общих ошибок)
            operation_type: Тип операции, в которой произошла ошибка
            error_category: Категория ошибки (из ERROR_CATEGORIES)
            error_message: Текст ошибки
            details: Дополнительная информация
            cycle: Номер цикла (если применимо)
        """
        if not self._is_connected():
            logger.debug(f"[offline] Error: {error_category} in {operation_type}: {error_message}")
            return

        doc = {
            "test_run_id": self.test_run_id,
            "unit_id": unit_id,
            "operation_type": operation_type,
            "error_category": error_category,
            "error_message": error_message,
            "details": details or {},
            "timestamp": datetime.now(timezone.utc),
        }

        if cycle is not None:
            doc["cycle"] = cycle

        try:
            self.test_errors.insert_one(doc)
        except Exception as e:
            logger.warning(f"Failed to record error: {e}")

    def categorize_error(
        self,
        error_message: str,
        operation_type: str,
    ) -> str:
        """
        Определяет категорию ошибки по сообщению.

        Args:
            error_message: Текст ошибки
            operation_type: Тип операции

        Returns:
            Категория ошибки из ERROR_CATEGORIES
        """
        error_lower = error_message.lower()

        # Проверка по ключевым словам
        if "not found" in error_lower or "no such file" in error_lower:
            return "missing_dependency"
        if "permission" in error_lower or "denied" in error_lower:
            return "permission_denied"
        if "timeout" in error_lower or "timed out" in error_lower:
            return "timeout"
        if "corrupted" in error_lower or "invalid" in error_lower or "bad" in error_lower:
            return "corrupted_file"
        if "disk" in error_lower or "space" in error_lower:
            return "disk_space"
        if "unsupported" in error_lower or "unknown format" in error_lower:
            return "unsupported_format"
        if "empty" in error_lower:
            return "empty_unit"

        # Специфические проверки по типу операции
        if operation_type == "convert":
            if "libreoffice" in error_lower or "soffice" in error_lower:
                return "conversion_failed"
        elif operation_type == "extract":
            if "archive" in error_lower or "rar" in error_lower or "zip" in error_lower:
                return "extraction_failed"

        return "validation_failed"

    # ========================================================================
    # Агрегация и отчёты
    # ========================================================================

    def get_summary(self) -> Dict[str, Any]:
        """
        Возвращает сводную информацию о тестовом запуске.

        Returns:
            Словарь со сводной информацией
        """
        if not self._is_connected():
            return {
                "test_run_id": self.test_run_id,
                "status": "offline_mode",
            }

        try:
            # Получаем результаты теста
            result = self.test_results.find_one({"_id": self.test_run_id})

            # Агрегируем статистику операций
            operation_stats = {}
            for op_type in ["classify", "extract", "convert", "normalize", "merge"]:
                success_count = self.test_operation_stats.count_documents({
                    "test_run_id": self.test_run_id,
                    "operation_type": op_type,
                    "status": "success",
                })
                failed_count = self.test_operation_stats.count_documents({
                    "test_run_id": self.test_run_id,
                    "operation_type": op_type,
                    "status": "failed",
                })
                operation_stats[op_type] = {
                    "success": success_count,
                    "failed": failed_count,
                    "total": success_count + failed_count,
                }

            # Агрегируем ошибки по категориям
            error_pipeline = [
                {"$match": {"test_run_id": self.test_run_id}},
                {"$group": {
                    "_id": "$error_category",
                    "count": {"$sum": 1},
                }},
                {"$sort": {"count": -1}},
            ]

            errors_by_category = {}
            for doc in self.test_errors.aggregate(error_pipeline):
                errors_by_category[doc["_id"]] = doc["count"]

            return {
                "test_run_id": self.test_run_id,
                "result": result,
                "operation_stats": operation_stats,
                "errors_by_category": errors_by_category,
            }

        except Exception as e:
            logger.warning(f"Failed to get summary: {e}")
            return {
                "test_run_id": self.test_run_id,
                "error": str(e),
            }

    # ========================================================================
    # Вспомогательные методы
    # ========================================================================

    def _is_connected(self) -> bool:
        """Проверяет, подключены ли тестовые коллекции."""
        return self.test_db is not None and self.db.is_connected()

    def is_connected(self) -> bool:
        """Публичный метод проверки подключения."""
        return self._is_connected()


class TestDatabaseSetup:
    """
    Настройка тестовой MongoDB базы для интеграционных тестов.

    Создаёт отдельную базу данных с необходимыми коллекциями и индексами
    для изоляции тестовых данных от production.

    Использование:
        setup = TestDatabaseSetup()
        db_name = setup.create_test_database()

        # Используем тестовую базу
        metrics = MetricsCollector(db_name=db_name)
    """

    def __init__(
        self,
        connection_string: str = "mongodb://localhost:27017",
        prefix: str = "docprep_test",
    ):
        """
        Инициализирует настройку тестовой базы.

        Args:
            connection_string: Строка подключения MongoDB
            prefix: Префикс для имени тестовой базы
        """
        try:
            from pymongo import MongoClient
            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
            # Проверяем подключение
            self.client.admin.command('ping')
            self.prefix = prefix
            self.db_name = None
            self._connected = True
        except Exception as e:
            logger.warning(f"MongoDB connection failed: {e}")
            self.client = None
            self.prefix = prefix
            self.db_name = None
            self._connected = False

    def create_test_database(self, dataset_name: str = None) -> Optional[str]:
        """
        Создаёт тестовую базу с необходимыми коллекциями и индексами.

        Args:
            dataset_name: Опциональное имя датасета для идентификации

        Returns:
            Имя созданной базы данных или None при ошибке
        """
        if not self._connected:
            logger.warning("MongoDB not connected - cannot create test database")
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = f"_{dataset_name}" if dataset_name else ""
        self.db_name = f"{self.prefix}_{timestamp}{suffix}"

        try:
            db = self.client[self.db_name]

            # Коллекции с индексами
            collections = {
                "test_results": [
                    [("test_run_id", 1), ("unique", 1)],
                    [("start_time", -1)],
                    [("status", 1)],
                ],
                "test_operation_stats": [
                    [("test_run_id", 1), ("operation_type", 1)],
                    [("cycle", 1), ("stage", 1)],
                    [("unit_id", 1)],
                    [("status", 1)],
                ],
                "test_errors": [
                    [("test_run_id", 1)],
                    [("unit_id", 1)],
                    [("operation_type", 1)],
                    [("error_category", 1)],
                ],
                "test_file_stats": [
                    [("test_run_id", 1)],
                    [("extension", 1)],
                    [("detected_type", 1)],
                ],
                "test_unit_states": [
                    [("test_run_id", 1), ("unit_id", 1), ("unique", 1)],
                    [("current_state", 1)],
                    [("processing_cycle", 1)],
                ],
            }

            from pymongo import ASCENDING, DESCENDING

            for coll_name, indexes in collections.items():
                coll = db[coll_name]
                for index_def in indexes:
                    # Преобразуем порядок сортировки
                    index = []
                    for field, order in index_def:
                        if order == "unique":
                            continue  # unique - это не порядок, а опция индекса
                        order_type = ASCENDING if order == 1 else DESCENDING
                        index.append((field, order_type))
                    if index:
                        coll.create_index(index)

            logger.info(f"Test database created: {self.db_name}")
            return self.db_name

        except Exception as e:
            logger.error(f"Failed to create test database: {e}")
            return None

    def get_connection_string(self) -> Optional[str]:
        """
        Возвращает connection string для использования в тестах.

        Returns:
            Connection string или None
        """
        if not self._connected:
            return None

        # Извлекаем хост из текущего подключения
        try:
            host = self.client.HOST
            port = self.client.PORT
            return f"mongodb://{host}:{port}/{self.db_name}"
        except Exception:
            return f"mongodb://localhost:27017/{self.db_name}"

    def cleanup(self, drop_database: bool = False) -> None:
        """
        Очищает ресурсы тестовой базы.

        Args:
            drop_database: Удалить тестовую базу данных
        """
        if drop_database and self.db_name and self._connected:
            try:
                self.client.drop_database(self.db_name)
                logger.info(f"Test database dropped: {self.db_name}")
            except Exception as e:
                logger.warning(f"Failed to drop test database: {e}")

        if self.client:
            self.client.close()

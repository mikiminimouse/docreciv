"""
Setup для MongoDB интеграционных тестов.

Предоставляет утилиты для создания и управления тестовыми базами данных
MongoDB для изоляции тестовых данных от production.

Использование:
    from docprep.tests.integration.mongodb_test_setup import create_test_db, cleanup_test_db

    # Создаём тестовую базу
    db_name = create_test_db("2025-12-02")

    # Используем в тестах
    os.environ["MONGODB_URI"] = f"mongodb://localhost:27017/{db_name}"

    # После теста удаляем
    cleanup_test_db(db_name)
"""
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def get_mongo_client():
    """
    Возвращает MongoClient для MongoDB.

    Returns:
        MongoClient или None при ошибке подключения
    """
    try:
        from pymongo import MongoClient

        # По умолчанию используем localhost:27017 для тестов
        connection_string = os.getenv(
            "MONGODB_URI",
            "mongodb://localhost:27017"
        )

        # Очищаем connection string от имени базы данных
        # mongodb://localhost:27017/db_name -> mongodb://localhost:27017
        import re
        cleaned = re.sub(r'/[^/]*$', '', connection_string)
        if not cleaned.endswith('/'):
            cleaned += '/'

        client = MongoClient(
            cleaned,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
        )

        # Проверяем подключение
        client.admin.command('ping')
        return client

    except ImportError:
        logger.warning("pymongo not installed. Install with: pip install pymongo")
        return None
    except Exception as e:
        logger.warning(f"MongoDB connection failed: {e}")
        return None


def create_test_db(
    dataset_name: str,
    prefix: str = "docprep_test",
    drop_existing: bool = False,
) -> Optional[str]:
    """
    Создаёт тестовую базу данных с коллекциями для метрик.

    Args:
        dataset_name: Имя датасета для идентификации (например, "2025-12-02")
        prefix: Префикс для имени базы данных
        drop_existing: Удалить базу если уже существует

    Returns:
        Имя созданной базы данных или None при ошибке
    """
    client = get_mongo_client()
    if not client:
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    db_name = f"{prefix}_{dataset_name}_{timestamp}"

    try:
        db = client[db_name]

        # Определяем коллекции и индексы
        collections: Dict[str, List[Tuple[str, int]]] = {
            # test_results - итоговые результаты теста
            "test_results": [
                ("test_run_id", 1),
                ("start_time", -1),
                ("status", 1),
                ("dataset_name", 1),
            ],
            # test_operation_stats - статистика по операциям
            "test_operation_stats": [
                ("test_run_id", 1),
                ("operation_type", 1),
                ("cycle", 1),
                ("stage", 1),
                ("unit_id", 1),
                ("status", 1),
                ("timestamp", -1),
            ],
            # test_errors - ошибки с категоризацией
            "test_errors": [
                ("test_run_id", 1),
                ("unit_id", 1),
                ("operation_type", 1),
                ("error_category", 1),
                ("timestamp", -1),
            ],
            # test_file_stats - статистика по файлам
            "test_file_stats": [
                ("test_run_id", 1),
                ("extension", 1),
                ("detected_type", 1),
                ("unit_id", 1),
            ],
            # test_unit_states - состояния UNIT
            "test_unit_states": [
                ("test_run_id", 1),
                ("unit_id", 1),
                ("current_state", 1),
                ("processing_cycle", 1),
            ],
        }

        from pymongo import ASCENDING, DESCENDING

        for coll_name, indexes in collections.items():
            coll = db[coll_name]

            # Создаём индексы
            for field, order in indexes:
                order_type = ASCENDING if order == 1 else DESCENDING
                try:
                    coll.create_index([(field, order_type)])
                except Exception as idx_err:
                    logger.debug(f"Index creation note for {coll_name}.{field}: {idx_err}")

            # Для уникальных индексов
            if coll_name == "test_results":
                try:
                    coll.create_index([("test_run_id", ASCENDING)], unique=True)
                except Exception:
                    pass
            elif coll_name == "test_unit_states":
                try:
                    coll.create_index([
                        ("test_run_id", ASCENDING),
                        ("unit_id", ASCENDING)
                    ], unique=True)
                except Exception:
                    pass

        logger.info(f"Test database created: {db_name}")
        return db_name

    except Exception as e:
        logger.error(f"Failed to create test database: {e}")
        return None
    finally:
        client.close()


def cleanup_test_db(db_name: str) -> bool:
    """
    Удаляет тестовую базу данных.

    Args:
        db_name: Имя базы данных для удаления

    Returns:
        True если успешно удалена
    """
    client = get_mongo_client()
    if not client:
        return False

    try:
        client.drop_database(db_name)
        logger.info(f"Test database dropped: {db_name}")
        return True
    except Exception as e:
        logger.warning(f"Failed to drop test database {db_name}: {e}")
        return False
    finally:
        client.close()


def list_test_dbs(prefix: str = "docprep_test") -> List[str]:
    """
    Возвращает список тестовых баз данных.

    Args:
        prefix: Префикс для фильтрации

    Returns:
        Список имён тестовых баз
    """
    client = get_mongo_client()
    if not client:
        return []

    try:
        db_names = client.list_database_names()
        test_dbs = [name for name in db_names if name.startswith(prefix)]
        return sorted(test_dbs, reverse=True)
    except Exception as e:
        logger.warning(f"Failed to list test databases: {e}")
        return []
    finally:
        client.close()


def get_db_stats(db_name: str) -> Optional[Dict]:
    """
    Возвращает статистику по тестовой базе данных.

    Args:
        db_name: Имя базы данных

    Returns:
        Словарь со статистикой или None
    """
    client = get_mongo_client()
    if not client:
        return None

    try:
        db = client[db_name]
        stats = {
            "db_name": db_name,
            "collections": {},
            "total_documents": 0,
        }

        for coll_name in db.list_collection_names():
            coll = db[coll_name]
            count = coll.estimated_document_count()
            stats["collections"][coll_name] = count
            stats["total_documents"] += count

        return stats

    except Exception as e:
        logger.warning(f"Failed to get stats for {db_name}: {e}")
        return None
    finally:
        client.close()


def cleanup_old_test_dbs(
    prefix: str = "docprep_test",
    keep_latest: int = 5,
    dry_run: bool = False,
) -> List[str]:
    """
    Удаляет старые тестовые базы данных, оставляя только последние.

    Args:
        prefix: Префикс для фильтрации
        keep_latest: Сколько последних баз оставить
        dry_run: Только показать, что будет удалено

    Returns:
        Список удалённых (или которые были бы удалены) баз
    """
    test_dbs = list_test_dbs(prefix)

    if len(test_dbs) <= keep_latest:
        logger.info(f"Only {len(test_dbs)} test databases found (keeping {keep_latest})")
        return []

    to_delete = test_dbs[keep_latest:]

    if dry_run:
        logger.info(f"[DRY RUN] Would delete {len(to_delete)} test databases:")
        for db_name in to_delete:
            logger.info(f"  - {db_name}")
        return to_delete

    deleted = []
    for db_name in to_delete:
        if cleanup_test_db(db_name):
            deleted.append(db_name)

    logger.info(f"Deleted {len(deleted)} old test databases")
    return deleted


class TestDatabaseContext:
    """
    Context manager для тестовой базы данных.

    Автоматически создаёт базу при входе и удаляет при выходе.

    Использование:
        with TestDatabaseContext("2025-12-02") as db_name:
            os.environ["MONGODB_URI"] = f"mongodb://localhost:27017/{db_name}"
            # Запускаем тесты...
        # База автоматически удаляется после выхода
    """

    def __init__(
        self,
        dataset_name: str,
        prefix: str = "docprep_test",
        drop_on_exit: bool = True,
    ):
        """
        Инициализирует context manager.

        Args:
            dataset_name: Имя датасета
            prefix: Префикс для имени базы
            drop_on_exit: Удалить базу при выходе
        """
        self.dataset_name = dataset_name
        self.prefix = prefix
        self.drop_on_exit = drop_on_exit
        self.db_name = None

    def __enter__(self) -> Optional[str]:
        """Создаёт тестовую базу данных."""
        self.db_name = create_test_db(self.dataset_name, self.prefix)
        return self.db_name

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Удаляет тестовую базу данных."""
        if self.drop_on_exit and self.db_name:
            cleanup_test_db(self.db_name)


def seed_test_data(db_name: str, test_run_id: str, num_units: int = 10) -> bool:
    """
    Создаёт тестовые данные в базе для разработки.

    Args:
        db_name: Имя базы данных
        test_run_id: ID тестового запуска
        num_units: Количество UNIT для создания

    Returns:
        True если данные созданы успешно
    """
    client = get_mongo_client()
    if not client:
        return False

    try:
        db = client[db_name]

        from datetime import datetime, timezone
        import random

        # Создаём test_results
        db.test_results.insert_one({
            "_id": test_run_id,
            "test_run_id": test_run_id,
            "dataset_name": "test_dataset",
            "start_time": datetime.now(timezone.utc),
            "initial_units": num_units,
            "initial_files": num_units * 2,
            "status": "running",
        })

        # Создаём test_operation_stats
        operation_types = ["classify", "extract", "convert", "normalize", "merge"]
        statuses = ["success", "failed", "skipped"]

        for i in range(num_units * 5):
            db.test_operation_stats.insert_one({
                "test_run_id": test_run_id,
                "operation_type": random.choice(operation_types),
                "cycle": random.randint(1, 3),
                "stage": random.choice(operation_types),
                "unit_id": f"UNIT_{i:04d}",
                "status": random.choice(statuses),
                "duration_ms": random.randint(10, 5000),
                "timestamp": datetime.now(timezone.utc),
                "details": {},
            })

        # Создаём test_errors
        if num_units > 5:
            for i in random.sample(range(num_units), min(3, num_units)):
                db.test_errors.insert_one({
                    "test_run_id": test_run_id,
                    "unit_id": f"UNIT_{i:04d}",
                    "operation_type": random.choice(operation_types),
                    "error_category": random.choice([
                        "missing_dependency",
                        "corrupted_file",
                        "timeout",
                    ]),
                    "error_message": f"Test error for UNIT_{i:04d}",
                    "details": {},
                    "timestamp": datetime.now(timezone.utc),
                })

        logger.info(f"Test data seeded in {db_name}")
        return True

    except Exception as e:
        logger.error(f"Failed to seed test data: {e}")
        return False
    finally:
        client.close()

"""
Упрощенный микросервис синхронизации протоколов.

Выполняет 5 этапов синхронизации:
1. Проверка подключения к локальной MongoDB
2. Проверка подключения к удаленной MongoDB
3. Проверка обновлений в коллекции протоколов
4. Синхронизация коллекций баз данных/обновления локальной MongoDB
5. Отчет с метриками
"""

import os
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError


def load_env_file():
    """Загружает переменные окружения из .env файла если он существует."""
    # Ищем .env файл в корневой директории проекта
    env_file = Path(__file__).parent.parent.parent.parent / ".env"  # Теперь на уровень выше
    if env_file.exists():
        try:
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        # Удаляем кавычки если они есть
                        value = value.strip('"').strip("'")
                        os.environ[key] = value
        except Exception as e:
            print(f"⚠️  Не удалось загрузить .env файл: {e}")


# Загружаем переменные окружения при импорте модуля
load_env_file()


@dataclass
class SyncResult:
    """Результат синхронизации."""
    status: str
    message: str
    date: str
    scanned: int = 0
    inserted: int = 0
    skipped_existing: int = 0
    errors_count: int = 0
    duration: float = 0.0
    errors: Optional[List[str]] = None

    @property
    def success(self) -> bool:
        """True если синхронизация успешна или частично успешна."""
        return self.status in ["success", "partial"]

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


@dataclass
class SyncConfig:
    """Конфигурация синхронизации."""
    # MongoDB settings for remote connection (through VPN)
    mongo_server: str = os.getenv("MONGO_SERVER", "192.168.0.46:8635")
    mongo_user: str = os.getenv("MONGO_USER", "readProtocols223")
    mongo_password: str = os.getenv("MONGO_PASSWORD", "")
    mongo_ssl_cert: str = os.getenv("MONGO_SSL_CERT", "/root/winners_preprocessor/receiver/certs/sber2.crt")
    
    # MongoDB settings for local connection
    local_mongo_server: str = os.getenv("LOCAL_MONGO_SERVER", "localhost:27017")
    local_mongo_user: str = os.getenv("MONGO_METADATA_USER", "docling_user")
    local_mongo_password: str = os.getenv("MONGO_METADATA_PASSWORD", "")
    local_mongo_db: str = os.getenv("MONGO_METADATA_DB", "docling_metadata")
    
    # Synchronization parameters
    batch_size: int = 1000
    max_workers: int = 4
    
    # Collection names
    remote_collection: str = "protocols223.purchaseProtocol"
    local_collection: str = "docling_metadata.protocols"


class SyncService:
    """Сервис синхронизации протоколов."""

    def __init__(self, config: Optional[SyncConfig] = None):
        self.config = config or SyncConfig()
        self.remote_client = None
        self.local_client = None

    def _get_remote_mongo_client(self) -> Optional[MongoClient]:
        """Получает клиент MongoDB для удаленной базы данных."""
        try:
            # Проверяем обязательные параметры
            required_params = [
                ("MONGO_SERVER", self.config.mongo_server),
                ("MONGO_USER", self.config.mongo_user),
                ("MONGO_PASSWORD", self.config.mongo_password),
                ("MONGO_SSL_CERT", self.config.mongo_ssl_cert)
            ]
            
            missing_params = [param[0] for param in required_params if not param[1]]
            if missing_params:
                print(f"❌ Не все параметры удалённой Mongo заданы: {missing_params}")
                return None

            # Создаем URL подключения
            url = f"mongodb://{self.config.mongo_user}:{self.config.mongo_password}@{self.config.mongo_server}/?authSource=protocols223"
            
            # Создаем клиент с SSL настройками
            client = MongoClient(
                url,
                tls=True,
                tlsCAFile=self.config.mongo_ssl_cert,
                tlsAllowInvalidHostnames=True,
                serverSelectionTimeoutMS=20000
            )
            
            # Проверяем подключение
            client.admin.command("ping")
            print("✅ Подключение к удалённой MongoDB успешно")
            return client
            
        except ConnectionFailure as e:
            print(f"❌ Ошибка подключения к удалённой MongoDB: {e}")
            return None
        except Exception as e:
            print(f"❌ Неожиданная ошибка подключения к удалённой MongoDB: {e}")
            return None

    def _get_local_mongo_client(self) -> Optional[MongoClient]:
        """Получает клиент MongoDB для локальной базы данных."""
        try:
            # Проверяем обязательные параметры
            required_params = [
                ("LOCAL_MONGO_SERVER", self.config.local_mongo_server),
                ("MONGO_METADATA_USER", self.config.local_mongo_user),
                ("MONGO_METADATA_PASSWORD", self.config.local_mongo_password)
            ]
            
            missing_params = [param[0] for param in required_params if not param[1]]
            if missing_params:
                print(f"❌ Не все параметры локальной Mongo заданы: {missing_params}")
                return None

            # Создаем URL подключения
            url = f"mongodb://{self.config.local_mongo_user}:{self.config.local_mongo_password}@{self.config.local_mongo_server}/?authSource=admin"
            
            # Создаем клиент
            client = MongoClient(
                url,
                serverSelectionTimeoutMS=10000
            )
            
            # Проверяем подключение
            client.admin.command("ping")
            print("✅ Подключение к локальной MongoDB успешно")
            return client
            
        except ConnectionFailure as e:
            print(f"❌ Ошибка подключения к локальной MongoDB: {e}")
            return None
        except Exception as e:
            print(f"❌ Неожиданная ошибка подключения к локальной MongoDB: {e}")
            return None

    def _ensure_indexes(self, db) -> None:
        """Создает необходимые индексы в локальной базе данных."""
        try:
            collection = db[self.config.local_collection.split('.')[-1]]
            collection.create_index([("purchaseNoticeNumber", 1), ("source", 1)], name="pn_source_idx")
            collection.create_index([("loadDate", 1)], name="loadDate_idx")
            collection.create_index([("unit_id", 1)], name="unit_idx")
            collection.create_index([("status", 1)], name="status_idx")
            print("✅ Индексы в локальной MongoDB созданы")
        except Exception as e:
            print(f"⚠️  Ошибка создания индексов: {e}")

    def _generate_unit_id(self) -> str:
        """Генерирует уникальный ID для unit в формате UNIT_<16hex>."""
        import uuid
        return f"UNIT_{uuid.uuid4().hex[:16]}"

    def _extract_urls_from_attachments(self, raw_doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Извлекает URL из поля attachments с поддержкой разных структур."""
        urls = []

        def add_from_doc(doc: Dict[str, Any]) -> None:
            url = doc.get("url") or doc.get("downloadUrl") or doc.get("fileUrl")
            if url:
                urls.append({
                    "url": url,
                    "fileName": doc.get("fileName") or doc.get("name") or "",
                    "guid": doc.get("guid"),
                    "contentUid": doc.get("contentUid"),
                    "description": doc.get("description"),
                })

        # Обрабатываем разные структуры attachments
        attachments = raw_doc.get("attachments")

        if isinstance(attachments, dict):
            docs_field = attachments.get("document", [])
            if isinstance(docs_field, dict):
                docs_field = [docs_field]
            if isinstance(docs_field, list):
                for item in docs_field:
                    if isinstance(item, dict):
                        add_from_doc(item)
        elif isinstance(attachments, list):
            for item in attachments:
                if isinstance(item, dict):
                    add_from_doc(item)

        return urls

    def _create_protocol_document(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Создает документ протокола для вставки в локальную MongoDB."""
        # Извлекаем purchase notice number
        purchase_info = raw_doc.get("purchaseInfo", {})
        pn = purchase_info.get("purchaseNoticeNumber") if isinstance(purchase_info, dict) else None
        
        # Извлекаем URLs из attachments
        urls = self._extract_urls_from_attachments(raw_doc)
        
        # Получаем дату загрузки
        load_date = raw_doc.get("loadDate")
        now_ts = datetime.utcnow()
        
        # Создаем документ с сервисными полями
        doc_to_insert = {
            # FULL PROTOCOL DATA FROM MONGODB (без _id - MongoDB сгенерирует новый)
            **{k: v for k, v in raw_doc.items() if k != '_id'},  # Включаем ВСЕ поля кроме _id

            # Service fields for preprocessing
            "unit_id": self._generate_unit_id(),
            "urls": urls,
            "multi_url": len(urls) > 1,
            "url_count": len(urls),
            "source": "remote_mongo_direct",
            "status": "pending",
            "created_at": now_ts,
            "updated_at": now_ts,
        }
        
        return doc_to_insert

    def sync_protocols_for_date(
        self,
        target_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> SyncResult:
        """
        Выполняет полную синхронизацию протоколов за указанную дату.
        """
        start_time = time.time()
        
        if target_date is None:
            target_date = datetime.utcnow() - timedelta(days=1)
            
        if limit is None:
            limit = 200

        print("🚀 ЗАПУСК СИНХРОНИЗАЦИИ ПРОТОКОЛОВ")
        print(f"   Дата: {target_date.date()}")
        print(f"   Лимит: {limit}")

        # Этап 1: Проверка подключения к локальной MongoDB
        print("\n1️⃣  Проверка подключения к локальной MongoDB...")
        self.local_client = self._get_local_mongo_client()
        if not self.local_client:
            return SyncResult(
                status="error",
                message="Не удалось подключиться к локальной MongoDB",
                date=target_date.date().isoformat()
            )

        # Этап 2: Проверка подключения к удаленной MongoDB
        print("\n2️⃣  Проверка подключения к удаленной MongoDB...")
        self.remote_client = self._get_remote_mongo_client()
        if not self.remote_client:
            self.local_client.close()
            return SyncResult(
                status="error",
                message="Не удалось подключиться к удаленной MongoDB",
                date=target_date.date().isoformat()
            )

        try:
            # Получаем коллекции
            remote_parts = self.config.remote_collection.split('.')
            remote_db = self.remote_client[remote_parts[0]]
            remote_collection = remote_db[remote_parts[1]]
            
            local_parts = self.config.local_collection.split('.')
            local_db = self.local_client[local_parts[0]]
            local_collection = local_db[local_parts[1]]
            
            # Создаем индексы
            self._ensure_indexes(local_db)
            
            # Этап 3: Проверка обновлений в коллекции протоколов
            print("\n3️⃣  Проверка обновлений в коллекции протоколов...")
            start_dt = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)
            end_dt = datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59)
            
            query = {
                "loadDate": {
                    "$gte": start_dt,
                    "$lte": end_dt,
                }
            }
            
            # Получаем количество документов (приблизительно)
            try:
                total_count = remote_collection.count_documents(query)
                print(f"   Найдено документов: {total_count}")
            except:
                total_count = 0
                print("   Не удалось получить точное количество документов")
            
            # Этап 4: Синхронизация коллекций баз данных
            print("\n4️⃣  Синхронизация коллекций баз данных...")
            cursor = remote_collection.find(query, no_cursor_timeout=True, batch_size=self.config.batch_size)
            
            # Применяем лимит если задан
            if limit > 0:
                cursor = cursor.limit(limit)
            
            scanned = 0
            inserted = 0
            skipped_existing = 0
            errors_count = 0
            errors = []
            
            batch = []
            
            for raw_doc in cursor:
                batch.append(raw_doc)
                scanned += 1
                
                # Обрабатываем пакет
                if len(batch) >= self.config.batch_size:
                    print(f"   Обработка пакета из {len(batch)} документов...")
                    
                    for doc in batch:
                        try:
                            # Извлекаем purchase notice number
                            purchase_info = doc.get("purchaseInfo", {})
                            pn = purchase_info.get("purchaseNoticeNumber") if isinstance(purchase_info, dict) else None
                            
                            if not pn:
                                errors_count += 1
                                errors.append(f"Пропущен документ без purchaseNoticeNumber")
                                continue
                            
                            # Проверяем дубликаты
                            existing = local_collection.find_one({
                                "purchaseNoticeNumber": str(pn),
                                "source": "remote_mongo_direct"
                            })
                            
                            if existing:
                                skipped_existing += 1
                                continue
                            
                            # Создаем документ для вставки
                            doc_to_insert = self._create_protocol_document(doc)
                            
                            # Пытаемся вставить документ (игнорируем дубликаты)
                            try:
                                local_collection.insert_one(doc_to_insert)
                                inserted += 1
                            except Exception as e:
                                # Если дубликат - просто пропускаем, это нормально
                                if "duplicate key" in str(e):
                                    continue
                                else:
                                    # Другие ошибки - логируем
                                    raise e
                            
                        except Exception as e:
                            errors_count += 1
                            error_msg = f"Ошибка обработки документа: {e}"
                            errors.append(error_msg)
                            print(f"   ❌ {error_msg}")
                    
                    batch = []
            
            # Обрабатываем оставшиеся документы
            if batch:
                print(f"   Обработка последнего пакета из {len(batch)} документов...")
                
                for doc in batch:
                    try:
                        # Извлекаем purchase notice number
                        purchase_info = doc.get("purchaseInfo", {})
                        pn = purchase_info.get("purchaseNoticeNumber") if isinstance(purchase_info, dict) else None
                        
                        if not pn:
                            errors_count += 1
                            errors.append(f"Пропущен документ без purchaseNoticeNumber")
                            continue
                        
                        # Проверяем дубликаты
                        existing = local_collection.find_one({
                            "purchaseNoticeNumber": str(pn),
                            "source": "remote_mongo_direct"
                        })
                        
                        if existing:
                            skipped_existing += 1
                            continue
                        
                        # Создаем документ для вставки
                        doc_to_insert = self._create_protocol_document(doc)
                        
                        # Вставляем документ
                        local_collection.insert_one(doc_to_insert)
                        inserted += 1
                        
                    except Exception as e:
                        errors_count += 1
                        error_msg = f"Ошибка обработки документа: {e}"
                        errors.append(error_msg)
                        print(f"   ❌ {error_msg}")
            
            # Этап 5: Отчет с метриками
            duration = time.time() - start_time
            print("\n5️⃣  Отчет с метриками:")
            print(f"   ✅ Просмотрено: {scanned}")
            print(f"   💾 Вставлено: {inserted}")
            print(f"   ⏭️  Пропущено (дубликаты): {skipped_existing}")
            print(f"   ❌ Ошибок: {errors_count}")
            print(f"   ⏱️  Время выполнения: {duration:.2f} секунд")
            
            return SyncResult(
                status="success" if errors_count == 0 else "partial",
                message="Синхронизация завершена",
                date=target_date.date().isoformat(),
                scanned=scanned,
                inserted=inserted,
                skipped_existing=skipped_existing,
                errors_count=errors_count,
                duration=duration,
                errors=errors
            )
            
        except Exception as e:
            error_msg = f"Критическая ошибка синхронизации: {e}"
            print(f"\n❌ {error_msg}")
            return SyncResult(
                status="error",
                message=error_msg,
                date=target_date.date().isoformat(),
                errors=[error_msg]
            )
            
        finally:
            # Закрываем соединения
            if self.remote_client:
                self.remote_client.close()
            if self.local_client:
                self.local_client.close()

    def sync_full_collection(self, days: int = 14) -> SyncResult:
        """Полная синхронизация коллекции за указанное количество дней."""
        print(f"🔄 ПОЛНАЯ СИНХРОНИЗАЦИЯ ЗА {days} ДНЕЙ")
        
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)
        
        return self.sync_protocols_for_date_range(start_date, end_date)

    def sync_daily_updates(self, limit: Optional[int] = None) -> SyncResult:
        """Ежедневное обновление - синхронизация за вчерашний день."""
        print("🌅 ЕЖЕДНЕВНОЕ ОБНОВЛЕНИЕ")
        yesterday = datetime.utcnow() - timedelta(days=1)
        return self.sync_protocols_for_date(yesterday, limit)

    def sync_protocols_for_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: Optional[int] = None
    ) -> SyncResult:
        """
        Выполняет синхронизацию протоколов за диапазон дат.
        """
        start_time = time.time()
        
        if limit is None:
            limit = 0  # Без лимита

        print("🚀 ЗАПУСК СИНХРОНИЗАЦИИ ПРОТОКОЛОВ ЗА ПЕРИОД")
        print(f"   Период: {start_date.date()} - {end_date.date()}")
        if limit > 0:
            print(f"   Лимит: {limit}")

        # Этап 1: Проверка подключения к локальной MongoDB
        print("\n1️⃣  Проверка подключения к локальной MongoDB...")
        self.local_client = self._get_local_mongo_client()
        if not self.local_client:
            return SyncResult(
                status="error",
                message="Не удалось подключиться к локальной MongoDB",
                date=f"{start_date.date().isoformat()} - {end_date.date().isoformat()}"
            )

        # Этап 2: Проверка подключения к удаленной MongoDB
        print("\n2️⃣  Проверка подключения к удаленной MongoDB...")
        self.remote_client = self._get_remote_mongo_client()
        if not self.remote_client:
            self.local_client.close()
            return SyncResult(
                status="error",
                message="Не удалось подключиться к удаленной MongoDB",
                date=f"{start_date.date().isoformat()} - {end_date.date().isoformat()}"
            )

        try:
            # Получаем коллекции
            remote_parts = self.config.remote_collection.split('.')
            remote_db = self.remote_client[remote_parts[0]]
            remote_collection = remote_db[remote_parts[1]]
            
            local_parts = self.config.local_collection.split('.')
            local_db = self.local_client[local_parts[0]]
            local_collection = local_db[local_parts[1]]
            
            # Создаем индексы
            self._ensure_indexes(local_db)
            
            # Этап 3: Проверка обновлений в коллекции протоколов
            print("\n3️⃣  Проверка обновлений в коллекции протоколов...")
            query = {
                "loadDate": {
                    "$gte": start_date,
                    "$lte": end_date,
                }
            }
            
            # Получаем количество документов (приблизительно)
            try:
                total_count = remote_collection.count_documents(query)
                print(f"   Найдено документов: {total_count}")
            except:
                total_count = 0
                print("   Не удалось получить точное количество документов")
            
            # Этап 4: Синхронизация коллекций баз данных
            print("\n4️⃣  Синхронизация коллекций баз данных...")
            cursor = remote_collection.find(query, no_cursor_timeout=True, batch_size=self.config.batch_size)
            
            # Применяем лимит если задан
            if limit > 0:
                cursor = cursor.limit(limit)
            
            scanned = 0
            inserted = 0
            skipped_existing = 0
            errors_count = 0
            errors = []
            
            batch = []
            
            for raw_doc in cursor:
                batch.append(raw_doc)
                scanned += 1
                
                # Обрабатываем пакет
                if len(batch) >= self.config.batch_size:
                    print(f"   Обработка пакета из {len(batch)} документов...")
                    
                    for doc in batch:
                        try:
                            # Извлекаем purchase notice number
                            purchase_info = doc.get("purchaseInfo", {})
                            pn = purchase_info.get("purchaseNoticeNumber") if isinstance(purchase_info, dict) else None
                            
                            if not pn:
                                errors_count += 1
                                errors.append(f"Пропущен документ без purchaseNoticeNumber")
                                continue
                            
                            # Проверяем дубликаты
                            existing = local_collection.find_one({
                                "purchaseNoticeNumber": str(pn),
                                "source": "remote_mongo_direct"
                            })
                            
                            if existing:
                                skipped_existing += 1
                                continue
                            
                            # Создаем документ для вставки
                            doc_to_insert = self._create_protocol_document(doc)
                            
                            # Пытаемся вставить документ (игнорируем дубликаты)
                            try:
                                local_collection.insert_one(doc_to_insert)
                                inserted += 1
                            except Exception as e:
                                # Если дубликат - просто пропускаем, это нормально
                                if "duplicate key" in str(e):
                                    continue
                                else:
                                    # Другие ошибки - логируем
                                    raise e
                            
                        except Exception as e:
                            errors_count += 1
                            error_msg = f"Ошибка обработки документа: {e}"
                            errors.append(error_msg)
                            print(f"   ❌ {error_msg}")
                    
                    batch = []
            
            # Обрабатываем оставшиеся документы
            if batch:
                print(f"   Обработка последнего пакета из {len(batch)} документов...")
                
                for doc in batch:
                    try:
                        # Извлекаем purchase notice number
                        purchase_info = doc.get("purchaseInfo", {})
                        pn = purchase_info.get("purchaseNoticeNumber") if isinstance(purchase_info, dict) else None
                        
                        if not pn:
                            errors_count += 1
                            errors.append(f"Пропущен документ без purchaseNoticeNumber")
                            continue
                        
                        # Проверяем дубликаты
                        existing = local_collection.find_one({
                            "purchaseNoticeNumber": str(pn),
                            "source": "remote_mongo_direct"
                        })
                        
                        if existing:
                            skipped_existing += 1
                            continue
                        
                        # Создаем документ для вставки
                        doc_to_insert = self._create_protocol_document(doc)
                        
                        # Вставляем документ
                        local_collection.insert_one(doc_to_insert)
                        inserted += 1
                        
                    except Exception as e:
                        errors_count += 1
                        error_msg = f"Ошибка обработки документа: {e}"
                        errors.append(error_msg)
                        print(f"   ❌ {error_msg}")
            
            # Этап 5: Отчет с метриками
            duration = time.time() - start_time
            print("\n5️⃣  Отчет с метриками:")
            print(f"   ✅ Просмотрено: {scanned}")
            print(f"   💾 Вставлено: {inserted}")
            print(f"   ⏭️  Пропущено (дубликаты): {skipped_existing}")
            print(f"   ❌ Ошибок: {errors_count}")
            print(f"   ⏱️  Время выполнения: {duration:.2f} секунд")
            
            return SyncResult(
                status="success" if errors_count == 0 else "partial",
                message="Синхронизация завершена",
                date=f"{start_date.date().isoformat()} - {end_date.date().isoformat()}",
                scanned=scanned,
                inserted=inserted,
                skipped_existing=skipped_existing,
                errors_count=errors_count,
                duration=duration,
                errors=errors
            )
            
        except Exception as e:
            error_msg = f"Критическая ошибка синхронизации: {e}"
            print(f"\n❌ {error_msg}")
            return SyncResult(
                status="error",
                message=error_msg,
                date=f"{start_date.date().isoformat()} - {end_date.date().isoformat()}",
                errors=[error_msg]
            )
            
        finally:
            # Закрываем соединения
            if self.remote_client:
                self.remote_client.close()
            if self.local_client:
                self.local_client.close()


def main():
    """Точка входа для CLI."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Simple Sync Microservice")
    parser.add_argument(
        "command",
        choices=["sync-date", "sync-range", "sync-full", "sync-daily"],
        help="Command to execute"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Target date for sync-date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for sync-range (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for sync-range (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Number of days for sync-full (default: 14)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of documents (0 = no limit)"
    )
    
    args = parser.parse_args()
    
    # Создаем сервис
    service = SimpleSyncService()
    
    # Выполняем команду
    if args.command == "sync-date":
        if not args.date:
            print("❌ --date argument is required for sync-date")
            return 1
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d")
            result = service.sync_protocols_for_date(target_date, args.limit)
        except ValueError:
            print(f"❌ Invalid date format: {args.date}. Use YYYY-MM-DD")
            return 1
    elif args.command == "sync-range":
        if not args.start_date or not args.end_date:
            print("❌ --start-date and --end-date arguments are required for sync-range")
            return 1
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
            result = service.sync_protocols_for_date_range(start_date, end_date, args.limit)
        except ValueError as e:
            print(f"❌ Invalid date format: {e}. Use YYYY-MM-DD")
            return 1
    elif args.command == "sync-full":
        result = service.sync_full_collection(args.days)
    elif args.command == "sync-daily":
        result = service.sync_daily_updates()
    else:
        print(f"❌ Unknown command: {args.command}")
        return 1
    
    # Выводим результат
    print("\n" + "="*60)
    print("📊 РЕЗУЛЬТАТЫ СИНХРОНИЗАЦИИ")
    print("="*60)
    
    if result.status == "success":
        print("✅ УСПЕШНО!")
    elif result.status == "partial":
        print("⚠️  ЧАСТИЧНО УСПЕШНО (с ошибками)")
    else:
        print("❌ ОШИБКА!")
    
    print(f"📅 Период: {result.date}")
    print(f"🔍 Просмотрено: {result.scanned}")
    print(f"💾 Вставлено: {result.inserted}")
    print(f"⏭️  Пропущено (дубликаты): {result.skipped_existing}")
    print(f"❌ Ошибок: {result.errors_count}")
    print(f"⏱️  Время выполнения: {result.duration:.2f} секунд")
    
    if result.errors:
        print("\n📝 Ошибки:")
        for i, error in enumerate(result.errors[:5], 1):
            print(f"   {i}. {error[:100]}{'...' if len(error) > 100 else ''}")
        if len(result.errors) > 5:
            print(f"   ... и еще {len(result.errors) - 5} ошибок")
    
    return 0 if result.status in ["success", "partial"] else 1


if __name__ == "__main__":
    exit(main())

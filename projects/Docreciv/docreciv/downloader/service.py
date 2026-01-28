"""
Microservice for downloading protocols from zakupki.gov.ru.
Main service class for protocol processing.
"""
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from .utils import load_env_file, sanitize_filename, get_metadata_client, check_zakupki_health, get_session, reset_session
from .config import (
    MONGO_METADATA_DB,
    MONGO_METADATA_PROTOCOLS_COLLECTION,
    MAX_URLS_PER_PROTOCOL,
    DOWNLOAD_HTTP_TIMEOUT,
    DOWNLOAD_CONCURRENCY,
    PROTOCOLS_CONCURRENCY,
    DEFAULT_INPUT_DIR,
)

# Load env on module import
load_env_file()


@dataclass
class DownloadResult:
    """Результат загрузки."""
    status: str
    message: str
    processed: int = 0
    downloaded: int = 0
    failed: int = 0
    duration: float = 0.0
    errors: Optional[List[str]] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class ProtocolDownloader:
    """Сервис загрузки протоколов с zakupki.gov.ru."""

    def __init__(self, output_dir: Optional[Path] = None):
        """
        Инициализирует загрузчик протоколов.
        
        Args:
            output_dir: Директория для сохранения файлов
        """
        self.output_dir = output_dir or DEFAULT_INPUT_DIR
        self.max_urls_per_protocol = MAX_URLS_PER_PROTOCOL
        self.http_timeout = DOWNLOAD_HTTP_TIMEOUT
        self.download_concurrency = DOWNLOAD_CONCURRENCY
        self.protocols_concurrency = PROTOCOLS_CONCURRENCY

    def process_pending_protocols(self, limit: int = 200) -> DownloadResult:
        """
        Обрабатывает ожидающие загрузки протоколы.
        
        Args:
            limit: Максимальное количество протоколов для обработки
            
        Returns:
            Результат обработки
        """
        start_time = time.time()
        print(f"📥 НАЧАЛО ОБРАБОТКИ ПРОТОКОЛОВ")
        print(f"   Лимит: {limit}")
        print(f"   Директория: {self.output_dir}")

        # Проверяем доступность zakupki.gov.ru
        print("\n1️⃣  Проверка доступности zakupki.gov.ru...")
        if not check_zakupki_health():
            return DownloadResult(
                status="error",
                message="zakupki.gov.ru недоступен",
                duration=0.0
            )
        print("✅ zakupki.gov.ru доступен")

        # Подключаемся к MongoDB
        print("\n2️⃣  Подключение к MongoDB...")
        client = get_metadata_client()
        if not client:
            return DownloadResult(
                status="error",
                message="Не удалось подключиться к MongoDB",
                duration=0.0
            )
        
        try:
            db = client[MONGO_METADATA_DB]
            collection = db[MONGO_METADATA_PROTOCOLS_COLLECTION]
            print("✅ Подключение к MongoDB успешно")
            
            # Находим ожидающие протоколы
            print("\n3️⃣  Поиск ожидающих протоколов...")
            query = {"status": "pending", "source": "remote_mongo_direct"}
            cursor = collection.find(query).limit(limit)
            protocols = list(cursor)
            
            print(f"   Найдено протоколов: {len(protocols)}")
            
            if not protocols:
                return DownloadResult(
                    status="success",
                    message="Нет протоколов для загрузки",
                    duration=0.0
                )
            
            # Обрабатываем протоколы
            print("\n4️⃣  Загрузка документов...")
            processed = 0
            downloaded = 0
            failed = 0
            errors = []
            
            # Обрабатываем протоколы параллельно
            with ThreadPoolExecutor(max_workers=self.protocols_concurrency) as executor:
                futures = []
                
                for protocol in protocols:
                    if processed >= limit > 0:
                        break
                    
                    future = executor.submit(self._process_single_protocol, protocol, collection)
                    futures.append(future)
                    processed += 1
                
                # Собираем результаты
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        downloaded += result.get("downloaded", 0)
                        failed += result.get("failed", 0)
                        if result.get("error"):
                            errors.append(result["error"])
                    except Exception as e:
                        failed += 1
                        errors.append(f"Ошибка обработки протокола: {e}")
            
            # Результат
            duration = time.time() - start_time
            print("\n5️⃣  Результаты:")
            print(f"   ✅ Обработано протоколов: {processed}")
            print(f"   💾 Скачано документов: {downloaded}")
            print(f"   ❌ Ошибок: {failed}")
            print(f"   ⏱️  Время выполнения: {duration:.2f} секунд")
            
            return DownloadResult(
                status="success" if failed == 0 else "partial",
                message="Обработка завершена",
                processed=processed,
                downloaded=downloaded,
                failed=failed,
                duration=duration,
                errors=errors
            )
            
        except Exception as e:
            error_msg = f"Критическая ошибка обработки: {e}"
            print(f"\n❌ {error_msg}")
            return DownloadResult(
                status="error",
                message=error_msg,
                duration=time.time() - start_time,
                errors=[error_msg]
            )
            
        finally:
            if client:
                client.close()

    def _process_single_protocol(self, protocol: Dict[str, Any], collection) -> Dict[str, Any]:
        """Обрабатывает один протокол."""
        unit_id = protocol.get("unit_id")
        urls = protocol.get("urls", [])
        
        if not unit_id:
            return {"downloaded": 0, "failed": 1, "error": "Протокол без unit_id"}
        
        if not urls:
            try:
                collection.update_one(
                    {"unit_id": unit_id},
                    {"$set": {"status": "downloaded", "updated_at": datetime.utcnow()}}
                )
                return {"downloaded": 0, "failed": 0}
            except Exception as e:
                return {"downloaded": 0, "failed": 1, "error": f"Ошибка обновления статуса: {e}"}
        
        # Создаем директорию для unit
        unit_dir = self.output_dir / unit_id
        unit_dir.mkdir(parents=True, exist_ok=True)
        
        downloaded_count = 0
        failed_count = 0
        
        # Ограничиваем количество URL для загрузки
        urls_to_download = urls[:self.max_urls_per_protocol]
        
        # Скачиваем документы параллельно
        with ThreadPoolExecutor(max_workers=self.download_concurrency) as executor:
            futures = []
            
            for i, url_info in enumerate(urls_to_download):
                url = url_info.get("url")
                if not url:
                    continue
                
                # Создаем имя файла
                original_name = url_info.get("fileName") or f"document_{i+1}.pdf"
                safe_name = sanitize_filename(original_name)
                file_path = unit_dir / safe_name
                
                future = executor.submit(self._download_file, url, file_path)
                futures.append((future, file_path, url))
            
            # Собираем результаты
            for future, file_path, url in futures:
                try:
                    success = future.result()
                    if success:
                        downloaded_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    failed_count += 1
                    print(f"❌ Ошибка при ожидании результата для {url}: {e}")
        
        # Обновляем статус в MongoDB
        try:
            collection.update_one(
                {"unit_id": unit_id},
                {"$set": {"status": "downloaded", "updated_at": datetime.utcnow()}}
            )
        except Exception as e:
            print(f"❌ Ошибка обновления статуса для {unit_id}: {e}")
            failed_count += 1
        
        return {
            "downloaded": downloaded_count,
            "failed": failed_count,
            "error": None if failed_count == 0 else f"Не удалось скачать {failed_count} документов"
        }

    def _download_file(self, url: str, output_path: Path) -> bool:
        """Скачивает один файл."""
        try:
            session = get_session()
            response = session.get(url, timeout=self.http_timeout, stream=True)
            response.raise_for_status()
            
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            return True
        except Exception as e:
            print(f"❌ Ошибка скачивания {url}: {e}")
            return False


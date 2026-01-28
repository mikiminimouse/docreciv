"""
Asyncio-based protocol downloader with aiohttp.

Миграция с ThreadPoolExecutor на asyncio для:
- Увеличения количества одновременных соединений (100+ вместо 15-20)
- Уменьшения memory overhead
- Улучшения throughput для I/O bound задач

Использование:
    async with AsyncioProtocolDownloader(output_dir) as downloader:
        result = await downloader.process_download_request(request)
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

import aiohttp
import aiofiles
from motor.motor_asyncio import AsyncIOMotorClient

from ..core.config import get_config
from ..vpn_utils import ensure_vpn_connected, check_vpn_routes, is_vpn_interface_up
from .utils import sanitize_filename, check_zakupki_health
from .meta_generator import MetaGenerator
from .file_manager import FileManager
from .models import DownloadRequest


# DownloadResult defined locally (sync to models.py structure)
@dataclass
class DownloadResult:
    """Результат выполнения скачивания."""
    status: str
    message: str
    processed: int = 0
    downloaded: int = 0
    failed: int = 0
    duration: float = 0.0
    errors: Optional[List[str]] = None
    warnings: Optional[List[str]] = None
    statistics: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
        if self.statistics is None:
            self.statistics = {}

# Configure logging
logger = logging.getLogger(__name__)


class AsyncioProtocolDownloader:
    """
    Асинхронный downloader с поддержкой 100+ одновременных соединений.

    Использует:
    - aiohttp для HTTP запросов (async)
    - Motor для MongoDB (async)
    - aiofiles для файловых операций (async)
    """

    def __init__(self, output_dir: Optional[Path] = None):
        """
        Инициализация асинхронного downloader.

        Args:
            output_dir: Директория для сохранения файлов
        """
        self.config = get_config().async_downloader
        self.output_dir = output_dir or get_config().downloader.output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Семофоры для ограничения конкурентности
        self.request_semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        self.protocol_semaphore = asyncio.Semaphore(self.config.max_concurrent_protocols)

        # Сессии будут созданы при входе в context manager
        self.session: Optional[aiohttp.ClientSession] = None
        self.mongo_client: Optional[AsyncIOMotorClient] = None

        # Вспомогательные компоненты
        self.file_manager = FileManager(self.output_dir)
        self.meta_generator = MetaGenerator()

        # Счётчики для статистики
        self.reset_counters()

        logger.info(f"Initialized AsyncioProtocolDownloader with output dir: {self.output_dir}")

    def reset_counters(self):
        """Сброс счётчиков статистики."""
        self.counters = {
            "file_sizes": [],
            "download_times": [],
            "error_types": {},
            "file_types": {},
        }

    async def __aenter__(self):
        """Создание сессий при входе в async context manager."""
        # aiohttp сессия с настройками подключения
        connector = aiohttp.TCPConnector(
            limit=self.config.max_concurrent_requests,  # размер пула соединений
            limit_per_host=self.config.limit_per_host,  # макс. соединений на один хост
            ttl_dns_cache=300,  # кеширование DNS на 5 минут
            enable_cleanup_closed=True,  # очистка закрытых соединений
        )

        timeout = aiohttp.ClientTimeout(
            total=self.config.connection_timeout,
            connect=30,
            sock_read=self.config.read_timeout
        )

        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=self.config.browser_headers
        )

        # Motor MongoDB client
        mongo_config = get_config().sync_db.local_mongo
        mongo_url = (
            f"mongodb://{mongo_config.user}:{mongo_config.password}"
            f"@{mongo_config.server}"
        )

        self.mongo_client = AsyncIOMotorClient(
            mongo_url,
            serverSelectionTimeoutMS=20000,
            connectTimeoutMS=10000
        )

        logger.info("Async sessions created (aiohttp, Motor)")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Очистка ресурсов при выходе из async context manager."""
        if self.session:
            await self.session.close()
            logger.debug("aiohttp session closed")
        if self.mongo_client:
            self.mongo_client.close()
            logger.debug("Motor client closed")

    async def _download_single_file(
        self,
        url: str,
        output_path: Path
    ) -> bool:
        """
        Скачивание одного файла с aiohttp.

        Args:
            url: URL для скачивания
            output_path: Путь для сохранения файла

        Returns:
            True если успешно, False иначе
        """
        start_time = time.time()
        file_size = 0

        async with self.request_semaphore:  # Ограничиваем одновременные запросы
            try:
                output_path.parent.mkdir(parents=True, exist_ok=True)

                async with self.session.get(url) as response:
                    response.raise_for_status()

                    # Читаем и записываем файл асинхронно
                    async with aiofiles.open(output_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(self.config.chunk_size):
                            await f.write(chunk)

                    # Получаем размер файла
                    file_size = output_path.stat().st_size

                download_time = time.time() - start_time
                self._update_statistics(url, file_size, download_time, True)

                logger.info(f"Successfully downloaded {url} ({file_size} bytes in {download_time:.2f}s)")
                return True

            except asyncio.TimeoutError:
                error_msg = f"Timeout downloading {url}"
                logger.error(error_msg)
                self.counters["error_types"]["timeout"] = \
                    self.counters["error_types"].get("timeout", 0) + 1
                self._update_statistics(url, file_size, time.time() - start_time, False)
                return False

            except aiohttp.ClientConnectorError as e:
                error_msg = f"Connection error downloading {url}: {e}"
                logger.error(error_msg)
                self.counters["error_types"]["connection"] = \
                    self.counters["error_types"].get("connection", 0) + 1
                self._update_statistics(url, file_size, time.time() - start_time, False)
                return False

            except aiohttp.ClientResponseError as e:
                error_msg = f"HTTP error {e.status} downloading {url}"
                logger.error(error_msg)
                error_type = f"http_{e.status}"
                self.counters["error_types"][error_type] = \
                    self.counters["error_types"].get(error_type, 0) + 1
                self._update_statistics(url, file_size, time.time() - start_time, False)
                return False

            except Exception as e:
                error_msg = f"Error downloading {url}: {str(e)}"
                logger.error(error_msg)
                self.counters["error_types"]["other"] = \
                    self.counters["error_types"].get("other", 0) + 1
                self._update_statistics(url, file_size, time.time() - start_time, False)
                return False

    def _update_statistics(self, url: str, file_size: int, download_time: float, success: bool):
        """Обновление внутренней статистики."""
        self.counters["file_sizes"].append(file_size)
        self.counters["download_times"].append(download_time)

        # Тип файла
        file_extension = Path(url).suffix.lower() if '.' in url else "unknown"
        self.counters["file_types"][file_extension] = \
            self.counters["file_types"].get(file_extension, 0) + 1

    async def _process_protocol(
        self,
        protocol: Dict[str, Any],
        db,
        skip_existing: bool = True,
        force_reload: bool = False
    ) -> Dict[str, Any]:
        """
        Обработка одного протокола со всеми файлами.

        Args:
            protocol: Документ протокола из MongoDB
            db: База данных MongoDB
            skip_existing: Пропускать существующие директории
            force_reload: Принудительная перезагрузка

        Returns:
            Словарь с результатами обработки
        """
        async with self.protocol_semaphore:
            unit_id = protocol.get("unit_id")
            urls = protocol.get("urls", [])

            if not unit_id:
                return {"downloaded": 0, "failed": 1, "error": "Protocol missing unit_id"}

            if not urls:
                # Обновляем статус даже если нет URL
                await db.protocols.update_one(
                    {"unit_id": unit_id},
                    {"$set": {"status": "downloaded", "updated_at": datetime.utcnow()}}
                )
                return {"downloaded": 0, "failed": 0}

            # Формируем директорию
            load_date = protocol.get("loadDate")
            if isinstance(load_date, datetime):
                date_str = load_date.strftime("%Y-%m-%d")
            elif isinstance(load_date, str):
                try:
                    date_obj = datetime.fromisoformat(load_date.replace('Z', '+00:00'))
                    date_str = date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    date_str = datetime.utcnow().strftime("%Y-%m-%d")
            else:
                date_str = datetime.utcnow().strftime("%Y-%m-%d")

            # Проверяем существование директории
            unit_dir = self.file_manager.get_unit_dir(date_str, unit_id)
            unit_exists = self.file_manager.unit_dir_exists(date_str, unit_id)

            if unit_exists and not force_reload and skip_existing:
                logger.debug(f"Skipping existing unit directory: {unit_dir}")
                await db.protocols.update_one(
                    {"unit_id": unit_id},
                    {"$set": {"status": "downloaded", "updated_at": datetime.utcnow()}}
                )
                return {"downloaded": 0, "failed": 0, "skipped": True}

            # Создаём директорию
            unit_dir = self.file_manager.create_unit_dir(date_str, unit_id)

            # При force_reload удаляем существующие файлы (кроме meta)
            if unit_exists and force_reload:
                logger.info(f"Force reload for existing unit: {unit_dir}")
                for file_path in unit_dir.iterdir():
                    if file_path.is_file() and file_path.name not in ["unit.meta.json", "raw_url_map.json"]:
                        file_path.unlink()

            # Создаём задачи для всех файлов
            file_tasks = []
            download_results = []

            for i, url_info in enumerate(urls[:self.config.max_urls_per_protocol]):
                url = url_info.get("url")
                if not url:
                    continue

                filename = sanitize_filename(url_info.get("fileName", f"document_{i+1}.pdf"))
                file_path = unit_dir / filename

                # Создаём задачу скачивания
                task = self._download_single_file(url, file_path)
                file_tasks.append((task, file_path, url, url_info))

            # Ждём завершения всех файлов
            results = await asyncio.gather(
                *[task for task, *_ in file_tasks],
                return_exceptions=True
            )

            # Собираем результаты
            downloaded_count = 0
            failed_count = 0

            for i, result in enumerate(results):
                _, file_path, url, url_info = file_tasks[i]

                if isinstance(result, Exception):
                    failed_count += 1
                    download_results.append({
                        "url": url,
                        "filename": file_path.name,
                        "status": "error",
                        "error": str(result)
                    })
                elif result is True:
                    downloaded_count += 1
                    download_results.append({
                        "url": url,
                        "filename": file_path.name,
                        "status": "ok",
                        "guid": url_info.get("guid"),
                        "contentUid": url_info.get("contentUid"),
                    })
                else:
                    failed_count += 1
                    download_results.append({
                        "url": url,
                        "filename": file_path.name,
                        "status": "failed",
                    })

            # Создаём meta файлы
            try:
                self.meta_generator.create_unit_meta(
                    unit_dir, unit_id, protocol, date_str,
                    downloaded_count, failed_count, len(urls)
                )
                self.meta_generator.create_raw_url_map(unit_dir, download_results)
            except Exception as e:
                logger.error(f"Error creating meta files for {unit_id}: {e}")

            # Обновляем статус в MongoDB
            try:
                await db.protocols.update_one(
                    {"unit_id": unit_id},
                    {"$set": {"status": "downloaded", "updated_at": datetime.utcnow()}}
                )
            except Exception as e:
                logger.error(f"Error updating status for {unit_id}: {e}")
                failed_count += 1

            return {
                "downloaded": downloaded_count,
                "failed": failed_count,
                "error": None if failed_count == 0 else f"Failed to download {failed_count} documents"
            }

    async def _check_vpn(self) -> bool:
        """
        Проверка VPN соединения.

        Returns:
            True если VPN доступен
        """
        # Проверяем интерфейс
        if not is_vpn_interface_up():
            logger.error("VPN interface (tun0/tap0) is not up")
            return False

        # Проверяем маршруты
        routes = check_vpn_routes(["zakupki.gov.ru", "www.zakupki.gov.ru"])
        if not any(routes.values()):
            logger.warning("Route to zakupki.gov.ru not found through VPN interface")

        # Проверяем доступность
        vpn_connected, vpn_message = ensure_vpn_connected()
        if not vpn_connected:
            logger.error(f"VPN check failed: {vpn_message}")
            return False

        # Проверяем через HTTP
        try:
            async with self.session.get("https://zakupki.gov.ru", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                return resp.status < 500
        except Exception as e:
            logger.error(f"zakupki.gov.ru unavailable: {e}")
            return False

    async def process_download_request(self, request: DownloadRequest) -> DownloadResult:
        """
        Главная функция обработки запроса на скачивание.

        Args:
            request: DownloadRequest с параметрами

        Returns:
            DownloadResult с результатами
        """
        start_time = time.time()
        self.reset_counters()

        logger.info(f"Starting async download request: max_units={request.max_units_per_run}, "
                   f"dry_run={request.dry_run}, force_reload={request.force_reload}")

        # Проверяем VPN
        logger.info("Checking VPN connection...")
        if not await self._check_vpn():
            return DownloadResult(
                status="error",
                message="VPN connection required",
                duration=0.0,
                errors=["VPN not connected"]
            )

        # Подключаемся к MongoDB
        db_name = get_config().downloader.mongo.db
        collection_name = get_config().downloader.mongo.collection
        db = self.mongo_client[db_name]

        # Строим запрос
        query = {"source": "remote_mongo_direct"}

        if request.record_ids:
            from bson import ObjectId
            object_ids = []
            for rid in request.record_ids:
                try:
                    object_ids.append(ObjectId(rid) if len(rid) == 24 else rid)
                except:
                    object_ids.append(rid)
            query["_id"] = {"$in": object_ids}
        else:
            if request.force_reload:
                query["status"] = {"$in": ["pending", "downloaded"]}
            else:
                query["status"] = "pending"

        # Добавляем фильтр по дате
        if request.from_date and request.to_date:
            date_start = datetime.combine(request.from_date.date(), datetime.min.time())
            date_end = datetime.combine(request.to_date.date(), datetime.max.time())
            query["loadDate"] = {"$gte": date_start, "$lte": date_end}
            logger.info(f"Filtering by loadDate range: {request.from_date.date()} - {request.to_date.date()}")

        # Получаем протоколы
        cursor = db[collection_name].find(query).sort("loadDate", 1)

        # Применяем лимит
        if request.max_units_per_run and request.max_units_per_run > 0:
            cursor = cursor.limit(request.max_units_per_run)

        # Загружаем все протоколы (асинхронно)
        protocols = await cursor.to_list(length=None)
        logger.info(f"Found {len(protocols)} protocols to process")

        if not protocols:
            return DownloadResult(
                status="success",
                message="No protocols to process",
                duration=0.0
            )

        if request.dry_run:
            return DownloadResult(
                status="success",
                message=f"DRY-RUN: Would process {len(protocols)} protocols",
                processed=len(protocols),
                duration=time.time() - start_time
            )

        # Создаём задачи для протоколов
        protocol_tasks = [
            self._process_protocol(
                protocol,
                db,
                skip_existing=request.skip_existing,
                force_reload=request.force_reload
            )
            for protocol in protocols
        ]

        # Выполняем с gather
        results = await asyncio.gather(*protocol_tasks, return_exceptions=True)

        # Собираем статистику
        downloaded = 0
        failed = 0
        skipped = 0
        errors = []

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed += 1
                errors.append(str(result))
            elif isinstance(result, dict):
                downloaded += result.get("downloaded", 0)
                failed += result.get("failed", 0)
                if result.get("skipped"):
                    skipped += 1
                if result.get("error"):
                    errors.append(result["error"])

        duration = time.time() - start_time

        result = DownloadResult(
            status="success" if failed == 0 else "partial",
            message=f"Processed {len(protocols)} protocols: {downloaded} files downloaded, {failed} failed, {skipped} skipped",
            processed=len(protocols),
            downloaded=downloaded,
            failed=failed,
            duration=duration,
            errors=errors,
            statistics={}
        )

        logger.info("Async download completed:")
        logger.info(f"  Processed: {result.processed}")
        logger.info(f"  Downloaded: {result.downloaded}")
        logger.info(f"  Failed: {result.failed}")
        logger.info(f"  Skipped: {skipped}")
        logger.info(f"  Duration: {duration:.2f}s")
        logger.info(f"  Throughput: {result.downloaded / duration:.2f} files/s" if duration > 0 else "")

        return result


# Вспомогательная функция для запуска из синхронного кода
def run_async_download(coro):
    """
    Запуск асинхронной функции из синхронного кода.

    Args:
        coro: Корутина для выполнения

    Returns:
        Результат выполнения корутины
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(coro)

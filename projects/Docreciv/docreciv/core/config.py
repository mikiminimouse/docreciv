"""
Централизованная система конфигурации для модуля docreciv.

Загружает и валидирует все настройки из переменных окружения.
Предоставляет типизированные dataclasses для каждой группы настроек.
"""
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


def load_env_file(env_file: Optional[Path] = None) -> None:
    """
    Загружает переменные окружения из .env файла если он существует.
    
    Args:
        env_file: Путь к .env файлу. Если не указан, ищет в корне проекта и preprocessing директории.
    """
    if env_file is None:
        # Ищем .env файл в нескольких местах
        # __file__ = /home/pak/projects/Docreciv/docreciv/core/config.py
        project_root = Path(__file__).parent.parent.parent  # /home/pak/projects/Docreciv
        possible_locations = [
            project_root / ".env",  # /home/pak/projects/Docreciv/.env (приоритет)
        ]
        
        # Используем первый найденный файл
        for location in possible_locations:
            if location.exists():
                env_file = location
                break
        else:
            # Если файл не найден, используем project_root/.env
            env_file = project_root / ".env"
    
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
class MongoConfig:
    """Конфигурация подключения к MongoDB."""
    server: str
    user: str
    password: str
    db: str
    auth_source: str = "admin"
    ssl_cert: Optional[str] = None
    collection: Optional[str] = None
    
    def get_connection_url(self) -> str:
        """Возвращает URL подключения к MongoDB."""
        url = f"mongodb://{self.user}:{self.password}@{self.server}/"
        if self.auth_source:
            url += f"?authSource={self.auth_source}"
        return url


@dataclass
class RouterConfig:
    """Конфигурация Router компонента (используется в Docprep/Doclingproc)."""

    @staticmethod
    def _get_default_dir(env_var: str, fallback: str) -> Path:
        """Получает дефолтную директорию с fallback на local dev путь."""
        base_dir = Path(os.environ.get("PROCESSING_DATA_DIR", "/home/pak/Processing data"))
        return Path(os.environ.get(env_var, base_dir / fallback))

    # Директории (используем base_dir как основу)
    input_dir: Path = field(default_factory=lambda: RouterConfig._get_default_dir("INPUT_DIR", "Input"))
    temp_dir: Path = field(default_factory=lambda: RouterConfig._get_default_dir("TEMP_DIR", "temp"))
    output_dir: Path = field(default_factory=lambda: RouterConfig._get_default_dir("OUTPUT_DIR", "output"))
    extracted_dir: Path = field(default_factory=lambda: RouterConfig._get_default_dir("EXTRACTED_DIR", "extracted"))
    normalized_dir: Path = field(default_factory=lambda: RouterConfig._get_default_dir("NORMALIZED_DIR", "normalized"))
    archive_dir: Path = field(default_factory=lambda: RouterConfig._get_default_dir("ARCHIVE_DIR", "archive"))

    # Pending директории
    pending_dir: Path = field(default_factory=lambda: RouterConfig._get_default_dir("PENDING_DIR", "pending"))
    
    @property
    def pending_direct_dir(self) -> Path:
        return self.pending_dir / "direct"
    
    @property
    def pending_normalize_dir(self) -> Path:
        return self.pending_dir / "normalize"
    
    @property
    def pending_convert_dir(self) -> Path:
        return self.pending_dir / "convert"
    
    @property
    def pending_extract_dir(self) -> Path:
        return self.pending_dir / "extract"
    
    @property
    def pending_special_dir(self) -> Path:
        return self.pending_dir / "special"
    
    @property
    def pending_mixed_dir(self) -> Path:
        return self.pending_dir / "mixed"
    
    # Extract sorted директории
    extract_sorted_dir: Path = field(default_factory=lambda: RouterConfig._get_default_dir("EXTRACT_SORTED_DIR", "extract_sorted"))

    # Ready директории
    ready_docling_dir: Path = field(default_factory=lambda: RouterConfig._get_default_dir("READY_DOCLING_DIR", "ready_docling"))
    ready_dir: Path = field(default_factory=lambda: RouterConfig._get_default_dir("READY_DIR", "ready"))

    # Дополнительные директории
    detected_dir: Path = field(default_factory=lambda: RouterConfig._get_default_dir("DETECTED_DIR", "detected"))
    converted_dir: Path = field(default_factory=lambda: RouterConfig._get_default_dir("CONVERTED_DIR", "converted"))
    
    # API конфигурация
    docling_api: str = field(default_factory=lambda: os.environ.get("DOCLING_API", "http://docling:8000/process"))
    webhook_secret: str = field(default_factory=lambda: os.environ.get("WEBHOOK_SECRET", "secret"))
    
    # Лимиты и параметры
    max_unpack_size_mb: int = field(default_factory=lambda: int(os.environ.get("MAX_UNPACK_SIZE_MB", "500")))
    max_files_in_archive: int = field(default_factory=lambda: int(os.environ.get("MAX_FILES_IN_ARCHIVE", "1000")))
    protocols_count_limit: int = field(default_factory=lambda: int(os.environ.get("PROTOCOLS_COUNT_LIMIT", "100")))
    
    # Типы файлов
    signature_extensions: List[str] = field(default_factory=lambda: [".sig", ".p7s", ".pem", ".cer", ".crt"])
    unsupported_extensions: List[str] = field(default_factory=lambda: [".exe", ".dll", ".db", ".tmp", ".log", ".ini", ".sys", ".bat", ".sh"])
    complex_extensions: List[str] = field(default_factory=lambda: [".tar.gz", ".tar.bz2", ".tar.xz", ".docm", ".xlsm", ".pptm", ".dotx", ".xltx", ".potx"])
    convertible_types: Dict[str, str] = field(default_factory=lambda: {"doc": "docx", "xls": "xlsx", "ppt": "pptx"})
    supported_file_types: List[str] = field(default_factory=lambda: ["pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "rtf", "html", "xml", "jpg", "jpeg", "png", "gif", "bmp", "tiff"])
    archive_types: List[str] = field(default_factory=lambda: ["zip_archive", "rar_archive", "7z_archive"])
    
    def ensure_directories(self) -> None:
        """Создает все необходимые директории. Игнорирует ошибки для неиспользуемых путей."""
        directories = [
            self.input_dir, self.temp_dir, self.output_dir, self.extracted_dir,
            self.normalized_dir, self.archive_dir, self.pending_dir,
            self.pending_direct_dir, self.pending_normalize_dir, self.pending_convert_dir,
            self.pending_extract_dir, self.pending_special_dir, self.pending_mixed_dir,
            self.extract_sorted_dir, self.ready_docling_dir, self.ready_dir,
            self.detected_dir, self.converted_dir
        ]
        errors = []
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except (PermissionError, OSError) as e:
                # Логируем, но не прерываем работу — директория может быть не нужна
                errors.append(f"{directory}: {e}")

        if errors:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning("Ошибки конфигурации:\n  - " + "\n  - ".join(errors))


@dataclass
class SyncDBConfig:
    """Конфигурация Sync DB компонента."""
    # Удаленная MongoDB
    remote_mongo: MongoConfig = field(default_factory=lambda: MongoConfig(
        server=os.environ.get("MONGO_SERVER", "192.168.0.46:8635"),
        user=os.environ.get("MONGO_USER", "readProtocols223"),
        password=os.environ.get("MONGO_PASSWORD", ""),
        db=os.environ.get("MONGO_PROTOCOLS_DB", "protocols223"),
        auth_source="protocols223",
        ssl_cert=os.environ.get("MONGO_SSL_CERT"),
        collection=os.environ.get("MONGO_PROTOCOLS_COLLECTION", "purchaseProtocol")
    ))
    
    # Локальная MongoDB
    local_mongo: MongoConfig = field(default_factory=lambda: MongoConfig(
        server=os.environ.get("LOCAL_MONGO_SERVER", os.environ.get("MONGO_METADATA_SERVER", "localhost:27018")),
        user=os.environ.get("MONGO_METADATA_USER", "docling_user"),
        password=os.environ.get("MONGO_METADATA_PASSWORD", ""),
        db=os.environ.get("MONGO_METADATA_DB", "docling_metadata"),
        auth_source="admin",
        collection=os.environ.get("MONGO_PROTOCOLS_COLLECTION_LOCAL", "protocols")
    ))
    
    # Параметры синхронизации
    batch_size: int = field(default_factory=lambda: int(os.environ.get("SYNC_BATCH_SIZE", "1000")))
    max_workers: int = field(default_factory=lambda: int(os.environ.get("SYNC_MAX_WORKERS", "4")))


@dataclass
class DownloaderConfig:
    """Конфигурация Downloader компонента."""
    # MongoDB для чтения протоколов
    mongo: MongoConfig = field(default_factory=lambda: MongoConfig(
        server=os.environ.get("MONGO_METADATA_SERVER", os.environ.get("LOCAL_MONGO_SERVER", "localhost:27018")),
        user=os.environ.get("MONGO_METADATA_USER", "docling_user"),
        password=os.environ.get("MONGO_METADATA_PASSWORD", ""),
        db=os.environ.get("MONGO_METADATA_DB", "docling_metadata"),
        auth_source="admin",
        collection=os.environ.get("MONGO_METADATA_PROTOCOLS_COLLECTION", "protocols")
    ))
    
    # Директория для сохранения файлов
    # Используем processing_base_dir как базовый путь для consistency
    output_dir: Path = field(default_factory=lambda: Path(
        os.environ.get("INPUT_DIR",
                      os.environ.get("PROCESSING_DATA_DIR", "/home/pak/Processing data"))
    ))
    
    # Параметры загрузки
    max_urls_per_protocol: int = field(default_factory=lambda: int(os.environ.get("MAX_URLS_PER_PROTOCOL", "15")))
    download_http_timeout: int = field(default_factory=lambda: int(os.environ.get("DOWNLOAD_HTTP_TIMEOUT", "120")))
    download_concurrency: int = field(default_factory=lambda: int(os.environ.get("DOWNLOAD_CONCURRENCY", "20")))
    protocols_concurrency: int = field(default_factory=lambda: int(os.environ.get("PROTOCOLS_CONCURRENCY", "20")))
    
    # HTTP заголовки
    browser_headers: Dict[str, str] = field(default_factory=lambda: {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })


@dataclass
class AsyncDownloaderConfig:
    """Конфигурация Asyncio Downloader компонента."""
    # Максимальное количество одновременных HTTP запросов
    max_concurrent_requests: int = field(default_factory=lambda: int(os.environ.get("ASYNC_MAX_CONCURRENT_REQUESTS", "100")))

    # Максимальное количество параллельно обрабатываемых протоколов
    max_concurrent_protocols: int = field(default_factory=lambda: int(os.environ.get("ASYNC_MAX_CONCURRENT_PROTOCOLS", "50")))

    # Таймауты
    connection_timeout: int = field(default_factory=lambda: int(os.environ.get("ASYNC_CONNECTION_TIMEOUT", "120")))
    read_timeout: int = field(default_factory=lambda: int(os.environ.get("ASYNC_READ_TIMEOUT", "300")))

    # Размер чанка для скачивания
    chunk_size: int = field(default_factory=lambda: int(os.environ.get("ASYNC_CHUNK_SIZE", "8192")))

    # Максимальное количество URL на протокол
    max_urls_per_protocol: int = field(default_factory=lambda: int(os.environ.get("ASYNC_MAX_URLS_PER_PROTOCOL", "15")))

    # Максимальное соединений на один хост
    limit_per_host: int = field(default_factory=lambda: int(os.environ.get("ASYNC_LIMIT_PER_HOST", "20")))

    # HTTP заголовки (те же что и для sync downloader)
    browser_headers: Dict[str, str] = field(default_factory=lambda: {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })


@dataclass
class SchedulerConfig:
    """Конфигурация Scheduler компонента."""
    router_api: str = field(default_factory=lambda: os.environ.get("ROUTER_API", "http://router:8080/process_now"))
    schedule_cron: str = field(default_factory=lambda: os.environ.get("SCHEDULE_CRON", "*/15 * * * *"))
    sync_schedule_cron: str = field(default_factory=lambda: os.environ.get("SYNC_SCHEDULE_CRON", "0 2 * * *"))
    log_level: str = field(default_factory=lambda: os.environ.get("LOG_LEVEL", "INFO"))


@dataclass
class MetricsConfig:
    """Конфигурация метрик."""
    # MongoDB для метрик
    mongo: MongoConfig = field(default_factory=lambda: MongoConfig(
        server=os.environ.get("MONGO_METADATA_SERVER", os.environ.get("LOCAL_MONGO_SERVER", "localhost:27018")),
        user=os.environ.get("MONGO_METADATA_USER", "docling_user"),
        password=os.environ.get("MONGO_METADATA_PASSWORD", ""),
        db=os.environ.get("MONGO_METADATA_DB", "docling_metadata"),
        auth_source="admin",
        collection=os.environ.get("MONGO_METRICS_COLLECTION", "processing_metrics")
    ))

    # Коллекции
    manifests_collection: str = field(default_factory=lambda: os.environ.get("MONGO_METADATA_COLLECTION", "manifests"))
    protocols_collection: str = field(default_factory=lambda: os.environ.get("MONGO_PROTOCOLS_COLLECTION_LOCAL", "protocols"))


@dataclass
class PipelineConfig:
    """Конфигурация pipeline метаданных."""
    # MongoDB для pipeline runs
    mongo: MongoConfig = field(default_factory=lambda: MongoConfig(
        server=os.environ.get("MONGO_METADATA_SERVER", os.environ.get("LOCAL_MONGO_SERVER", "localhost:27018")),
        user=os.environ.get("MONGO_METADATA_USER", "docling_user"),
        password=os.environ.get("MONGO_METADATA_PASSWORD", ""),
        db=os.environ.get("MONGO_METADATA_DB", "docling_metadata"),
        auth_source="admin",
        collection="pipeline_runs"
    ))

    # Базовая директория для Processing data
    processing_base_dir: Path = field(default_factory=lambda: Path(
        os.environ.get("PROCESSING_DATA_DIR", "/home/pak/Processing data")
    ))

    # Настройки contract файла
    contract_filename: str = "pipeline.contract.json"
    contract_version: str = "1.0"

    # Этапы pipeline
    stages: List[str] = field(default_factory=lambda: ["download", "docprep", "docling", "llm_qaenrich"])


@dataclass
class AppConfig:
    """Главная конфигурация приложения."""
    router: RouterConfig = field(default_factory=RouterConfig)
    sync_db: SyncDBConfig = field(default_factory=SyncDBConfig)
    downloader: DownloaderConfig = field(default_factory=DownloaderConfig)
    async_downloader: AsyncDownloaderConfig = field(default_factory=AsyncDownloaderConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    
    def validate(self) -> None:
        """
        Валидирует конфигурацию.
        
        Raises:
            ValueError: Если конфигурация невалидна
        """
        errors = []
        
        # Проверка обязательных полей MongoDB
        if not self.sync_db.remote_mongo.password:
            errors.append("MONGO_PASSWORD не установлен для удаленной MongoDB")
        
        if not self.sync_db.local_mongo.password:
            errors.append("MONGO_METADATA_PASSWORD не установлен для локальной MongoDB")
        
        # Проверка директорий
        try:
            self.router.ensure_directories()
        except Exception as e:
            errors.append(f"Ошибка создания директорий: {e}")
        
        if errors:
            raise ValueError(f"Ошибки конфигурации:\n" + "\n".join(f"  - {e}" for e in errors))


# Глобальный экземпляр конфигурации
_config: Optional[AppConfig] = None


def get_config(force_reload: bool = False) -> AppConfig:
    """
    Получает глобальную конфигурацию приложения.
    
    Args:
        force_reload: Если True, перезагружает конфигурацию из переменных окружения
    
    Returns:
        Экземпляр AppConfig
    """
    global _config
    
    if _config is None or force_reload:
        _config = AppConfig()
        _config.validate()
    
    return _config


def set_config(config: AppConfig) -> None:
    """
    Устанавливает конфигурацию (для тестирования).
    
    Args:
        config: Экземпляр AppConfig
    """
    global _config
    _config = config


# Инициализируем конфигурацию при импорте модуля
try:
    _config = AppConfig()
    _config.validate()
except ValueError as e:
    # Не падаем при импорте, просто выводим предупреждение
    print(f"⚠️  Предупреждение при загрузке конфигурации: {e}")
    _config = AppConfig()


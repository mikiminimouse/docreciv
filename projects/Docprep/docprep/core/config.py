"""
Конфигурация путей и директорий для preprocessing системы.

Все пути настраиваются через переменные окружения.
Поддерживает структуру с датами согласно PRD.
"""
import os
from pathlib import Path
from typing import Dict, Optional, List

# Re-export MAX_CYCLES для обратной совместимости (источник: constants.py)
from .constants import MAX_CYCLES


# ============================================================================
# БАЗОВЫЕ ДИРЕКТОРИИ (настраиваются через env)
# ============================================================================

# Базовая директория Data (может быть переопределена через env)
# Используем абсолютный путь относительно проекта для избежания проблем с относительными путями
_default_data_dir = Path(__file__).parent.parent.parent / "Data"
DATA_BASE_DIR = Path(os.environ.get("DATA_BASE_DIR", str(_default_data_dir.resolve())))

# Базовые директории относительно DATA_BASE_DIR
INPUT_DIR = DATA_BASE_DIR / "Input"
PROCESSING_DIR = DATA_BASE_DIR / "Processing"
MERGE_DIR = DATA_BASE_DIR / "Merge"
READY2DOCLING_DIR = DATA_BASE_DIR / "Ready2Docling"
EXCEPTIONS_DIR = DATA_BASE_DIR / "Exceptions"  # Отдельная директория, аналогично Merge
ER_MERGE_DIR = DATA_BASE_DIR / "ErMerge"  # Директория для ошибок при финальном merge

# Поддерживаемые расширения для сортировки
EXTENSIONS_CONVERT = ["doc", "xls", "ppt", "rtf"]  # RTF требует конвертации через LibreOffice
EXTENSIONS_ARCHIVES = ["zip", "rar", "7z", "tar", "gz"]  # Добавлены tar, gz
EXTENSIONS_DIRECT = ["docx", "html", "jpeg", "jpg", "md", "pdf", "png", "pptx", "tiff", "tif", "bmp", "xlsx", "xml"]  # Добавлены html, md, tif, bmp; убран rtf
EXTENSIONS_NORMALIZE = EXTENSIONS_DIRECT  # Те же расширения для нормализации

# Поддиректории для Exceptions
EXCEPTION_SUBDIRS = {
    "Empty": "Empty",
    "Special": "Special",
    "Ambiguous": "Ambiguous",
    "Mixed": "Mixed",
    "ErConvert": "ErConvert",
    "ErExtract": "ErExtract",  # ИСПРАВЛЕНО: было ErExtact
    "ErNormalize": "ErNormalize",  # ИСПРАВЛЕНО: было ErNormalaze
    "NoProcessableFiles": "NoProcessableFiles",
}


def get_cycle_paths(cycle: int, processing_base: Optional[Path] = None, merge_base: Optional[Path] = None, exceptions_base: Optional[Path] = None) -> Dict[str, Path]:
    """
    Получает пути для указанного цикла обработки.

    НОВАЯ СТРУКТУРА (v2):
    - Merge/Direct/ - для файлов готовых к Docling сразу (без обработки)
    - Merge/Processed_N/ - для файлов обработанных в цикле N
    - Exceptions/Direct/ - для исключений до обработки через Processing
    - Exceptions/Processed_N/ - для исключений после обработки в цикле N

    Args:
        cycle: Номер цикла (1, 2, 3)
        processing_base: Базовая директория для Processing (по умолчанию PROCESSING_DIR)
        merge_base: Базовая директория для Merge (по умолчанию MERGE_DIR)
        exceptions_base: Базовая директория для Exceptions (по умолчанию EXCEPTIONS_DIR)

    Returns:
        Словарь с путями:
        - processing: Processing_N директория
        - merge: Processed_N директория (для обработанных units)
        - exceptions: Processed_N директория (для исключений после обработки)
        - merge_direct: Direct директория (только для цикла 1, для прямых файлов)
        - exceptions_direct: Direct директория (только для цикла 1, для исключений до обработки)
    """
    if cycle < 1 or cycle > MAX_CYCLES:
        raise ValueError(f"Cycle must be between 1 and {MAX_CYCLES}, got {cycle}")

    if processing_base is None:
        processing_base = PROCESSING_DIR
    if merge_base is None:
        merge_base = MERGE_DIR
    if exceptions_base is None:
        exceptions_base = EXCEPTIONS_DIR

    result = {
        "processing": processing_base / f"Processing_{cycle}",
        # ИСПРАВЛЕНО: Merge/Processed_N - для уже обработанных units
        "merge": merge_base / f"Processed_{cycle}",
        "exceptions": exceptions_base / f"Processing_{cycle}",
    }

    # Добавляем Direct директории (только для цикла 1)
    if cycle == 1:
        result["merge_direct"] = merge_base / "Direct"
        result["exceptions_direct"] = exceptions_base / "Direct"

    return result


def get_processing_paths(cycle: int, processing_base: Optional[Path] = None) -> Dict[str, Path]:
    """
    Получает пути поддиректорий для Processing_N цикла.

    Args:
        cycle: Номер цикла (1, 2, 3)
        processing_base: Базовая директория для Processing (по умолчанию PROCESSING_DIR)

    Returns:
        Словарь с путями:
        - Convert: директория для конвертации
        - Extract: директория для разархивации
        - Normalize: директория для нормализации
        Примечание: Direct НЕ возвращается, так как Direct файлы идут напрямую в Merge/Direct/
    """
    if processing_base is None:
        processing_base = PROCESSING_DIR
    
    processing_dir = processing_base / f"Processing_{cycle}"

    return {
        "Convert": processing_dir / "Convert",
        "Extract": processing_dir / "Extract",
        "Normalize": processing_dir / "Normalize",
        "Mixed": processing_dir / "Mixed",  # Добавлена Mixed директория
    }


def init_directory_structure(base_dir: Optional[Path] = None, date: Optional[str] = None) -> None:
    """
    Инициализирует базовую структуру директорий для обработки.

    ВАЖНО: Создаёт только базовые директории категорий.
    Поддиректории с расширениями (doc, docx, pdf и т.д.) создаются ON-DEMAND
    при перемещении первого unit в move_unit_to_target().

    Args:
        base_dir: Базовая директория (по умолчанию DATA_BASE_DIR)
        date: Дата в формате YYYY-MM-DD (опционально)
    """
    if base_dir is None:
        base_dir = DATA_BASE_DIR

    # Если указана дата, создаем структуру внутри Data/date/
    if date:
        date_dir = base_dir / date
        date_dir.mkdir(parents=True, exist_ok=True)

        input_base = date_dir / "Input"
        processing_base = date_dir / "Processing"
        merge_base = date_dir / "Merge"
        exceptions_base = date_dir / "Exceptions"
        ready_base = date_dir / "Ready2Docling"
    else:
        # ИСПРАВЛЕНО: Используем переданный base_dir вместо глобальных переменных
        # Глобальные переменные не учитывают переопределенный DATA_BASE_DIR через os.environ
        input_base = base_dir / "Input"
        processing_base = base_dir / "Processing"
        merge_base = base_dir / "Merge"
        exceptions_base = base_dir / "Exceptions"
        ready_base = base_dir / "Ready2Docling"

    # Создаем только базовые директории (без поддиректорий расширений)
    input_base.mkdir(parents=True, exist_ok=True)

    # Ready2Docling создается только базовая директория
    # Поддиректории расширений создаются on-demand при перемещении units
    ready_base.mkdir(parents=True, exist_ok=True)

    # =========================================================================
    # СТРУКТУРА v3 (on-demand для расширений):
    # - Базовые директории создаются здесь
    # - Поддиректории расширений создаются автоматически при перемещении units
    # =========================================================================

    # Merge/Direct/ - базовая директория (без поддиректорий расширений)
    (merge_base / "Direct").mkdir(parents=True, exist_ok=True)

    # Exceptions/Direct/ с категориями исключений
    exceptions_direct_dir = exceptions_base / "Direct"
    for subdir in ["Empty", "Special", "Ambiguous", "Mixed", "NoProcessableFiles"]:
        (exceptions_direct_dir / subdir).mkdir(parents=True, exist_ok=True)

    # Создаем директории для каждого цикла (только базовые, без расширений)
    for cycle in range(1, MAX_CYCLES + 1):
        # Processing/Processing_N - базовые категории
        processing_paths = get_processing_paths(cycle, processing_base)
        processing_paths["Convert"].mkdir(parents=True, exist_ok=True)
        processing_paths["Extract"].mkdir(parents=True, exist_ok=True)
        processing_paths["Normalize"].mkdir(parents=True, exist_ok=True)
        processing_paths["Mixed"].mkdir(parents=True, exist_ok=True)  # Добавлена Mixed

        # Exceptions/Processing_N с категориями исключений
        # ИСПРАВЛЕНО: Processed_ → Processing_ для согласованности со структурой директорий
        exceptions_processed_dir = exceptions_base / f"Processing_{cycle}"
        for subdir in ["Empty", "Special", "Ambiguous", "Mixed", "ErConvert", "ErNormalize", "ErExtract", "NoProcessableFiles"]:
            (exceptions_processed_dir / subdir).mkdir(parents=True, exist_ok=True)

        # Merge/Processed_N - базовые категории (без расширений)
        # ИСПРАВЛЕНО: Processed_N - для уже обработанных units
        # ПРИМЕЧАНИЕ: Direct НЕ создаётся в Processed_N, т.к. все Direct units идут в Merge/Direct/
        merge_processed_dir = merge_base / f"Processed_{cycle}"
        for category in ["Converted", "Extracted", "Normalized", "Mixed"]:
            (merge_processed_dir / category).mkdir(parents=True, exist_ok=True)


def get_data_paths(date: Optional[str] = None, base_dir: Optional[Path] = None) -> Dict[str, Path]:
    """
    Получает все базовые пути для работы с данными.

    Если указана дата, возвращает пути внутри Data/date/, иначе стандартные пути.

    Args:
        date: Дата в формате YYYY-MM-DD (опционально)
        base_dir: Базовая директория (опционально, переопределяет DATA_BASE_DIR)

    Returns:
        Словарь с путями:
        - input: Input директория
        - processing: Processing директория
        - merge: Merge директория
        - exceptions: Exceptions директория
        - er_merge: ErMerge директория
        - ready2docling: Ready2Docling директория
    """
    # Определяем рабочую базовую директорию
    work_base_dir = base_dir if base_dir is not None else DATA_BASE_DIR

    if date:
        # Если указана дата, все категории внутри base_dir/date/
        date_dir = work_base_dir / date
        return {
            "input": date_dir / "Input",
            "processing": date_dir / "Processing",
            "merge": date_dir / "Merge",
            "exceptions": date_dir / "Exceptions",
            "er_merge": date_dir / "ErMerge",
            "ready2docling": date_dir / "Ready2Docling",
        }
    else:
        # Без даты используем базовую директорию напрямую
        return {
            "input": work_base_dir / "Input",
            "processing": work_base_dir / "Processing",
            "merge": work_base_dir / "Merge",
            "exceptions": work_base_dir / "Exceptions",
            "er_merge": work_base_dir / "ErMerge",
            "ready2docling": work_base_dir / "Ready2Docling",
        }


# ============================================================================
# MongoDB конфигурация (единая для всех сервисов)
# ============================================================================

from dataclasses import dataclass
from typing import Optional


@dataclass
class MongoDBConfig:
    """
    Конфигурация MongoDB для DocPrep и связанных сервисов.

    Обеспечивает единообразие настроек MongoDB между:
    - Docreciv (создаёт записи в protocols)
    - DocPrep (обновляет записи протоколов)
    - doclingproc (читает результаты обработки)

    Атрибуты:
        host: Хост MongoDB (по умолчанию localhost)
        port: Порт MongoDB (по умолчанию 27018 — единый для Docreciv)
        database: Имя базы данных (docling_metadata для совместимости с Docreciv)
        protocols_collection: Имя коллекции протоколов (protocols)
        docprep_database: Имя базы DocPrep для собственных метрик
    """
    host: str = "localhost"
    port: int = 27018  # Единый порт для Docreciv/DocPrep
    database: str = "docling_metadata"
    protocols_collection: str = "protocols"
    docprep_database: str = "docprep"

    @property
    def connection_string(self) -> str:
        """Возвращает строку подключения MongoDB."""
        return f"mongodb://{self.host}:{self.port}"

    @property
    def docprep_connection_string(self) -> str:
        """Возвращает строку подключения для DocPrep базы."""
        return f"mongodb://{self.host}:{self.port}/{self.docprep_database}"

    @classmethod
    def from_env(cls) -> "MongoDBConfig":
        """
        Создаёт конфигурацию из переменных окружения.

        Переменные окружения:
            MONGODB_HOST: хост MongoDB (по умолчанию localhost)
            MONGODB_PORT: порт MongoDB (по умолчанию 27018)
            MONGODB_DATABASE: имя базы данных (по умолчанию docling_metadata)
            MONGODB_ENABLED: включить MongoDB (true/false)
        """
        return cls(
            host=os.getenv("MONGODB_HOST", "localhost"),
            port=int(os.getenv("MONGODB_PORT", "27018")),
            database=os.getenv("MONGODB_DATABASE", "docling_metadata"),
            docprep_database=os.getenv("MONGODB_DOCPREP_DB", "docprep"),
        )

    def to_env_dict(self) -> Dict[str, str]:
        """Возвращает словарь переменных окружения для экспорта."""
        return {
            "MONGODB_URI": self.connection_string,
            "MONGODB_HOST": self.host,
            "MONGODB_PORT": str(self.port),
            "MONGODB_DATABASE": self.database,
            "MONGODB_DOCPREP_DB": self.docprep_database,
        }


# Глобальный экземпляр конфигурации
_mongodb_config = MongoDBConfig.from_env()


def get_mongodb_config() -> MongoDBConfig:
    """
    Возвращает глобальную конфигурацию MongoDB.

    Returns:
        Экземпляр MongoDBConfig
    """
    return _mongodb_config


def set_mongodb_config(config: MongoDBConfig) -> None:
    """
    Устанавливает глобальную конфигурацию MongoDB.

    Args:
        config: Новая конфигурация MongoDB
    """
    global _mongodb_config
    _mongodb_config = config


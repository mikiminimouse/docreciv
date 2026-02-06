"""
Конфигурация для высокопроизводительного сервера.

Оптимизирована для 64-ядерного сервера с 16GB RAM.
Автоматически определяет ресурсы системы и выдаёт оптимальные настройки.
"""
import os
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ServerProfile:
    """Профиль конфигурации сервера."""

    # CPU
    cpu_count: int = 4
    cpu_model: str = "Unknown"

    # Memory
    total_memory_gb: float = 4.0
    available_memory_gb: float = 2.0

    # Disk
    disk_type: str = "HDD"  # HDD или SSD
    disk_available_gb: float = 10.0

    # Вычисленные лимиты
    classifier_workers: int = 4
    converter_workers: int = 2
    extractor_workers: int = 4
    normalizer_workers: int = 4
    unit_processor_workers: int = 4
    xvfb_max_displays: int = 2

    # I/O настройки
    manifest_flush_interval: int = 10
    manifest_flush_timeout: float = 5.0
    file_type_cache_size: int = 5000

    # Флаги
    parallel_enabled: bool = True


class ServerConfig:
    """
    Конфигурация сервера с автоопределением ресурсов.

    Использование:
        config = ServerConfig()
        profile = config.get_profile()
        print(f"Classifier workers: {profile.classifier_workers}")
    """

    # Лимиты памяти на worker (MB)
    MEMORY_PER_WORKER = {
        "classifier": 256,      # I/O bound, легкие операции
        "converter": 1024,      # LibreOffice тяжёлый
        "extractor": 512,       # Распаковка архивов
        "normalizer": 256,      # Переименование файлов
        "unit_processor": 512,  # Обработка UNIT
    }

    # Максимальные лимиты workers по типам
    # ОПТИМИЗАЦИЯ: Увеличены в 2x для обработки больших объёмов данных (70,000+ UNIT)
    MAX_WORKERS = {
        "classifier": 32,       # I/O bound - больше потоков (было 16)
        "converter": 12,        # CPU + Memory bound (было 6)
        "extractor": 16,        # I/O bound (было 8)
        "normalizer": 32,       # Легкие операции (было 16)
        "unit_processor": 24,   # Средняя нагрузка (было 12)
    }

    def __init__(self):
        """Инициализирует ServerConfig с автоопределением ресурсов."""
        self._profile: Optional[ServerProfile] = None

    def _detect_system_resources(self) -> Dict[str, Any]:
        """Определяет ресурсы системы."""
        resources = {
            "cpu_count": os.cpu_count() or 4,
            "cpu_model": "Unknown",
            "total_memory_gb": 4.0,
            "available_memory_gb": 2.0,
            "disk_type": "HDD",
            "disk_available_gb": 10.0,
        }

        # Определяем CPU модель (Linux)
        try:
            with open("/proc/cpuinfo", "r") as f:
                for line in f:
                    if "model name" in line:
                        resources["cpu_model"] = line.split(":")[1].strip()
                        break
        except (FileNotFoundError, PermissionError):
            pass

        # Определяем память через psutil
        try:
            import psutil

            mem = psutil.virtual_memory()
            resources["total_memory_gb"] = mem.total / (1024 ** 3)
            resources["available_memory_gb"] = mem.available / (1024 ** 3)

            # Определяем диск
            disk = psutil.disk_usage("/")
            resources["disk_available_gb"] = disk.free / (1024 ** 3)

            # Простая эвристика для типа диска (SSD vs HDD)
            # На Linux можно проверить /sys/block/*/queue/rotational
            try:
                import subprocess
                result = subprocess.run(
                    ["cat", "/sys/block/sda/queue/rotational"],
                    capture_output=True,
                    text=True,
                    timeout=1,
                )
                if result.returncode == 0:
                    resources["disk_type"] = "HDD" if result.stdout.strip() == "1" else "SSD"
            except Exception:
                pass

        except ImportError:
            logger.warning("psutil not available, using default resource estimates")

        return resources

    def _calculate_workers(
        self,
        operation_type: str,
        available_memory_gb: float,
        cpu_count: int,
    ) -> int:
        """
        Рассчитывает оптимальное количество workers.

        Args:
            operation_type: Тип операции
            available_memory_gb: Доступная память (GB)
            cpu_count: Количество CPU

        Returns:
            Оптимальное количество workers
        """
        memory_per_worker_mb = self.MEMORY_PER_WORKER.get(operation_type, 512)
        max_for_type = self.MAX_WORKERS.get(operation_type, 8)

        # Лимит по памяти (оставляем 20% резерва)
        usable_memory_mb = available_memory_gb * 1024 * 0.8
        memory_based_limit = int(usable_memory_mb / memory_per_worker_mb)

        # Лимит по CPU (не больше CPU для CPU-bound, 2x для I/O-bound)
        io_bound_types = {"classifier", "normalizer", "extractor"}
        if operation_type in io_bound_types:
            cpu_based_limit = cpu_count * 2
        else:
            cpu_based_limit = cpu_count

        # Итоговый лимит
        return max(1, min(memory_based_limit, cpu_based_limit, max_for_type))

    def get_profile(self, force_refresh: bool = False) -> ServerProfile:
        """
        Возвращает профиль конфигурации сервера.

        Args:
            force_refresh: Принудительно пересчитать профиль

        Returns:
            ServerProfile с оптимальными настройками
        """
        if self._profile is not None and not force_refresh:
            return self._profile

        resources = self._detect_system_resources()

        cpu_count = resources["cpu_count"]
        available_memory_gb = resources["available_memory_gb"]
        total_memory_gb = resources["total_memory_gb"]

        # Создаём профиль
        profile = ServerProfile(
            cpu_count=cpu_count,
            cpu_model=resources["cpu_model"],
            total_memory_gb=total_memory_gb,
            available_memory_gb=available_memory_gb,
            disk_type=resources["disk_type"],
            disk_available_gb=resources["disk_available_gb"],
        )

        # Рассчитываем workers для каждого типа операций
        profile.classifier_workers = self._calculate_workers(
            "classifier", available_memory_gb, cpu_count
        )
        profile.converter_workers = self._calculate_workers(
            "converter", available_memory_gb, cpu_count
        )
        profile.extractor_workers = self._calculate_workers(
            "extractor", available_memory_gb, cpu_count
        )
        profile.normalizer_workers = self._calculate_workers(
            "normalizer", available_memory_gb, cpu_count
        )
        profile.unit_processor_workers = self._calculate_workers(
            "unit_processor", available_memory_gb, cpu_count
        )

        # Xvfb displays (для LibreOffice)
        # 1 display на 1GB памяти, максимум 6
        profile.xvfb_max_displays = min(6, max(2, int(available_memory_gb / 1.5)))

        # I/O настройки (зависят от типа диска)
        if resources["disk_type"] == "SSD":
            profile.manifest_flush_interval = 20  # Больше буферизации для SSD
            profile.file_type_cache_size = 15000  # Больше кеш
        else:
            profile.manifest_flush_interval = 10  # Меньше для HDD
            profile.file_type_cache_size = 10000

        # Включаем параллелизм только если достаточно ресурсов
        profile.parallel_enabled = cpu_count >= 2 and available_memory_gb >= 2.0

        self._profile = profile

        logger.info(
            f"Server profile detected: "
            f"{cpu_count} CPUs, {total_memory_gb:.1f}GB RAM, "
            f"{resources['disk_type']}, {resources['disk_available_gb']:.1f}GB free"
        )
        logger.info(
            f"Optimal workers: classifier={profile.classifier_workers}, "
            f"converter={profile.converter_workers}, "
            f"extractor={profile.extractor_workers}, "
            f"unit_processor={profile.unit_processor_workers}"
        )

        return profile

    def get_optimal_config(self) -> Dict[str, Any]:
        """
        Возвращает оптимальную конфигурацию как словарь.

        Returns:
            Словарь с настройками
        """
        profile = self.get_profile()

        return {
            # Workers
            "classifier_workers": profile.classifier_workers,
            "converter_workers": profile.converter_workers,
            "extractor_workers": profile.extractor_workers,
            "normalizer_workers": profile.normalizer_workers,
            "unit_processor_workers": profile.unit_processor_workers,

            # Xvfb
            "xvfb_max_displays": profile.xvfb_max_displays,

            # I/O
            "manifest_flush_interval": profile.manifest_flush_interval,
            "manifest_flush_timeout": profile.manifest_flush_timeout,
            "file_type_cache_size": profile.file_type_cache_size,

            # Flags
            "parallel_enabled": profile.parallel_enabled,

            # Info
            "cpu_count": profile.cpu_count,
            "cpu_model": profile.cpu_model,
            "total_memory_gb": profile.total_memory_gb,
            "disk_type": profile.disk_type,
        }


# Глобальный синглтон
_server_config: Optional[ServerConfig] = None


def get_server_config() -> ServerConfig:
    """Возвращает глобальный экземпляр ServerConfig."""
    global _server_config
    if _server_config is None:
        _server_config = ServerConfig()
    return _server_config


def get_server_profile() -> ServerProfile:
    """Shortcut для получения профиля сервера."""
    return get_server_config().get_profile()


def print_server_info() -> None:
    """Выводит информацию о сервере и оптимальных настройках."""
    profile = get_server_profile()

    print("=" * 60)
    print("DocPrep Server Configuration")
    print("=" * 60)
    print(f"CPU: {profile.cpu_count} cores ({profile.cpu_model})")
    print(f"RAM: {profile.total_memory_gb:.1f} GB total, {profile.available_memory_gb:.1f} GB available")
    print(f"Disk: {profile.disk_type}, {profile.disk_available_gb:.1f} GB free")
    print("-" * 60)
    print("Optimal Worker Configuration:")
    print(f"  Classifier:      {profile.classifier_workers} workers")
    print(f"  Converter:       {profile.converter_workers} workers")
    print(f"  Extractor:       {profile.extractor_workers} workers")
    print(f"  Normalizer:      {profile.normalizer_workers} workers")
    print(f"  Unit Processor:  {profile.unit_processor_workers} workers")
    print(f"  Xvfb Displays:   {profile.xvfb_max_displays}")
    print("-" * 60)
    print("I/O Settings:")
    print(f"  Manifest Flush:  every {profile.manifest_flush_interval} operations")
    print(f"  File Type Cache: {profile.file_type_cache_size} entries")
    print(f"  Parallel Mode:   {'enabled' if profile.parallel_enabled else 'disabled'}")
    print("=" * 60)

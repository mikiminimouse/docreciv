"""
Скрипт для профилирования производительности скачивания.

Позволяет:
1. Измерить baseline характеристики системы (CPU, RAM, Disk, Network, VPN)
2. Мониторить ресурсы во время теста
3. Определить bottleneck системы
4. Сравнить разные уровни конкурентности

Использование:
    python -m docreciv.scripts.performance_profiler --date 2026-01-23 --concurrency 15
    python -m docreciv.scripts.performance_profiler --baseline-only
    python -m docreciv.scripts.performance_profiler --compare 6 10 14 15
"""

import argparse
import json
import os
import shutil
import sys
import time
import threading
import subprocess
import socket
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from pymongo import MongoClient

from docreciv.core.config import get_config
from docreciv.downloader.enhanced_service import EnhancedProtocolDownloader
from docreciv.downloader.models import DownloadRequest

# Временные директории для тестов
TEST_OUTPUT_BASE = Path("/tmp/perf_test")


@dataclass
class BaselineMetrics:
    """Baseline метрики системы (до теста)."""
    cpu_cores: int
    memory_total_mb: float
    memory_available_mb: float
    cpu_total_hz: Optional[float] = None
    disk_write_mb_per_sec: float = 0.0
    disk_read_mb_per_sec: float = 0.0
    vpn_latency_ms: float = 0.0
    vpn_connected: bool = False
    zakupki_accessible: bool = False
    timestamp: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PerformanceMetrics:
    """Метрики производительности во время теста."""
    # Основные показатели
    concurrency_level: int
    duration_seconds: float
    files_downloaded: int
    files_failed: int
    protocols_processed: int
    throughput_files_per_sec: float
    throughput_protocols_per_sec: float
    success_rate: float

    # Метрики CPU
    cpu_percent_avg: float = 0.0
    cpu_percent_max: float = 0.0
    cpu_per_core_avg: List[float] = field(default_factory=list)

    # Метрики памяти
    memory_mb_avg: float = 0.0
    memory_mb_max: float = 0.0
    memory_mb_start: float = 0.0
    memory_mb_end: float = 0.0

    # Метрики диска
    disk_write_mb_total: float = 0.0
    disk_write_mb_per_sec: float = 0.0
    disk_read_mb_total: float = 0.0
    disk_io_wait_percent: float = 0.0

    # Метрики сети
    network_download_mb: float = 0.0
    network_download_mbps: float = 0.0
    network_upload_mb: float = 0.0

    # VPN метрики
    vpn_latency_ms_avg: float = 0.0
    vpn_latency_ms_min: float = 0.0
    vpn_latency_ms_max: float = 0.0

    # Качество
    errors_count: int = 0
    timeout_count: int = 0
    connection_errors: int = 0
    error_types: Dict[str, int] = field(default_factory=dict)

    # Worker utilization
    worker_utilization_percent: float = 0.0
    active_threads_avg: float = 0.0
    sample_count: int = 0

    # Временные метки
    timestamp_start: str = ""
    timestamp_end: str = ""

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        # Convert lists to ensure JSON serializable
        if result.get("cpu_per_core_avg") is None:
            result["cpu_per_core_avg"] = []
        return result


class PerformanceMonitor:
    """
    Мониторинг системных ресурсов во время теста.

    Собирает метрики CPU, памяти, диска и сети в отдельном потоке.
    """

    def __init__(self, sample_interval: float = 0.5):
        """
        Инициализация монитора.

        Args:
            sample_interval: Интервал сбора метрик в секундах
        """
        self.sample_interval = sample_interval
        self.metrics: List[Dict[str, Any]] = []
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.start_disk_io = None
        self.start_network_io = None

        # Try to import psutil
        try:
            import psutil
            self.psutil = psutil
        except ImportError:
            self.psutil = None
            print("Warning: psutil not available, limited monitoring")

    def start(self) -> None:
        """Запуск сбора метрик в отдельном потоке."""
        if self.psutil is None:
            return

        self.running = True
        self.start_disk_io = self.psutil.disk_io_counters()
        self.start_network_io = self.psutil.net_io_counters()
        self.thread = threading.Thread(target=self._collect_loop, daemon=True)
        self.thread.start()

    def _collect_loop(self) -> None:
        """Постоянный сбор метрик в фоновом потоке."""
        while self.running:
            try:
                sample = {
                    "timestamp": time.time(),
                    "cpu_percent": self.psutil.cpu_percent(interval=0.1),
                    "cpu_per_core": self.psutil.cpu_percent(interval=0.1, percpu=True),
                    "memory_mb": self.psutil.virtual_memory().used / 1024 / 1024,
                    "memory_percent": self.psutil.virtual_memory().percent,
                }

                # Disk I/O
                disk_io = self.psutil.disk_io_counters()
                if disk_io:
                    sample["disk_read_bytes"] = disk_io.read_bytes
                    sample["disk_write_bytes"] = disk_io.write_bytes
                    sample["disk_read_count"] = disk_io.read_count
                    sample["disk_write_count"] = disk_io.write_count

                # Network I/O
                net_io = self.psutil.net_io_counters()
                if net_io:
                    sample["net_bytes_sent"] = net_io.bytes_sent
                    sample["net_bytes_recv"] = net_io.bytes_recv
                    sample["net_packets_sent"] = net_io.packets_sent
                    sample["net_packets_recv"] = net_io.packets_recv

                # Active threads in current process
                sample["active_threads"] = threading.active_count()

                self.metrics.append(sample)

            except Exception as e:
                print(f"Error collecting sample: {e}")

            time.sleep(self.sample_interval)

    def stop(self) -> PerformanceMetrics:
        """
        Остановка сбора и возврат агрегированных метрик.

        Args:
            concurrency_level: Уровень конкурентности теста
            test_results: Результаты скачивания

        Returns:
            PerformanceMetrics с агрегированными данными
        """
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)

        end_disk_io = None
        end_network_io = None

        if self.psutil:
            end_disk_io = self.psutil.disk_io_counters()
            end_network_io = self.psutil.net_io_counters()

        return self._aggregate(end_disk_io, end_network_io)

    def _aggregate(
        self,
        end_disk_io: Optional[Any] = None,
        end_network_io: Optional[Any] = None
    ) -> PerformanceMetrics:
        """Агрегация собранных метрик."""
        if not self.metrics or self.psutil is None:
            return PerformanceMetrics(
                concurrency_level=0,
                duration_seconds=0,
                files_downloaded=0,
                files_failed=0,
                protocols_processed=0,
                throughput_files_per_sec=0,
                throughput_protocols_per_sec=0,
                success_rate=0
            )

        cpu_values = [m["cpu_percent"] for m in self.metrics]
        mem_values = [m["memory_mb"] for m in self.metrics]
        thread_values = [m.get("active_threads", 0) for m in self.metrics]

        # CPU per core average
        if self.metrics and "cpu_per_core" in self.metrics[0]:
            num_cores = len(self.metrics[0]["cpu_per_core"])
            cpu_per_core_avg = []
            for i in range(num_cores):
                core_values = [m["cpu_per_core"][i] for m in self.metrics]
                cpu_per_core_avg.append(sum(core_values) / len(core_values))
        else:
            cpu_per_core_avg = []

        # Calculate disk and network totals
        disk_write_total = 0
        disk_read_total = 0
        network_download = 0
        network_upload = 0

        if end_disk_io and self.start_disk_io:
            disk_write_total = (end_disk_io.write_bytes - self.start_disk_io.write_bytes) / 1024 / 1024
            disk_read_total = (end_disk_io.read_bytes - self.start_disk_io.read_bytes) / 1024 / 1024

        if end_network_io and self.start_network_io:
            network_download = (end_network_io.bytes_recv - self.start_network_io.bytes_recv) / 1024 / 1024
            network_upload = (end_network_io.bytes_sent - self.start_network_io.bytes_sent) / 1024 / 1024

        return PerformanceMetrics(
            concurrency_level=0,  # Will be set by caller
            duration_seconds=0,   # Will be set by caller
            files_downloaded=0,   # Will be set by caller
            files_failed=0,       # Will be set by caller
            protocols_processed=0,  # Will be set by caller
            throughput_files_per_sec=0,
            throughput_protocols_per_sec=0,
            success_rate=0,
            cpu_percent_avg=sum(cpu_values) / len(cpu_values),
            cpu_percent_max=max(cpu_values),
            cpu_per_core_avg=cpu_per_core_avg,
            memory_mb_avg=sum(mem_values) / len(mem_values),
            memory_mb_max=max(mem_values),
            memory_mb_start=mem_values[0] if mem_values else 0,
            memory_mb_end=mem_values[-1] if mem_values else 0,
            disk_write_mb_total=disk_write_total,
            disk_read_mb_total=disk_read_total,
            network_download_mb=network_download,
            network_upload_mb=network_upload,
            active_threads_avg=sum(thread_values) / len(thread_values),
            sample_count=len(self.metrics)
        )


def check_vpn_latency() -> float:
    """
    Измерение задержки до zakupki.gov.ru.

    Returns:
        Задержка в миллисекундах или -1 при ошибке
    """
    start = time.time()
    try:
        sock = socket.create_connection(("zakupki.gov.ru", 443), timeout=10)
        sock.close()
        latency_ms = (time.time() - start) * 1000
        return latency_ms
    except Exception:
        return -1.0


def measure_disk_speed(path: Path, size_mb: int = 50) -> float:
    """
    Измерение скорости записи на диск.

    Args:
        path: Путь для тестового файла
        size_mb: Размер тестового файла в МБ

    Returns:
        Скорость записи в МБ/с
    """
    test_file = path / f".speed_test_{int(time.time())}.tmp"
    data = b"0" * (1024 * 1024)  # 1 MB chunk

    try:
        start = time.time()
        with open(test_file, "wb") as f:
            for _ in range(size_mb):
                f.write(data)
        duration = time.time() - start

        test_file.unlink()
        return size_mb / duration if duration > 0 else 0
    except Exception as e:
        print(f"Disk speed test error: {e}")
        return 0.0


def collect_baseline() -> BaselineMetrics:
    """
    Сбор baseline метрик системы.

    Returns:
        BaselineMetrics с характеристиками системы
    """
    print("\n" + "=" * 60)
    print("COLLECTING BASELINE SYSTEM METRICS")
    print("=" * 60)

    # CPU cores
    cpu_cores = os.cpu_count() or 1

    # Memory
    try:
        import psutil
        memory = psutil.virtual_memory()
        memory_total_mb = memory.total / 1024 / 1024
        memory_available_mb = memory.available / 1024 / 1024
    except ImportError:
        memory_total_mb = 0
        memory_available_mb = 0
        print("Warning: psutil not available for memory info")

    # Disk speed
    print("\nMeasuring disk write speed...")
    test_path = Path("/tmp")
    disk_speed = measure_disk_speed(test_path, size_mb=50)
    print(f"  Disk write speed: {disk_speed:.2f} MB/s")

    # VPN latency
    print("\nMeasuring VPN latency to zakupki.gov.ru...")
    vpn_latency = check_vpn_latency()
    vpn_connected = vpn_latency > 0
    print(f"  VPN latency: {vpn_latency:.0f} ms" if vpn_latency > 0 else "  VPN: Not accessible")

    # Check zakupki.gov.ru
    from docreciv.downloader.utils import check_zakupki_health
    zakupki_ok = check_zakupki_health()
    print(f"  zakupki.gov.ru: {'Accessible' if zakupki_ok else 'Not accessible'}")

    baseline = BaselineMetrics(
        cpu_cores=cpu_cores,
        memory_total_mb=memory_total_mb,
        memory_available_mb=memory_available_mb,
        disk_write_mb_per_sec=disk_speed,
        vpn_latency_ms=vpn_latency,
        vpn_connected=vpn_connected,
        zakupki_accessible=zakupki_ok,
        timestamp=datetime.now().isoformat()
    )

    print("\n" + "-" * 60)
    print("BASELINE SUMMARY:")
    print(f"  CPU cores: {baseline.cpu_cores}")
    print(f"  RAM total: {baseline.memory_total_mb:.0f} MB")
    print(f"  RAM available: {baseline.memory_available_mb:.0f} MB")
    print(f"  Disk write: {baseline.disk_write_mb_per_sec:.2f} MB/s")
    print(f"  VPN latency: {baseline.vpn_latency_ms:.0f} ms")
    print("=" * 60)

    return baseline


def get_mongo_client() -> MongoClient:
    """Получает клиент MongoDB из конфигурации."""
    config = get_config()
    local_config = config.sync_db.local_mongo

    connection_url = (
        f"mongodb://{local_config.user}:{local_config.password}"
        f"@{local_config.server}"
    )

    return MongoClient(
        connection_url,
        serverSelectionTimeoutMS=20000,
        connectTimeoutMS=10000
    )


def get_sample_protocols(
    target_date: datetime,
    sample_size: int = 50
) -> List[Dict[str, Any]]:
    """
    Получает выборку протоколов для тестирования.

    Args:
        target_date: Целевая дата
        sample_size: Размер выборки

    Returns:
        Список протоколов
    """
    client = get_mongo_client()
    config = get_config()
    db = client[config.sync_db.local_mongo.db]
    collection = db[config.sync_db.local_mongo.collection]

    start_dt = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)
    end_dt = datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59)

    query = {
        "loadDate": {
            "$gte": start_dt,
            "$lte": end_dt
        },
        "urls.0": {"$exists": True},
        "url_count": {"$gt": 0}
    }

    protocols = list(collection.find(query).limit(sample_size))
    client.close()

    return protocols


def run_performance_test(
    target_date: datetime,
    concurrency: int,
    sample_size: int = 50
) -> PerformanceMetrics:
    """
    Запуск теста производительности с мониторингом.

    Args:
        target_date: Целевая дата
        concurrency: Уровень конкурентности
        sample_size: Размер выборки протоколов

    Returns:
        PerformanceMetrics с результатами теста
    """
    print("\n" + "=" * 80)
    print(f"PERFORMANCE TEST: concurrency={concurrency}, date={target_date.date()}")
    print("=" * 80)

    # Подготавливаем директорию
    output_dir = TEST_OUTPUT_BASE / f"concurrency_{concurrency}"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Получаем протоколы
    protocols = get_sample_protocols(target_date, sample_size)
    if not protocols:
        print("No protocols found for testing")
        return PerformanceMetrics(
            concurrency_level=concurrency,
            duration_seconds=0,
            files_downloaded=0,
            files_failed=0,
            protocols_processed=0,
            throughput_files_per_sec=0,
            throughput_protocols_per_sec=0,
            success_rate=0
        )

    print(f"\nTesting with {len(protocols)} protocols")
    print(f"Output directory: {output_dir}")

    # Устанавливаем конкурентность
    os.environ["DOWNLOAD_CONCURRENCY"] = str(concurrency)
    os.environ["PROTOCOLS_CONCURRENCY"] = str(concurrency)

    # Создаём downloader и монитор
    downloader = EnhancedProtocolDownloader(output_dir=output_dir)
    monitor = PerformanceMonitor(sample_interval=0.5)

    # Формируем список record_id
    record_ids = [str(p.get("_id", "")) for p in protocols if p.get("_id")]

    # Запускаем мониторинг
    monitor.start()

    # Запускаем скачивание
    start_time = time.time()
    timestamp_start = datetime.now().isoformat()

    try:
        result = downloader.process_download_request(
            DownloadRequest(
                from_date=target_date,
                to_date=target_date,
                record_ids=record_ids,
                skip_existing=False,
                force_reload=True,
                dry_run=False
            )
        )
    except Exception as e:
        print(f"Error during download: {e}")
        monitor.stop()
        return PerformanceMetrics(
            concurrency_level=concurrency,
            duration_seconds=0,
            files_downloaded=0,
            files_failed=0,
            protocols_processed=0,
            throughput_files_per_sec=0,
            throughput_protocols_per_sec=0,
            success_rate=0,
            timestamp_start=timestamp_start,
            timestamp_end=datetime.now().isoformat()
        )

    end_time = time.time()
    timestamp_end = datetime.now().isoformat()
    duration = end_time - start_time

    # Останавливаем мониторинг и получаем метрики
    perf_metrics = monitor.stop()

    # Обновляем метрики результатами теста
    stats = result.statistics if hasattr(result, "statistics") else {}
    errors = result.errors if hasattr(result, "errors") else []

    # Считаем файлы
    files_downloaded = 0
    for pdf_file in output_dir.glob("**/*.pdf"):
        files_downloaded += 1

    files_failed = stats.get("files_failed", 0) or result.failed

    # Детализация ошибок
    error_types: Dict[str, int] = {}
    timeout_count = 0
    connection_errors = 0

    for error in errors:
        error_str = str(error).lower()
        if "timeout" in error_str:
            timeout_count += 1
            error_types["timeout"] = error_types.get("timeout", 0) + 1
        elif "connection" in error_str:
            connection_errors += 1
            error_types["connection"] = error_types.get("connection", 0) + 1
        else:
            error_types["other"] = error_types.get("other", 0) + 1

    # Добавляем error_types из statistics
    if "error_types" in stats:
        for err_type, count in stats["error_types"].items():
            error_types[err_type] = error_types.get(err_type, 0) + count

    total_files = files_downloaded + files_failed
    success_rate = (files_downloaded / total_files * 100) if total_files > 0 else 0

    # Обновляем perf_metrics
    perf_metrics.concurrency_level = concurrency
    perf_metrics.duration_seconds = duration
    perf_metrics.files_downloaded = files_downloaded
    perf_metrics.files_failed = files_failed
    perf_metrics.protocols_processed = len(record_ids)
    perf_metrics.throughput_files_per_sec = files_downloaded / duration if duration > 0 else 0
    perf_metrics.throughput_protocols_per_sec = len(record_ids) / duration if duration > 0 else 0
    perf_metrics.success_rate = success_rate
    perf_metrics.errors_count = len(errors)
    perf_metrics.timeout_count = timeout_count
    perf_metrics.connection_errors = connection_errors
    perf_metrics.error_types = error_types

    # Дополнительные вычисления
    if duration > 0:
        perf_metrics.disk_write_mb_per_sec = perf_metrics.disk_write_mb_total / duration
        perf_metrics.network_download_mbps = (perf_metrics.network_download_mb * 8) / duration

    perf_metrics.timestamp_start = timestamp_start
    perf_metrics.timestamp_end = timestamp_end

    # Вывод результатов
    print(f"\n{'─' * 80}")
    print(f"RESULTS FOR CONCURRENCY {concurrency}:")
    print(f"{'─' * 80}")
    print(f"  Duration: {duration:.2f}s")
    print(f"  Protocols processed: {perf_metrics.protocols_processed}")
    print(f"  Files downloaded: {files_downloaded}")
    print(f"  Files failed: {files_failed}")
    print(f"  Success rate: {success_rate:.1f}%")
    print(f"  Throughput (files): {perf_metrics.throughput_files_per_sec:.2f} files/s")
    print(f"  Throughput (protocols): {perf_metrics.throughput_protocols_per_sec:.2f} protocols/s")
    print(f"\n  CPU utilization:")
    print(f"    Average: {perf_metrics.cpu_percent_avg:.1f}%")
    print(f"    Maximum: {perf_metrics.cpu_percent_max:.1f}%")
    print(f"  Memory:")
    print(f"    Start: {perf_metrics.memory_mb_start:.0f} MB")
    print(f"    End: {perf_metrics.memory_mb_end:.0f} MB")
    print(f"    Peak: {perf_metrics.memory_mb_max:.0f} MB")
    print(f"  Disk I/O:")
    print(f"    Written: {perf_metrics.disk_write_mb_total:.1f} MB")
    print(f"    Write speed: {perf_metrics.disk_write_mb_per_sec:.2f} MB/s")
    print(f"  Network:")
    print(f"    Downloaded: {perf_metrics.network_download_mb:.1f} MB")
    print(f"    Speed: {perf_metrics.network_download_mbps:.2f} Mbps")
    print(f"  Active threads (avg): {perf_metrics.active_threads_avg:.1f}")
    print(f"{'─' * 80}")

    if error_types:
        print(f"\n  Errors:")
        for err_type, count in error_types.items():
            print(f"    {err_type}: {count}")

    return perf_metrics


def analyze_bottleneck(metrics: PerformanceMetrics, baseline: BaselineMetrics) -> str:
    """
    Анализ bottleneck по метрикам.

    Args:
        metrics: Метрики производительности
        baseline: Baseline метрики системы

    Returns:
        Тип bottleneck и объяснение
    """
    lines = []
    lines.append("\n" + "=" * 80)
    lines.append("BOTTLENECK ANALYSIS")
    lines.append("=" * 80)

    cpu_util = metrics.cpu_percent_avg
    cpu_cores = baseline.cpu_cores
    total_cpu_capacity = cpu_util * cpu_cores
    cpu_efficiency = (cpu_util / cpu_cores) * 100

    lines.append(f"\n1. CPU ANALYSIS:")
    lines.append(f"   CPU utilization (avg): {cpu_util:.1f}%")
    lines.append(f"   CPU cores: {cpu_cores}")
    lines.append(f"   Per-core efficiency: {cpu_efficiency:.1f}%")

    if cpu_util > 80:
        lines.append(f"   ⚠️  CPU BOTTLENECK: CPU utilization > 80%")
    elif cpu_efficiency > 70:
        lines.append(f"   ⚠️  POTENTIAL GIL BOTTLENECK: High per-core usage with multiple cores")
        lines.append(f"      Consider using multiprocessing instead of threading")
    else:
        lines.append(f"   ✅ CPU: Not a bottleneck")

    lines.append(f"\n2. DISK I/O ANALYSIS:")
    write_speed = metrics.disk_write_mb_per_sec
    baseline_speed = baseline.disk_write_mb_per_sec

    if baseline_speed > 0:
        disk_util = (write_speed / baseline_speed) * 100
        lines.append(f"   Write speed during test: {write_speed:.2f} MB/s")
        lines.append(f"   Baseline disk speed: {baseline_speed:.2f} MB/s")
        lines.append(f"   Disk utilization: {disk_util:.1f}%")

        if disk_util > 80:
            lines.append(f"   ⚠️  DISK I/O BOTTLENECK: Near maximum disk speed")
        else:
            lines.append(f"   ✅ Disk I/O: Not a bottleneck")
    else:
        lines.append(f"   ℹ️  Disk speed: Unknown (baseline not measured)")

    lines.append(f"\n3. NETWORK/VPN ANALYSIS:")
    network_mbps = metrics.network_download_mbps
    vpn_latency = baseline.vpn_latency_ms

    lines.append(f"   Network throughput: {network_mbps:.2f} Mbps")
    lines.append(f"   VPN latency: {vpn_latency:.0f} ms")

    if network_mbps < 10:
        lines.append(f"   ⚠️  NETWORK BOTTLENECK: Very low throughput (< 10 Mbps)")
    elif network_mbps < 20:
        lines.append(f"   ⚠️  POTENTIAL NETWORK BOTTLENECK: Low throughput (< 20 Mbps)")
    else:
        lines.append(f"   ✅ Network: Not a bottleneck")

    if vpn_latency > 500:
        lines.append(f"   ⚠️  HIGH VPN LATENCY: {vpn_latency:.0f} ms (> 500ms)")

    lines.append(f"\n4. MEMORY ANALYSIS:")
    memory_peak = metrics.memory_mb_max
    memory_total = baseline.memory_total_mb
    memory_util = (memory_peak / memory_total) * 100 if memory_total > 0 else 0

    lines.append(f"   Peak memory: {memory_peak:.0f} MB")
    lines.append(f"   Total memory: {memory_total:.0f} MB")
    lines.append(f"   Memory utilization: {memory_util:.1f}%")

    if memory_util > 80:
        lines.append(f"   ⚠️  MEMORY PRESSURE: Using > 80% of available RAM")
    else:
        lines.append(f"   ✅ Memory: Not a bottleneck")

    lines.append(f"\n5. WORKER UTILIZATION:")
    active_threads = metrics.active_threads_avg
    expected_threads = metrics.concurrency_level + 5  # Expected threads

    lines.append(f"   Active threads (avg): {active_threads:.1f}")
    lines.append(f"   Expected: ~{expected_threads}")

    if active_threads < expected_threads * 0.7:
        lines.append(f"   ⚠️  LOW THREAD UTILIZATION: Workers may be waiting on I/O")
    else:
        lines.append(f"   ✅ Thread utilization: Good")

    # Overall conclusion
    lines.append(f"\n" + "=" * 80)
    lines.append("CONCLUSION:")
    lines.append("=" * 80)

    bottlenecks = []

    if cpu_util > 80:
        bottlenecks.append("CPU")
    if disk_util > 80 if baseline_speed > 0 else False:
        bottlenecks.append("Disk I/O")
    if network_mbps < 10:
        bottlenecks.append("Network")
    if memory_util > 80:
        bottlenecks.append("Memory")
    if cpu_efficiency > 70 and cpu_cores > 2:
        bottlenecks.append("GIL (Python threading limitation)")

    if bottlenecks:
        lines.append(f"\n🔴 PRIMARY BOTTLENECK: {bottlenecks[0]}")
        if len(bottlenecks) > 1:
            lines.append(f"Secondary: {', '.join(bottlenecks[1:])}")
    else:
        lines.append(f"\n✅ NO CLEAR BOTTLENECK DETECTED")
        lines.append(f"   The system may be limited by external factors (server response time)")

    return "\n".join(lines)


def generate_comparison_report(
    results: List[PerformanceMetrics],
    baseline: BaselineMetrics
) -> str:
    """
    Генерация сравнительного отчёта.

    Args:
        results: Список результатов тестов
        baseline: Baseline метрики

    Returns:
        Текст отчёта
    """
    lines = []
    lines.append("\n" + "=" * 100)
    lines.append("PERFORMANCE COMPARISON REPORT")
    lines.append("=" * 100)

    # Таблица результатов
    lines.append(f"\n{'Concurrency':<12} {'Files':<8} {'Failed':<8} "
                f"{'Duration':<10} {'Files/s':<10} {'CPU%':<8} {'RAM MB':<10} "
                f"{'Disk MB/s':<12} {'Net Mbps':<10} {'Success%':<10}")
    lines.append("-" * 100)

    for m in results:
        lines.append(
            f"{m.concurrency_level:<12} "
            f"{m.files_downloaded:<8} "
            f"{m.files_failed:<8} "
            f"{m.duration_seconds:<10.2f} "
            f"{m.throughput_files_per_sec:<10.2f} "
            f"{m.cpu_percent_avg:<8.1f} "
            f"{m.memory_mb_avg:<10.0f} "
            f"{m.disk_write_mb_per_sec:<12.2f} "
            f"{m.network_download_mbps:<10.2f} "
            f"{m.success_rate:<10.1f}"
        )

    lines.append("=" * 100)

    # Анализ зависимости throughput от конкурентности
    lines.append("\nTHROUGHPUT VS CONCURRENCY:")
    lines.append("-" * 40)

    if len(results) >= 2:
        for m in results:
            lines.append(f"  {m.concurrency_level} threads: {m.throughput_files_per_sec:.2f} files/s")

        # Проверяем plateau
        max_throughput = max(m.throughput_files_per_sec for m in results)
        best = next(m for m in results if m.throughput_files_per_sec == max_throughput)

        lines.append(f"\n  Best throughput: {best.concurrency_level} threads ({max_throughput:.2f} files/s)")

        # Проверяем растёт ли throughput с конкурентностью
        if len(results) >= 3:
            first_half = results[:len(results)//2]
            second_half = results[len(results)//2:]

            avg_first = sum(m.throughput_files_per_sec for m in first_half) / len(first_half)
            avg_second = sum(m.throughput_files_per_sec for m in second_half) / len(second_half)

            if avg_second <= avg_first * 1.05:  # Менее 5% улучшения
                lines.append(f"\n  ⚠️  THROUGHPUT PLATEAU DETECTED")
                lines.append(f"      Increasing concurrency beyond {results[len(results)//2 - 1].concurrency_level} "
                           f"shows minimal improvement")
            else:
                lines.append(f"\n  ✅ Throughput improves with concurrency")

    # System baseline summary
    lines.append("\n" + "=" * 100)
    lines.append("SYSTEM BASELINE:")
    lines.append("-" * 40)
    lines.append(f"  CPU cores: {baseline.cpu_cores}")
    lines.append(f"  RAM total: {baseline.memory_total_mb:.0f} MB")
    lines.append(f"  RAM available: {baseline.memory_available_mb:.0f} MB")
    lines.append(f"  Disk write speed: {baseline.disk_write_mb_per_sec:.2f} MB/s")
    lines.append(f"  VPN latency: {baseline.vpn_latency_ms:.0f} ms")

    return "\n".join(lines)


def main():
    """Главная функция для CLI."""
    parser = argparse.ArgumentParser(
        description="Profile download performance and identify bottlenecks"
    )

    # Основные аргументы
    parser.add_argument(
        "--date",
        type=str,
        help="Target date for testing (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=15,
        help="Concurrency level to test (default: 15)"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=50,
        help="Number of protocols to test (default: 50)"
    )

    # Специальные режимы
    parser.add_argument(
        "--baseline-only",
        action="store_true",
        help="Only collect baseline metrics, don't run tests"
    )
    parser.add_argument(
        "--compare",
        nargs="+",
        type=int,
        metavar="LEVEL",
        help="Compare multiple concurrency levels (e.g., --compare 6 10 14 15)"
    )

    # Экспорт
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Export results to JSON file"
    )

    args = parser.parse_args()

    # Шаг 1: Сбор baseline метрик
    baseline = collect_baseline()

    if args.baseline_only:
        if args.output:
            with open(args.output, "w") as f:
                json.dump(baseline.to_dict(), f, indent=2)
            print(f"\nBaseline exported to: {args.output}")
        return 0

    # Проверяем дату
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            return 1
    elif not args.compare:
        # Если не указана дата и не режим сравнения, используем сегодня
        target_date = datetime.now()
        print(f"No date specified, using today: {target_date.date()}")

    # Шаг 2: Запуск тестов
    results: List[PerformanceMetrics] = []

    if args.compare:
        # Режим сравнения нескольких уровней
        for level in args.compare:
            if not target_date:
                print("--date is required for comparison tests")
                return 1

            metrics = run_performance_test(
                target_date=target_date,
                concurrency=level,
                sample_size=args.sample_size
            )
            results.append(metrics)

    else:
        # Одиночный тест
        if not target_date:
            print("--date is required for performance test")
            return 1

        metrics = run_performance_test(
            target_date=target_date,
            concurrency=args.concurrency,
            sample_size=args.sample_size
        )
        results.append(metrics)

        # Анализ bottleneck для одиночного теста
        bottleneck_analysis = analyze_bottleneck(metrics, baseline)
        print(bottleneck_analysis)

    # Шаг 3: Сравнительный отчёт
    if len(results) > 1:
        comparison = generate_comparison_report(results, baseline)
        print(comparison)

    # Шаг 4: Экспорт результатов
    if args.output:
        output_data = {
            "baseline": baseline.to_dict(),
            "tests": [m.to_dict() for m in results],
            "generated_at": datetime.now().isoformat()
        }

        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults exported to: {args.output}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

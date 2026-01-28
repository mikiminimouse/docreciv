"""
Скрипт для сравнения производительности sync vs async downloader.

Запускает идентичные тесты для ThreadPoolExecutor и asyncio реализаций,
сравнивая throughput, использование ресурсов и качество.

Использование:
    python -m docreciv.scripts.async_benchmark --date 2026-01-23 --sample-size 50
    python -m docreciv.scripts.async_benchmark --compare 10 50 100
"""

import argparse
import asyncio
import json
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from pymongo import MongoClient

from docreciv.core.config import get_config
from docreciv.downloader.enhanced_service import EnhancedProtocolDownloader
from docreciv.downloader.async_service import AsyncioProtocolDownloader
from docreciv.downloader.models import DownloadRequest

# Временные директории для тестов
TEST_OUTPUT_BASE = Path("/tmp/async_benchmark")


def get_mongo_client() -> MongoClient:
    """Получает клиент MongoDB."""
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


def get_sample_protocols(target_date: datetime, sample_size: int = 50) -> List[Dict]:
    """Получает выборку протоколов для тестирования."""
    client = get_mongo_client()
    config = get_config()
    db = client[config.sync_db.local_mongo.db]
    collection = db[config.sync_db.local_mongo.collection]

    start_dt = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)
    end_dt = datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59)

    query = {
        "loadDate": {"$gte": start_dt, "$lte": end_dt},
        "urls.0": {"$exists": True},
        "url_count": {"$gt": 0}
    }

    protocols = list(collection.find(query).limit(sample_size))
    client.close()
    return protocols


def run_sync_test(
    protocols: List[Dict],
    concurrency: int,
    output_dir: Path
) -> Dict[str, Any]:
    """Запуск синхронного теста с ThreadPoolExecutor."""
    print(f"\n{'='*60}")
    print(f"SYNC TEST: concurrency={concurrency}")
    print(f"{'='*60}")

    # Очищаем директорию
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Устанавливаем конкурентность
    import os
    os.environ["DOWNLOAD_CONCURRENCY"] = str(concurrency)
    os.environ["PROTOCOLS_CONCURRENCY"] = str(concurrency)

    # Создаём downloader
    downloader = EnhancedProtocolDownloader(output_dir=output_dir)

    # Формируем запрос
    record_ids = [str(p.get("_id", "")) for p in protocols if p.get("_id")]

    request = DownloadRequest(
        from_date=protocols[0].get("loadDate") if protocols else datetime.now(),
        to_date=protocols[0].get("loadDate") if protocols else datetime.now(),
        record_ids=record_ids,
        skip_existing=False,
        force_reload=True
    )

    # Запускаем
    start_time = time.time()
    result = downloader.process_download_request(request)
    duration = time.time() - start_time

    # Считаем файлы
    files_downloaded = 0
    for pdf_file in output_dir.glob("**/*.pdf"):
        files_downloaded += 1

    return {
        "type": "sync",
        "concurrency": concurrency,
        "duration_seconds": duration,
        "protocols_processed": len(record_ids),
        "files_downloaded": files_downloaded,
        "files_failed": result.failed,
        "throughput_files_per_sec": files_downloaded / duration if duration > 0 else 0,
        "success_rate": (files_downloaded / (files_downloaded + result.failed) * 100) if (files_downloaded + result.failed) > 0 else 0,
    }


async def run_async_test(
    protocols: List[Dict],
    concurrency: int,
    output_dir: Path
) -> Dict[str, Any]:
    """Запуск асинхронного теста с aiohttp."""
    print(f"\n{'='*60}")
    print(f"ASYNC TEST: concurrency={concurrency}")
    print(f"{'='*60}")

    # Очищаем директорию
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Устанавливаем конкурентность через переменные окружения
    import os
    os.environ["ASYNC_MAX_CONCURRENT_REQUESTS"] = str(concurrency)
    os.environ["ASYNC_MAX_CONCURRENT_PROTOCOLS"] = str(max(10, concurrency // 2))

    # Создаём downloader
    async with AsyncioProtocolDownloader(output_dir=output_dir) as downloader:

        # Формируем запрос
        record_ids = [str(p.get("_id", "")) for p in protocols if p.get("_id")]

        request = DownloadRequest(
            from_date=protocols[0].get("loadDate") if protocols else datetime.now(),
            to_date=protocols[0].get("loadDate") if protocols else datetime.now(),
            record_ids=record_ids,
            skip_existing=False,
            force_reload=True
        )

        # Запускаем
        start_time = time.time()
        result = await downloader.process_download_request(request)
        duration = time.time() - start_time

        # Считаем файлы
        files_downloaded = 0
        for pdf_file in output_dir.glob("**/*.pdf"):
            files_downloaded += 1

        return {
            "type": "async",
            "concurrency": concurrency,
            "duration_seconds": duration,
            "protocols_processed": len(record_ids),
            "files_downloaded": files_downloaded,
            "files_failed": result.failed,
            "throughput_files_per_sec": files_downloaded / duration if duration > 0 else 0,
            "success_rate": (files_downloaded / (files_downloaded + result.failed) * 100) if (files_downloaded + result.failed) > 0 else 0,
        }


def print_comparison_report(results: List[Dict[str, Any]]):
    """Выводит сравнительный отчёт."""
    print("\n" + "=" * 100)
    print("ASYNC BENCHMARK RESULTS")
    print("=" * 100)

    # Таблица результатов
    print(f"\n{'Type':<8} {'Concurrency':<12} {'Files':<8} {'Failed':<8} "
          f"{'Duration (s)':<15} {'Files/s':<12} {'Success %':<12}")
    print("-" * 100)

    for r in results:
        print(
            f"{r['type']:<8} "
            f"{r['concurrency']:<12} "
            f"{r['files_downloaded']:<8} "
            f"{r['files_failed']:<8} "
            f"{r['duration_seconds']:<15.2f} "
            f"{r['throughput_files_per_sec']:<12.2f} "
            f"{r['success_rate']:<12.1f}"
        )

    print("=" * 100)

    # Анализ улучшений
    sync_results = [r for r in results if r['type'] == 'sync']
    async_results = [r for r in results if r['type'] == 'async']

    if sync_results and async_results:
        print("\nIMPROVEMENT ANALYSIS:")
        print("-" * 40)

        for sync_r in sync_results:
            # Находим соответствующий async результат
            async_r = next((a for a in async_results if a['concurrency'] == sync_r['concurrency']), None)
            if async_r:
                speedup = sync_r['duration_seconds'] / async_r['duration_seconds'] if async_r['duration_seconds'] > 0 else 0
                throughput_improvement = (async_r['throughput_files_per_sec'] / sync_r['throughput_files_per_sec'] - 1) * 100 if sync_r['throughput_files_per_sec'] > 0 else 0

                print(f"  Concurrency {sync_r['concurrency']}:")
                print(f"    Speedup: {speedup:.2f}x")
                print(f"    Throughput improvement: {throughput_improvement:+.1f}%")
                print(f"    Sync: {sync_r['throughput_files_per_sec']:.2f} files/s → Async: {async_r['throughput_files_per_sec']:.2f} files/s")

        # Лучший результат
        best_sync = max(sync_results, key=lambda x: x['throughput_files_per_sec'])
        best_async = max(async_results, key=lambda x: x['throughput_files_per_sec'])

        print(f"\n  Best sync throughput: {best_sync['throughput_files_per_sec']:.2f} files/s (concurrency={best_sync['concurrency']})")
        print(f"  Best async throughput: {best_async['throughput_files_per_sec']:.2f} files/s (concurrency={best_async['concurrency']})")

        total_speedup = best_async['throughput_files_per_sec'] / best_sync['throughput_files_per_sec'] if best_sync['throughput_files_per_sec'] > 0 else 0
        print(f"  Total speedup: {total_speedup:.2f}x")


async def run_benchmark_async(
    target_date: datetime,
    concurrencies: List[int],
    sample_size: int = 50,
    output: Optional[str] = None
):
    """Асинхронная функция запуска бенчмарка."""
    # Получаем протоколы
    protocols = get_sample_protocols(target_date, sample_size)
    if not protocols:
        print("No protocols found for testing")
        return

    print(f"Testing with {len(protocols)} protocols")
    print(f"Concurrency levels: {concurrencies}")

    results = []

    for concurrency in concurrencies:
        # Sync тест
        sync_result = run_sync_test(
            protocols,
            concurrency,
            TEST_OUTPUT_BASE / f"sync_{concurrency}"
        )
        results.append(sync_result)

        # Async тест
        async_result = await run_async_test(
            protocols,
            concurrency,
            TEST_OUTPUT_BASE / f"async_{concurrency}"
        )
        results.append(async_result)

    # Выводим отчёт
    print_comparison_report(results)

    # Экспорт в JSON
    if output:
        output_data = {
            "benchmark_date": datetime.now().isoformat(),
            "sample_size": len(protocols),
            "target_date": target_date.isoformat(),
            "results": results
        }
        with open(output, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults exported to: {output}")


def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(description="Benchmark sync vs async downloader")
    parser.add_argument("--date", type=str, required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--compare", nargs="+", type=int, default=[15, 50, 100],
                        help="Concurrency levels to test (default: 15 50 100)")
    parser.add_argument("--sample-size", type=int, default=50, help="Sample size (default: 50)")
    parser.add_argument("--output", "-o", type=str, help="Export results to JSON file")

    args = parser.parse_args()

    try:
        target_date = datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"Invalid date format: {args.date}")
        sys.exit(1)

    # Запускаем бенчмарк
    asyncio.run(run_benchmark_async(
        target_date=target_date,
        concurrencies=args.compare,
        sample_size=args.sample_size,
        output=args.output
    ))


if __name__ == "__main__":
    main()

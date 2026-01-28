"""
Скрипт для тестирования разных уровней конкурентности при скачивании.

Запускает скачивание с разными уровнями конкурентности (6, 10, 14)
 и сравнивает результаты для определения оптимальной конфигурации.

Использование:
    python -m docreciv.scripts.concurrency_test --date 2026-01-28 --levels 6 10 14
    python -m docreciv.scripts.concurrency_test --date 2026-01-28 --levels 6 10 14 --sample-size 100
"""

import argparse
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from pymongo import MongoClient

from docreciv.core.config import get_config
from docreciv.downloader.enhanced_service import EnhancedProtocolDownloader
from docreciv.downloader.models import DownloadRequest
from docreciv.pipeline.models import ConcurrencyTestResult, StageStatus

# Временные директории для тестов
TEST_OUTPUT_BASE = Path("/tmp/concurrency_test")


@dataclass
class TestConfig:
    """Конфигурация теста."""
    target_date: datetime
    concurrency_level: int
    sample_size: int
    output_dir: Path
    clean_before: bool = True


class ConcurrencyTester:
    """Тестировщик конкурентности скачивания."""

    def __init__(self, mongo_client: MongoClient):
        """
        Инициализация тестировщика.

        Args:
            mongo_client: Клиент MongoDB для получения тестовых данных
        """
        self.mongo = mongo_client
        self.db = mongo_client[get_config().sync_db.local_mongo.db]
        self.collection = self.db[get_config().sync_db.local_mongo.collection]

    def get_sample_protocols(
        self,
        target_date: datetime,
        sample_size: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Получает выборку протоколов для тестирования.

        Args:
            target_date: Целевая дата
            sample_size: Размер выборки

        Returns:
            Список протоколов с URL
        """
        # Ищем по loadDate (дата загрузки в MongoDB)
        start_dt = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)
        end_dt = datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59)

        query = {
            "loadDate": {
                "$gte": start_dt,
                "$lte": end_dt
            },
            "urls.0": {"$exists": True},  # Есть хотя бы один URL
            "url_count": {"$gt": 0}
        }

        protocols = list(
            self.collection.find(query)
            .limit(sample_size)
        )

        print(f"Found {len(protocols)} protocols for testing")
        return protocols

    def prepare_test_output_dir(self, concurrency_level: int) -> Path:
        """
        Подготавливает выходную директорию для теста.

        Args:
            concurrency_level: Уровень конкурентности

        Returns:
            Путь к директории
        """
        output_dir = TEST_OUTPUT_BASE / f"concurrency_{concurrency_level}"

        # Очищаем если существует
        if output_dir.exists():
            shutil.rmtree(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def run_single_test(self, config: TestConfig) -> ConcurrencyTestResult:
        """
        Запускает одиночный тест с указанной конкурентностью.

        Args:
            config: Конфигурация теста

        Returns:
            Результат теста
        """
        print(f"\n{'='*60}")
        print(f"Running test with concurrency: {config.concurrency_level}")
        print(f"{'='*60}")

        # Подготавливаем директорию
        output_dir = self.prepare_test_output_dir(config.concurrency_level)

        # Получаем выборку протоколов
        protocols = self.get_sample_protocols(config.target_date, config.sample_size)

        if not protocols:
            print("No protocols found for testing")
            return ConcurrencyTestResult(concurrency_level=config.concurrency_level)

        # Создаём downloader с указанной конкурентностью
        os.environ["DOWNLOAD_CONCURRENCY"] = str(config.concurrency_level)
        os.environ["PROTOCOLS_CONCURRENCY"] = str(config.concurrency_level)

        downloader = EnhancedProtocolDownloader(output_dir=output_dir)

        # Формируем список record_id
        record_ids = [str(p.get("_id", "")) for p in protocols if p.get("_id")]

        print(f"Testing with {len(record_ids)} protocols")
        print(f"Output directory: {output_dir}")

        # Запускаем скачивание
        start_time = time.time()

        try:
            result = downloader.process_download_request(
                DownloadRequest(
                    from_date=config.target_date,
                    to_date=config.target_date,
                    record_ids=record_ids,
                    skip_existing=False,
                    force_reload=True,
                    dry_run=False
                )
            )
        except Exception as e:
            print(f"Error during download: {e}")
            return ConcurrencyTestResult(
                concurrency_level=config.concurrency_level,
                error_types={"error": 1}
            )

        end_time = time.time()
        duration = end_time - start_time

        # Собираем результаты
        stats = result.stats if hasattr(result, "stats") else {}
        errors = result.errors if hasattr(result, "errors") else []

        # Подсчитываем файлы (учитываем структуру с датой: YYYY-MM-DD/Input/UNIT_xxx/)
        files_downloaded = 0
        files_failed = 0

        # Рекурсивно ищем PDF файлы
        for pdf_file in output_dir.glob("**/*.pdf"):
            files_downloaded += 1

        # Рекурсивно ищем meta файлы для подсчёта ошибок
        for meta_file in output_dir.glob("**/unit.meta.json"):
            try:
                with open(meta_file, "r") as f:
                    meta = json.load(f)
                    failed = meta.get("files_failed", 0)
                    files_failed += failed
            except:
                pass

        # Анализируем ошибки
        error_types: Dict[str, int] = {}
        for error in errors:
            if "Connection" in str(error):
                error_types["connection"] = error_types.get("connection", 0) + 1
            elif "timeout" in str(error).lower():
                error_types["timeout"] = error_types.get("timeout", 0) + 1
            elif "file name" in str(error).lower():
                error_types["filename"] = error_types.get("filename", 0) + 1
            else:
                error_types["other"] = error_types.get("other", 0) + 1

        total_files = files_downloaded + files_failed
        success_rate = (files_downloaded / total_files * 100) if total_files > 0 else 0

        test_result = ConcurrencyTestResult(
            concurrency_level=config.concurrency_level,
            protocols_processed=len(record_ids),
            files_downloaded=files_downloaded,
            files_failed=files_failed,
            duration_seconds=duration,
            throughput_protocols_per_sec=len(record_ids) / duration if duration > 0 else 0,
            throughput_files_per_sec=files_downloaded / duration if duration > 0 else 0,
            success_rate=success_rate,
            error_types=error_types
        )

        print(f"\nResults for concurrency {config.concurrency_level}:")
        print(f"  Protocols: {test_result.protocols_processed}")
        print(f"  Files downloaded: {test_result.files_downloaded}")
        print(f"  Files failed: {test_result.files_failed}")
        print(f"  Duration: {test_result.duration_seconds:.2f}s")
        print(f"  Throughput: {test_result.throughput_files_per_sec:.2f} files/sec")
        print(f"  Success rate: {test_result.success_rate:.1f}%")

        if error_types:
            print(f"  Errors: {error_types}")

        return test_result

    def compare_concurrency_levels(
        self,
        target_date: datetime,
        levels: List[int],
        sample_size: int = 50
    ) -> Dict[int, ConcurrencyTestResult]:
        """
        Сравнивает несколько уровней конкурентности.

        Args:
            target_date: Целевая дата
            levels: Список уровней конкурентности для тестирования
            sample_size: Размер выборки протоколов

        Returns:
            Словарь {уровень: результат}
        """
        results = {}

        for level in levels:
            config = TestConfig(
                target_date=target_date,
                concurrency_level=level,
                sample_size=sample_size,
                output_dir=TEST_OUTPUT_BASE
            )

            result = self.run_single_test(config)
            results[level] = result

        return results

    def generate_comparison_report(
        self,
        results: Dict[int, ConcurrencyTestResult]
    ) -> str:
        """
        Генерирует отчёт сравнения.

        Args:
            results: Результаты тестов

        Returns:
            Текст отчёта
        """
        lines = []
        lines.append("=" * 100)
        lines.append("CONCURRENCY TEST COMPARISON REPORT")
        lines.append("=" * 100)
        lines.append("")

        # Таблица результатов
        lines.append(f"{'Concurrency':<12} {'Files':<10} {'Failed':<10} "
                    f"{'Duration (s)':<15} {'Files/s':<12} {'Success %':<12} {'Errors':<15}")
        lines.append("-" * 100)

        for level in sorted(results.keys()):
            r = results[level]
            errors_str = ", ".join(f"{k}:{v}" for k, v in r.error_types.items()) if r.error_types else "None"
            lines.append(
                f"{level:<12} "
                f"{r.files_downloaded:<10} "
                f"{r.files_failed:<10} "
                f"{r.duration_seconds:<15.2f} "
                f"{r.throughput_files_per_sec:<12.2f} "
                f"{r.success_rate:<12.1f} "
                f"{errors_str:<15}"
            )

        lines.append("=" * 100)
        lines.append("")

        # Анализ и рекомендации
        lines.append("ANALYSIS")
        lines.append("-" * 40)

        # Находим лучший по успеху
        best_success = max(results.items(), key=lambda x: x[1].success_rate)
        lines.append(f"Best success rate: concurrency={best_success[0]} ({best_success[1].success_rate:.1f}%)")

        # Находим лучший по скорости
        best_speed = max(results.items(), key=lambda x: x[1].throughput_files_per_sec)
        lines.append(f"Best throughput: concurrency={best_speed[0]} ({best_speed[1].throughput_files_per_sec:.2f} files/s)")

        # Находим минимальные ошибки
        min_errors = min(results.items(), key=lambda x: sum(x[1].error_types.values()))
        lines.append(f"Fewest errors: concurrency={min_errors[0]} ({sum(min_errors[1].error_types.values())} errors)")

        lines.append("")
        lines.append("RECOMMENDATION")
        lines.append("-" * 40)

        # Рекомендация на основе баланса успеха и скорости
        best_score = -1
        recommended_level = 10  # default

        for level, r in results.items():
            # Скор: успех * 0.7 + скорость (нормализованная) * 0.3
            speed_score = r.throughput_files_per_sec / max(
                rr.throughput_files_per_sec for rr in results.values()
            ) if r.throughput_files_per_sec > 0 else 0
            score = (r.success_rate / 100) * 0.7 + speed_score * 0.3

            if score > best_score:
                best_score = score
                recommended_level = level

        lines.append(f"Recommended concurrency level: {recommended_level}")
        lines.append(f"  (Based on balanced score of success rate and throughput)")
        lines.append("")

        # Детальная статистика ошибок
        lines.append("ERROR BREAKDOWN")
        lines.append("-" * 40)
        for level in sorted(results.keys()):
            r = results[level]
            if r.error_types:
                lines.append(f"Concurrency {level}:")
                for error_type, count in r.error_types.items():
                    lines.append(f"  - {error_type}: {count}")
            else:
                lines.append(f"Concurrency {level}: No errors")

        return "\n".join(lines)

    def export_results_json(
        self,
        results: Dict[int, ConcurrencyTestResult],
        output_path: Path
    ) -> None:
        """
        Экспортирует результаты в JSON.

        Args:
            results: Результаты тестов
            output_path: Путь к выходному файлу
        """
        output_data = {
            "test_date": datetime.utcnow().isoformat(),
            "results": {
                str(level): r.to_dict()
                for level, r in results.items()
            }
        }

        with open(output_path, "w") as f:
            json.dump(output_data, f, indent=2)

        print(f"\nResults exported to: {output_path}")


def main():
    """Главная функция для CLI."""
    parser = argparse.ArgumentParser(
        description="Test download concurrency levels"
    )
    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Target date for testing (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--levels",
        type=int,
        nargs="+",
        default=[6, 10, 14],
        help="Concurrency levels to test (default: 6 10 14)"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=50,
        help="Number of protocols to test (default: 50)"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output JSON file path"
    )

    args = parser.parse_args()

    # Парсим дату
    try:
        target_date = datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
        sys.exit(1)

    # Подключаемся к MongoDB
    try:
        config = get_config()
        local_config = config.sync_db.local_mongo
        connection_url = (
            f"mongodb://{local_config.user}:{local_config.password}"
            f"@{local_config.server}"
        )
        client = MongoClient(connection_url)
        client.admin.command("ping")
        print("Connected to MongoDB")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        sys.exit(1)

    # Запускаем тесты
    tester = ConcurrencyTester(client)

    print(f"\nStarting concurrency tests for {args.date}")
    print(f"Testing levels: {args.levels}")
    print(f"Sample size: {args.sample_size}")

    results = tester.compare_concurrency_levels(
        target_date=target_date,
        levels=args.levels,
        sample_size=args.sample_size
    )

    # Выводим отчёт
    report = tester.generate_comparison_report(results)
    print("\n" + report)

    # Экспорт в JSON
    if args.output:
        tester.export_results_json(results, Path(args.output))

    client.close()


if __name__ == "__main__":
    main()

"""
Интеграционный скрипт для запуска полного pipeline.

Оркестрирует:
1. Сбор метрик (опционально)
2. Синхронизацию протоколов из remote MongoDB
3. Скачивание файлов с указанной конкурентностью
4. Создание pipeline метаданных (MongoDB + JSON contract)

Использование:
    python -m docreciv.scripts.run_pipeline --date 2026-01-28
    python -m docreciv.scripts.run_pipeline --date 2026-01-28 --concurrency 10
    python -m docreciv.scripts.run_pipeline --metrics-only --days 7
    python -m docreciv.scripts.run_pipeline --concurrency-test --date 2026-01-28
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from pymongo import MongoClient

from docreciv.core.config import get_config
from docreciv.sync_db.enhanced_service import EnhancedSyncService
from docreciv.downloader.enhanced_service import EnhancedProtocolDownloader
from docreciv.downloader.models import DownloadRequest
from docreciv.pipeline.manager import PipelineManager
from docreciv.pipeline.models import StageStatus, PipelineDocument

# Константы
DEFAULT_PROCESSING_DIR = Path("/home/pak/Processing data")
DEFAULT_CONCURRENCY = 10


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


def run_sync_stage(target_date: datetime) -> bool:
    """
    Этап 1: Синхронизация протоколов из remote MongoDB.

    Args:
        target_date: Целевая дата для синхронизации

    Returns:
        True если синхронизация успешна
    """
    print("\n" + "=" * 60)
    print("STAGE 1: SYNCHRONIZING PROTOCOLS FROM REMOTE MONGODB")
    print("=" * 60)

    sync_service = EnhancedSyncService()

    try:
        result = sync_service.sync_protocols_for_date(target_date)

        print(f"\nSync result: {result.status}")
        print(f"  Scanned: {result.scanned}")
        print(f"  Inserted: {result.inserted}")
        print(f"  Skipped: {result.skipped_existing}")
        print(f"  Duration: {result.duration:.2f}s")

        return result.success

    except Exception as e:
        print(f"Sync error: {e}")
        return False

    finally:
        sync_service.close()


def run_download_stage(
    target_date: datetime,
    output_dir: Path,
    concurrency: int = DEFAULT_CONCURRENCY,
    pipeline_manager: Optional[PipelineManager] = None,
    run_id: Optional[str] = None,
    force_reload: bool = False
) -> bool:
    """
    Этап 2: Скачивание документов.

    Args:
        target_date: Целевая дата
        output_dir: Базовая директория для скачивания
        concurrency: Уровень конкурентности
        pipeline_manager: Менеджер pipeline для метаданных
        run_id: ID pipeline run
        force_reload: Принудительная перезагрузка даже если файлы существуют

    Returns:
        True если скачивание успешно
    """
    print("\n" + "=" * 60)
    print(f"STAGE 2: DOWNLOADING DOCUMENTS (concurrency={concurrency})")
    print("=" * 60)

    # Устанавливаем конкурентность через переменные окружения
    os.environ["DOWNLOAD_CONCURRENCY"] = str(concurrency)
    os.environ["PROTOCOLS_CONCURRENCY"] = str(concurrency)

    # Создаём downloader
    downloader = EnhancedProtocolDownloader(output_dir=output_dir)

    # Запускаем скачивание
    start_time = datetime.utcnow()

    if pipeline_manager and run_id:
        # Обновляем статус этапа
        pipeline_manager.update_stage(
            run_id,
            "download",
            StageStatus.RUNNING.value,
            metrics={"started": True}
        )

    try:
        result = downloader.process_download_request(
            DownloadRequest(
                from_date=target_date,
                to_date=target_date,
                skip_existing=not force_reload,  # Если force_reload, не пропускаем существующие
                force_reload=force_reload,
                dry_run=False
            )
        )

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        # Собираем метрики
        stats = result.stats if hasattr(result, "stats") else {}

        metrics = {
            "protocols_count": stats.get("processed", 0),
            "files_downloaded": stats.get("downloaded", 0),
            "files_failed": stats.get("failed", 0),
            "duration_seconds": duration,
            "concurrency": concurrency
        }

        print(f"\nDownload result:")
        print(f"  Protocols processed: {metrics['protocols_count']}")
        print(f"  Files downloaded: {metrics['files_downloaded']}")
        print(f"  Files failed: {metrics['files_failed']}")
        print(f"  Duration: {duration:.2f}s")

        if pipeline_manager and run_id:
            # Обновляем статус этапа
            final_status = StageStatus.COMPLETED.value
            if metrics["files_failed"] > metrics["files_downloaded"]:
                final_status = StageStatus.FAILED.value

            pipeline_manager.update_stage(
                run_id,
                "download",
                final_status,
                metrics=metrics
            )

        return True

    except Exception as e:
        print(f"Download error: {e}")

        if pipeline_manager and run_id:
            pipeline_manager.update_stage(
                run_id,
                "download",
                StageStatus.FAILED.value,
                error=str(e)
            )

        return False


def run_metrics_only_stage(days: int = 7, output: Optional[str] = None) -> bool:
    """
    Только сбор метрик без выполнения pipeline.

    Args:
        days: Количество дней для анализа
        output: Опциональный путь для экспорта JSON

    Returns:
        True если сбор метрик успешен
    """
    print("\n" + "=" * 60)
    print(f"METRICS COLLECTION: Last {days} days")
    print("=" * 60)

    from docreciv.scripts.metrics_collector import (
        collect_daily_metrics,
        collect_total_metrics,
        print_metrics_table,
        export_metrics_json
    )

    client = get_mongo_client()

    try:
        daily_metrics = collect_daily_metrics(
            client,
            "docling_metadata",
            "protocols",
            days
        )

        total_metrics = collect_total_metrics(
            client,
            "docling_metadata",
            "protocols"
        )

        print("\nTOTAL COLLECTION METRICS:")
        print(f"  Total protocols: {total_metrics['total_protocols']}")
        print(f"  Pending: {total_metrics['pending_protocols']}")
        print(f"  Downloaded: {total_metrics['downloaded_protocols']}")

        print_metrics_table(daily_metrics)

        if output:
            export_metrics_json(daily_metrics, total_metrics, Path(output))

        return True

    except Exception as e:
        print(f"Metrics collection error: {e}")
        return False
    finally:
        client.close()


def run_concurrency_test_stage(
    target_date: datetime,
    levels: list,
    sample_size: int = 50,
    output: Optional[str] = None
) -> bool:
    """
    Только тестирование конкурентности.

    Args:
        target_date: Целевая дата
        levels: Список уровней конкурентности
        sample_size: Размер выборки
        output: Опциональный путь для экспорта JSON

    Returns:
        True если тестирование успешно
    """
    print("\n" + "=" * 60)
    print(f"CONCURRENCY TEST: {target_date.date()}")
    print("=" * 60)

    from docreciv.scripts.concurrency_test import ConcurrencyTester

    client = get_mongo_client()

    try:
        tester = ConcurrencyTester(client)

        results = tester.compare_concurrency_levels(
            target_date=target_date,
            levels=levels,
            sample_size=sample_size
        )

        report = tester.generate_comparison_report(results)
        print("\n" + report)

        if output:
            tester.export_results_json(results, Path(output))

        return True

    except Exception as e:
        print(f"Concurrency test error: {e}")
        return False
    finally:
        client.close()


def run_full_pipeline(
    target_date: datetime,
    output_dir: Path,
    concurrency: int = DEFAULT_CONCURRENCY,
    skip_sync: bool = False,
    force_reload: bool = False
) -> bool:
    """
    Полный pipeline для указанной даты.

    Args:
        target_date: Целевая дата
        output_dir: Базовая директория для Processing data
        concurrency: Уровень конкурентности для скачивания
        skip_sync: Пропустить синхронизацию (если данные уже есть)
        force_reload: Принудительная перезагрузка файлов

    Returns:
        True если pipeline выполнен успешно
    """
    print("\n" + "=" * 80)
    print(f"PIPELINE RUN FOR {target_date.date()}")
    print("=" * 80)
    print(f"Output directory: {output_dir}")
    print(f"Concurrency: {concurrency}")

    client = get_mongo_client()
    batch_date_str = target_date.strftime("%Y-%m-%d")

    try:
        # Создаём менеджер pipeline
        pipeline_manager = PipelineManager(
            mongo_client=client,
            base_dir=output_dir
        )

        # Создаём pipeline run
        pipeline_run = pipeline_manager.create_run(batch_date_str)
        run_id = pipeline_run.run_id

        print(f"\nPipeline run ID: {run_id}")

        # Этап 1: Синхронизация (пропускаем если указано)
        if not skip_sync:
            if not run_sync_stage(target_date):
                pipeline_manager.update_stage(
                    run_id,
                    "download",
                    StageStatus.FAILED.value,
                    error="Sync stage failed"
                )
                return False
        else:
            print("\nSkipping sync stage (--skip-sync)")

        # Этап 2: Скачивание
        date_output_dir = output_dir / batch_date_str / "Input"
        date_output_dir.mkdir(parents=True, exist_ok=True)

        download_success = run_download_stage(
            target_date=target_date,
            output_dir=date_output_dir,
            concurrency=concurrency,
            pipeline_manager=pipeline_manager,
            run_id=run_id,
            force_reload=force_reload
        )

        if not download_success:
            return False

        # Записываем JSON contract
        contract_path = pipeline_manager.write_contract(run_id)

        if contract_path:
            print(f"\nContract file written: {contract_path}")
        else:
            print("\nWarning: Failed to write contract file")

        print("\n" + "=" * 80)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 80)

        return True

    except Exception as e:
        print(f"\nPipeline error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        client.close()


def main():
    """Главная функция для CLI."""
    parser = argparse.ArgumentParser(
        description="Run Docreciv pipeline: sync → download → metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline for a date
  python -m docreciv.scripts.run_pipeline --date 2026-01-28

  # With custom concurrency
  python -m docreciv.scripts.run_pipeline --date 2026-01-28 --concurrency 14

  # With custom output directory
  python -m docreciv.scripts.run_pipeline --date 2026-01-28 --output-dir "/home/pak/Processing data"

  # Skip sync (if data already exists)
  python -m docreciv.scripts.run_pipeline --date 2026-01-28 --skip-sync

  # Only collect metrics
  python -m docreciv.scripts.run_pipeline --metrics-only --days 7

  # Only test concurrency
  python -m docreciv.scripts.run_pipeline --concurrency-test --date 2026-01-28 --levels 6 10 14
        """
    )

    # Основные аргументы
    parser.add_argument(
        "--date",
        type=str,
        help="Target date for pipeline (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Download concurrency level (default: {DEFAULT_CONCURRENCY})"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_PROCESSING_DIR),
        help=f"Base output directory (default: {DEFAULT_PROCESSING_DIR})"
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip sync stage (use if data already exists)"
    )
    parser.add_argument(
        "--force-reload",
        action="store_true",
        help="Force reload of existing files"
    )

    # Специальные режимы
    parser.add_argument(
        "--metrics-only",
        action="store_true",
        help="Only collect metrics, don't run pipeline"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Days for metrics collection (default: 7)"
    )
    parser.add_argument(
        "--concurrency-test",
        action="store_true",
        help="Only run concurrency test"
    )
    parser.add_argument(
        "--test-levels",
        type=int,
        nargs="+",
        default=[6, 10, 14],
        help="Concurrency levels to test (default: 6 10 14)"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=50,
        help="Sample size for concurrency test (default: 50)"
    )

    # Экспорт
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Export results to JSON file"
    )

    args = parser.parse_args()

    # Режим только метрики
    if args.metrics_only:
        success = run_metrics_only_stage(args.days, args.output)
        sys.exit(0 if success else 1)

    # Режим тестирования конкурентности
    if args.concurrency_test:
        if not args.date:
            print("--date is required for concurrency test")
            sys.exit(1)

        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print(f"Invalid date format: {args.date}")
            sys.exit(1)

        success = run_concurrency_test_stage(
            target_date=target_date,
            levels=args.test_levels,
            sample_size=args.sample_size,
            output=args.output
        )
        sys.exit(0 if success else 1)

    # Обычный режим (полный pipeline)
    if not args.date:
        print("--date is required for pipeline run")
        print("Use --metrics-only to collect metrics without running pipeline")
        sys.exit(1)

    try:
        target_date = datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"Invalid date format: {args.date}")
        sys.exit(1)

    output_dir = Path(args.output_dir)

    success = run_full_pipeline(
        target_date=target_date,
        output_dir=output_dir,
        concurrency=args.concurrency,
        skip_sync=args.skip_sync,
        force_reload=args.force_reload
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

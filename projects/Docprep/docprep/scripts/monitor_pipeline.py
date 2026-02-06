#!/usr/bin/env python3
"""
Скрипт для мониторинга прогресса pipeline обработки.

Отслеживает:
- Текущий цикл обработки
- Количество обработанных units
- Статус по категориям (direct, convert, extract, normalize, empty, mixed)
- Время выполнения
"""
import sys
from pathlib import Path
from collections import Counter
from datetime import datetime
import time


def count_units_in_dir(dir_path: Path) -> int:
    """Подсчитывает количество UNIT директорий в указанном пути."""
    if not dir_path.exists():
        return 0
    return len(list(dir_path.rglob("UNIT_*")))


def get_pipeline_status(data_dir: Path):
    """Получает текущий статус pipeline обработки."""

    # Input (исходные данные)
    input_dir = data_dir / "Input"
    input_units = count_units_in_dir(input_dir)

    # Processing (текущая обработка)
    processing_stats = {}
    for cycle in [1, 2, 3]:
        processing_dir = data_dir / "Processing" / f"Processing_{cycle}"
        if processing_dir.exists():
            convert_count = count_units_in_dir(processing_dir / "Convert")
            extract_count = count_units_in_dir(processing_dir / "Extract")
            normalize_count = count_units_in_dir(processing_dir / "Normalize")
            processing_stats[cycle] = {
                "Convert": convert_count,
                "Extract": extract_count,
                "Normalize": normalize_count,
                "Total": convert_count + extract_count + normalize_count
            }

    # Merge (обработанные данные)
    merge_stats = {}
    merge_0_count = count_units_in_dir(data_dir / "Merge" / "Merge_0" / "Direct") if (data_dir / "Merge" / "Merge_0").exists() else 0
    merge_stats[0] = merge_0_count

    for cycle in [1, 2, 3]:
        merge_dir = data_dir / "Merge" / f"Merge_{cycle}"
        if merge_dir.exists():
            converted_count = count_units_in_dir(merge_dir / "Converted")
            extracted_count = count_units_in_dir(merge_dir / "Extracted")
            normalized_count = count_units_in_dir(merge_dir / "Normalized")
            merge_stats[cycle] = {
                "Converted": converted_count,
                "Extracted": extracted_count,
                "Normalized": normalized_count,
                "Total": converted_count + extracted_count + normalized_count
            }

    # Exceptions (ошибки)
    exceptions_stats = {}
    for cycle in [1, 2, 3]:
        exceptions_dir = data_dir / "Exceptions" / f"Exceptions_{cycle}"
        if exceptions_dir.exists():
            total_exceptions = count_units_in_dir(exceptions_dir)
            exceptions_stats[cycle] = total_exceptions

    # Ready2Docling (финальный результат)
    ready_dir = data_dir / "Ready2Docling"
    ready_units = count_units_in_dir(ready_dir) if ready_dir.exists() else 0

    return {
        "input_units": input_units,
        "processing": processing_stats,
        "merge": merge_stats,
        "exceptions": exceptions_stats,
        "ready": ready_units,
    }


def display_status(status: dict, start_time: datetime = None):
    """Отображает статус обработки."""
    print("\n" + "=" * 80)
    print(f"📊 СТАТУС PIPELINE ОБРАБОТКИ")
    if start_time:
        elapsed = datetime.now() - start_time
        print(f"⏱️  Время выполнения: {elapsed}")
    print("=" * 80)

    print(f"\n📥 Input: {status['input_units']} units")

    # Processing
    if status['processing']:
        print(f"\n⚙️  Processing (текущая обработка):")
        for cycle, stats in sorted(status['processing'].items()):
            if stats['Total'] > 0:
                print(f"  Цикл {cycle}: {stats['Total']} units")
                print(f"    - Convert: {stats['Convert']}")
                print(f"    - Extract: {stats['Extract']}")
                print(f"    - Normalize: {stats['Normalize']}")

    # Merge
    if status['merge']:
        print(f"\n🔀 Merge (обработанные):")
        total_merge = 0
        for cycle, stats in sorted(status['merge'].items()):
            if cycle == 0:
                if stats > 0:
                    print(f"  Merge_0 (Direct): {stats} units")
                    total_merge += stats
            else:
                if isinstance(stats, dict) and stats['Total'] > 0:
                    print(f"  Merge_{cycle}: {stats['Total']} units")
                    print(f"    - Converted: {stats['Converted']}")
                    print(f"    - Extracted: {stats['Extracted']}")
                    print(f"    - Normalized: {stats['Normalized']}")
                    total_merge += stats['Total']
        print(f"  📊 Всего в Merge: {total_merge} units")

    # Exceptions
    if status['exceptions']:
        print(f"\n⚠️  Exceptions (ошибки):")
        total_exceptions = 0
        for cycle, count in sorted(status['exceptions'].items()):
            if count > 0:
                print(f"  Exceptions_{cycle}: {count} units")
                total_exceptions += count
        print(f"  📊 Всего исключений: {total_exceptions} units")

    # Ready2Docling
    if status['ready'] > 0:
        print(f"\n✅ Ready2Docling (финальный результат): {status['ready']} units")

    # Прогресс
    total_processed = sum(
        stats['Total'] if isinstance(stats, dict) else stats
        for stats in status['merge'].values()
    )
    total_processing = sum(
        stats['Total']
        for stats in status['processing'].values()
    )
    total_exceptions = sum(status['exceptions'].values())

    total_accounted = total_processed + total_processing + total_exceptions

    print(f"\n📈 Прогресс:")
    print(f"  - Обработано: {total_processed} units")
    print(f"  - В обработке: {total_processing} units")
    print(f"  - Исключений: {total_exceptions} units")
    print(f"  - Учтено: {total_accounted} / {status['input_units']} units")

    if status['input_units'] > 0:
        progress = (total_accounted / status['input_units']) * 100
        print(f"  - Прогресс: {progress:.1f}%")

    print("=" * 80)


def monitor_continuous(data_dir: Path, interval: int = 60):
    """Непрерывный мониторинг с заданным интервалом."""
    start_time = datetime.now()

    print(f"🔍 Начало мониторинга pipeline для {data_dir}")
    print(f"⏱️  Обновление каждые {interval} секунд")
    print("Нажмите Ctrl+C для остановки\n")

    try:
        while True:
            status = get_pipeline_status(data_dir)
            display_status(status, start_time)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n\n⏹️  Мониторинг остановлен")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Мониторинг прогресса pipeline обработки"
    )
    parser.add_argument(
        "data_dir",
        type=Path,
        help="Путь к директории с данными (например, Data/2025-12-20)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Интервал обновления в секундах (по умолчанию: 60)"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Показать статус один раз (без непрерывного мониторинга)"
    )

    args = parser.parse_args()

    if not args.data_dir.exists():
        print(f"❌ Директория не существует: {args.data_dir}")
        sys.exit(1)

    if args.once:
        status = get_pipeline_status(args.data_dir)
        display_status(status)
    else:
        monitor_continuous(args.data_dir, args.interval)


if __name__ == "__main__":
    main()

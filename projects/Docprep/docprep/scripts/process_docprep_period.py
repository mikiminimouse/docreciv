#!/usr/bin/env python3
"""
Скрипт для последовательной обработки периода дат через Docprep pipeline.

Выполняет полный препроцессинг для каждой даты в периоде с возможностью
пакетной обработки для управления дисковым пространством.

Использование:
    python -m docprep.scripts.process_docprep_period 2025-12-17 2025-12-31
    python -m docprep.scripts.process_docprep_period 2025-12-17 2025-12-31 --batch-size 5
    python -m docprep.scripts.process_docprep_period 2025-12-17 2025-12-31 --continue-on-error
"""
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any


def parse_date(date_str: str) -> str:
    """Парсит дату из формата YYYY-MM-DD."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(f"Неверный формат даты: {date_str}. Используйте YYYY-MM-DD")


def get_dates_in_period(start_date: str, end_date: str) -> List[str]:
    """
    Возвращает список дат в периоде [start_date, end_date].

    Args:
        start_date: Начальная дата в формате YYYY-MM-DD
        end_date: Конечная дата в формате YYYY-MM-DD

    Returns:
        Список дат в формате YYYY-MM-DD
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)

    return dates


def process_single_day(
    date_str: str,
    max_cycles: int,
    base_dir: Path,
    dry_run: bool,
    verbose: bool
) -> Dict[str, Any]:
    """
    Обрабатывает один день через Docprep pipeline.

    Делегирует выполнение модулю process_docprep_day.
    """
    from .process_docprep_day import process_single_day

    return process_single_day(
        date_str=date_str,
        max_cycles=max_cycles,
        base_dir=base_dir,
        dry_run=dry_run,
        verbose=verbose
    )


def process_period(
    start_date: str,
    end_date: str,
    max_cycles: int = 3,
    base_dir: Path = Path("/home/pak/Processing data"),
    batch_size: int = 0,  # 0 = обрабатывать все даты
    dry_run: bool = False,
    verbose: bool = False,
    continue_on_error: bool = True
) -> Dict[str, Any]:
    """
    Обрабатывает период дат.

    Args:
        start_date: Начальная дата (YYYY-MM-DD)
        end_date: Конечная дата (YYYY-MM-DD)
        max_cycles: Максимальное количество циклов
        base_dir: Базовая директория
        batch_size: Размер пакета для обработки (0 = все)
        dry_run: Режим проверки
        verbose: Подробный вывод
        continue_on_error: Продолжать при ошибках

    Returns:
        Агрегированный результат обработки
    """
    print("=" * 70)
    print("ОБРАБОТКА ПЕРИОДА ЧЕРЕЗ DOCPREP")
    print("=" * 70)
    print(f"Период: {start_date} → {end_date}")
    print(f"Максимум циклов: {max_cycles}")
    print(f"Размер пакета: {batch_size if batch_size > 0 else 'все даты'}")
    print(f"Продолжать при ошибках: {continue_on_error}")
    print("=" * 70)

    dates = get_dates_in_period(start_date, end_date)
    print(f"\nДат для обработки: {len(dates)}")
    print(f"Даты: {', '.join(dates[:5])}{'...' if len(dates) > 5 else ''}")

    results = {
        "period": f"{start_date} → {end_date}",
        "total_days": len(dates),
        "daily_results": {},
        "summary": {
            "success": 0,
            "failed": 0,
            "error": 0,
            "dry_run": 0
        }
    }

    # Разбиваем даты на пакеты если указан batch_size
    if batch_size > 0:
        batches = [dates[i:i + batch_size] for i in range(0, len(dates), batch_size)]
        print(f"\nПакетов для обработки: {len(batches)}")
    else:
        batches = [dates]

    for batch_idx, batch in enumerate(batches, 1):
        if batch_size > 0:
            print(f"\n{'#'*70}")
            print(f"# ПАКЕТ {batch_idx}/{len(batches)}: {batch[0]} → {batch[-1]}")
            print(f"{'#'*70}")

        for i, date_str in enumerate(batch, 1):
            print(f"\n{'─'*60}")
            print(f"ДЕНЬ {i}/{len(batch)}: {date_str}")
            print(f"{'─'*60}")

            try:
                result = process_single_day(
                    date_str=date_str,
                    max_cycles=max_cycles,
                    base_dir=base_dir,
                    dry_run=dry_run,
                    verbose=verbose
                )

                results["daily_results"][date_str] = result

                # Обновляем сводку
                status = result.get("status", "unknown")
                if status == "success":
                    results["summary"]["success"] += 1
                elif status == "failed":
                    results["summary"]["failed"] += 1
                elif status == "error":
                    results["summary"]["error"] += 1
                elif status == "dry_run":
                    results["summary"]["dry_run"] += 1

                # Прерываем при ошибке если не продолжаем
                if status in ["failed", "error"] and not continue_on_error:
                    print(f"\n❌ Остановка обработки из-за ошибки в {date_str}")
                    return results

            except Exception as e:
                print(f"❌ Критическая ошибка при обработке {date_str}: {e}")
                results["daily_results"][date_str] = {
                    "status": "error",
                    "error": str(e)
                }
                results["summary"]["error"] += 1

                if not continue_on_error:
                    return results

    # Финальный отчёт
    print(f"\n\n{'='*70}")
    print("ФИНАЛЬНЫЙ ОТЧЁТ")
    print(f"{'='*70}")
    print(f"\nПериод: {results['period']}")
    print(f"Обработано дней: {len(results['daily_results'])}/{results['total_days']}")

    summary = results["summary"]
    print(f"\nРезультаты:")
    print(f"   ✅ Успешно: {summary['success']}")
    print(f"   ❌ Failed: {summary['failed']}")
    print(f"   ⚠️  Errors: {summary['error']}")
    if summary['dry_run'] > 0:
        print(f"   🔍 Dry-run: {summary['dry_run']}")

    return results


def main():
    """Точка входа для CLI."""
    parser = argparse.ArgumentParser(
        description="Последовательная обработка периода дат через Docprep",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  # Обработка всего периода
  python -m docprep.scripts.process_docprep_period 2025-12-17 2025-12-31

  # Пакетами по 5 дней
  python -m docprep.scripts.process_docprep_period 2025-12-17 2025-12-31 --batch-size 5

  # Останавливаться при первой ошибке
  python -m docprep.scripts.process_docprep_period 2025-12-17 2025-12-31 --no-continue

  # С увеличенным количеством циклов
  python -m docprep.scripts.process_docprep_period 2025-12-17 2025-12-31 --max-cycles 5
        """
    )

    parser.add_argument(
        "start_date",
        type=parse_date,
        help="Начальная дата в формате YYYY-MM-DD"
    )

    parser.add_argument(
        "end_date",
        type=parse_date,
        help="Конечная дата в формате YYYY-MM-DD"
    )

    parser.add_argument(
        "--max-cycles",
        type=int,
        default=3,
        help="Максимальное количество циклов (default: 3)"
    )

    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("/home/pak/Processing data"),
        help="Базовая директория Processing data"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Размер пакета для обработки (default: 0 = все даты)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Режим проверки без реальных изменений"
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Подробный вывод"
    )

    parser.add_argument(
        "--no-continue",
        action="store_true",
        help="Останавливаться при первой ошибке"
    )

    args = parser.parse_args()

    results = process_period(
        start_date=args.start_date,
        end_date=args.end_date,
        max_cycles=args.max_cycles,
        base_dir=args.base_dir,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        verbose=args.verbose,
        continue_on_error=not args.no_continue
    )

    # Код выхода на основе результатов
    summary = results["summary"]
    total_processed = summary["success"] + summary["failed"] + summary["error"]

    if summary["error"] > 0:
        sys.exit(1)
    elif summary["failed"] > 0 and summary["success"] == 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()

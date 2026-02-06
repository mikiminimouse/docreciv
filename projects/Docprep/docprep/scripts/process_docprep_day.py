#!/usr/bin/env python3
"""
Скрипт для обработки одного дня через Docprep pipeline.

Создаёт PipelineRun через Docreciv PipelineTracker и запускает полный
препроцессинг документов (классификация, конвертация, распаковка, merge).

Использование:
    python -m docprep.scripts.process_docprep_day 2025-12-17
    python -m docprep.scripts.process_docprep_day 2025-12-17 --dry-run
    python -m docprep.scripts.process_docprep_day 2025-12-17 --max-cycles 3 --verbose
"""
import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional


def parse_date(date_str: str) -> str:
    """Парсит дату из формата YYYY-MM-DD."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(f"Неверный формат даты: {date_str}. Используйте YYYY-MM-DD")


def _ensure_docreciv_path():
    """Добавляет путь к Docreciv в sys.path если нужно."""
    import sys
    docreciv_path = Path("/home/pak/projects/Docreciv")
    if docreciv_path.exists() and str(docreciv_path) not in sys.path:
        sys.path.insert(0, str(docreciv_path))


def process_single_day(
    date_str: str,
    max_cycles: int = 3,
    base_dir: Path = Path("/home/pak/Processing data"),
    dry_run: bool = False,
    verbose: bool = False
) -> Dict[str, Any]:
    """
    Обрабатывает один день через Docprep pipeline.

    Args:
        date_str: Дата в формате YYYY-MM-DD
        max_cycles: Максимальное количество циклов обработки
        base_dir: Базовая директория Processing data
        dry_run: Режим проверки без реальных изменений
        verbose: Подробный вывод

    Returns:
        Словарь с результатами обработки
    """
    # Убеждаемся что Docreciv доступен
    _ensure_docreciv_path()

    print(f"\n{'='*70}")
    print(f"DOCPREP PIPELINE ДЛЯ {date_str}")
    print(f"{'='*70}")

    input_dir = base_dir / date_str / "Input"
    output_dir = base_dir / date_str / "Ready2Docling"

    # Проверяем существование входной директории
    if not input_dir.exists():
        return {
            "status": "error",
            "error": f"Входная директория не существует: {input_dir}"
        }

    print(f"\nВходная директория: {input_dir}")
    print(f"Выходная директория: {output_dir}")
    print(f"Максимум циклов: {max_cycles}")

    # 1. Создаём PipelineRun через PipelineTracker
    tracker_run_id = None
    try:
        from docprep.core.database import get_database
        from docreciv.pipeline.events import Stage

        db = get_database()
        if db.is_connected():
            tracker_run_id = db.create_pipeline_run(
                batch_date=date_str,
                stage=Stage.DOCPREP,
                config={
                    "max_cycles": max_cycles,
                    "input_dir": str(input_dir),
                    "output_dir": str(output_dir),
                    "dry_run": dry_run
                }
            )
            if tracker_run_id:
                print(f"PipelineTracker run_id: {tracker_run_id}")
    except ImportError:
        print("PipelineTracker недоступен - работа без трекинга")
    except Exception as e:
        print(f"Предупреждение: не удалось создать PipelineRun: {e}")

    # 2. Запускаем Docprep pipeline
    if not dry_run:
        try:
            import sys
            from io import StringIO

            # Сохраняем stdout для перехвата вывода
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = StringIO()
            sys.stderr = StringIO()

            try:
                # Прямой вызов функции run из pipeline.py
                from docprep.cli.pipeline import run as run_pipeline

                # Вызываем pipeline напрямую
                run_pipeline(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    max_cycles=max_cycles,
                    dry_run=False,
                    verbose=verbose,
                    enable_mongo=True,
                    enable_tracker=True,
                    tracker_run_id=tracker_run_id
                )

                # Если дошли сюда, значит успешно
                status = "success"
                exit_code_val = 0

            except SystemExit as e:
                # Typer вызывает sys.exit(), перехватываем код
                exit_code_val = e.code if e.code is not None else 0
                status = "success" if exit_code_val == 0 else "failed"

            except Exception as pipeline_err:
                status = "error"
                exit_code_val = 1
                raise pipeline_err

            finally:
                # Восстанавливаем stdout/stderr
                captured_output = sys.stdout.getvalue()
                sys.stdout = old_stdout
                sys.stderr = old_stderr

                # Выводим захваченный текст если verbose
                if verbose and captured_output:
                    print(captured_output)

            # Считаем статистику
            ready_units = list(output_dir.rglob("UNIT_*")) if output_dir.exists() else []

            metrics = {
                "units_total": len(ready_units),
                "exit_code": exit_code_val
            }

        except Exception as e:
            print(f"Ошибка при выполнении pipeline: {e}")
            import traceback
            traceback.print_exc()
            status = "error"
            metrics = {"error": str(e)}
    else:
        print("DRY RUN MODE - изменения не применяются")
        status = "dry_run"
        metrics = {"dry_run": True}

    # 3. Обновляем PipelineRun
    if tracker_run_id:
        try:
            from docprep.core.database import get_database
            from docreciv.pipeline.events import RunStatus

            db = get_database()
            final_status = RunStatus.COMPLETED if status == "success" else (
                RunStatus.FAILED if status == "failed" else RunStatus.CANCELLED
            )

            db.update_pipeline_run(
                run_id=tracker_run_id,
                status=final_status,
                metrics=metrics
            )
            print(f"PipelineTracker обновлён: {final_status.value}")
        except Exception as e:
            print(f"Предупреждение: не удалось обновить PipelineRun: {e}")

    return {
        "status": status,
        "run_id": tracker_run_id,
        "metrics": metrics,
        "date": date_str
    }


def main():
    """Точка входа для CLI."""
    parser = argparse.ArgumentParser(
        description="Обработка одного дня через Docprep pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  # Обработка с параметрами по умолчанию
  python -m docprep.scripts.process_docprep_day 2025-12-17

  # Dry-run (проверка без изменений)
  python -m docprep.scripts.process_docprep_day 2025-12-17 --dry-run

  # С увеличенным количеством циклов
  python -m docprep.scripts.process_docprep_day 2025-12-17 --max-cycles 5

  # С подробным выводом
  python -m docprep.scripts.process_docprep_day 2025-12-17 --verbose
        """
    )

    parser.add_argument(
        "date",
        type=parse_date,
        help="Дата в формате YYYY-MM-DD"
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
        help="Базовая директория Processing data (default: /home/pak/Processing data)"
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

    args = parser.parse_args()

    result = process_single_day(
        date_str=args.date,
        max_cycles=args.max_cycles,
        base_dir=args.base_dir,
        dry_run=args.dry_run,
        verbose=args.verbose
    )

    # Возвращаем код выхода в зависимости от статуса
    if result["status"] == "success":
        sys.exit(0)
    elif result["status"] == "dry_run":
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()

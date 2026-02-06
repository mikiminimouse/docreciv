"""
CLI команды для Chunked Classifier.

Обеспечивает интерфейс для chunked processing с recovery.
"""

import typer
from pathlib import Path
from typing import Optional
import json

from ..engine.chunked_classifier import ChunkedClassifier
from ..core.config import get_data_paths

app = typer.Typer(
    name="chunked-classifier",
    help="Chunked классификация с recovery (решает проблему потери данных)"
)


@app.command("run")
def run_chunked_classification(
    input_dir: Path = typer.Argument(..., help="Директория с входными файлами"),
    cycle: int = typer.Option(1, help="Номер цикла (1, 2, 3)"),
    date: Optional[str] = typer.Option(None, help="Дата протокола (YYYY-MM-DD)"),
    chunk_size: int = typer.Option(100, help="Размер чанка (файлов)"),
    state_dir: Optional[Path] = typer.Option(None, help="Директория для состояния (по умолчанию input_dir/.chunked_state)"),
    force_recreate: bool = typer.Option(False, help="Принудительно пересоздать чанки"),
    max_time: int = typer.Option(3600, help="Максимальное время обработки (секунды)"),
    dry_run: bool = typer.Option(False, help="Режим проверки без выполнения операций"),
    verbose: bool = typer.Option(False, help="Подробный вывод")
):
    """
    Запускает chunked классификацию с recovery.

    Решает проблему потери 89% данных путем обработки порциями
    с поддержкой восстановления после прерываний.
    """
    import logging

    # Настройка логирования
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    try:
        # Определяем state_dir
        if state_dir is None:
            state_dir = input_dir / ".chunked_state"

        # Получаем список файлов для обработки
        all_files = []
        if input_dir.exists():
            # Сначала проверим, есть ли UNIT директории в input_dir
            unit_dirs = [d for d in input_dir.iterdir() if d.is_dir() and d.name.startswith("UNIT_")]

            if unit_dirs:
                # Если есть UNIT директории, ищем файлы внутри них
                typer.echo(f"📁 Найдено {len(unit_dirs)} UNIT директорий, ищу файлы внутри них")
                for ext in ["*.docx", "*.doc", "*.pdf", "*.html", "*.zip", "*.rar", "*.rtf", "*.xls", "*.xlsx"]:
                    for unit_dir in unit_dirs:
                        all_files.extend(list(unit_dir.glob(ext)))
            else:
                # Если UNIT директорий нет, ищем файлы прямо в input_dir
                typer.echo("📄 UNIT директорий не найдено, ищу файлы в корне директории")
                for ext in ["*.docx", "*.doc", "*.pdf", "*.html", "*.zip", "*.rar", "*.rtf", "*.xls", "*.xlsx"]:
                    all_files.extend(list(input_dir.glob(ext)))

        if not all_files:
            typer.echo(f"❌ Файлы не найдены в {input_dir}")
            raise typer.Exit(1)

        typer.echo(f"📂 Найдено {len(all_files)} файлов для обработки")
        typer.echo(f"📦 Размер чанка: {chunk_size}")
        typer.echo(f"📁 State dir: {state_dir}")

        # Создаем chunked classifier
        classifier = ChunkedClassifier(state_dir, chunk_size)

        # Запускаем классификацию
        result = classifier.classify_with_recovery(
            input_files=all_files,
            cycle=cycle,
            protocol_date=date,
            force_recreate_chunks=force_recreate,
            max_processing_time=max_time,
            dry_run=dry_run
        )

        # Выводим результаты
        if result["success"]:
            typer.echo("✅ Chunked классификация завершена успешно!")
        else:
            typer.echo("⚠️  Chunked классификация завершена с ошибками")
            if "error" in result:
                typer.echo(f"❌ Ошибка: {result['error']}")

        # Детальная статистика
        if "stats" in result:
            stats = result["stats"]
            typer.echo("\n📊 Статистика:")
            typer.echo(f"   📦 Чанков: {stats['chunks']['completed']}/{stats['chunks']['total']} ({stats['chunks']['completion_percentage']:.1f}%)")
            typer.echo(f"   📄 Файлов: {stats['files']['processed']}/{stats['files']['total']} ({stats['files']['completion_percentage']:.1f}%)")

        if "processing_time" in result:
            typer.echo(f"   ⏱️  Время: {result['processing_time']:.1f} сек")

        if result.get("errors"):
            typer.echo(f"🚨 Ошибок: {len(result['errors'])}")
            for error in result["errors"][:3]:  # Показываем первые 3 ошибки
                typer.echo(f"   • {error}")

    except Exception as e:
        typer.echo(f"💥 Критическая ошибка: {e}")
        raise typer.Exit(1)


@app.command("status")
def get_status(
    state_dir: Path = typer.Argument(..., help="Директория с состоянием"),
    detailed: bool = typer.Option(False, help="Подробный статус")
):
    """Показывает статус chunked классификации."""
    try:
        # Проверяем существует ли state
        chunks_file = state_dir / "chunks.json"
        if not chunks_file.exists():
            typer.echo(f"❌ Состояние не найдено в {state_dir}")
            raise typer.Exit(1)

        # Создаем classifier для чтения состояния
        classifier = ChunkedClassifier(state_dir)

        # Получаем статус
        status = classifier.get_status_report()

        typer.echo("📊 Статус Chunked Classification:")
        typer.echo(f"   📁 State dir: {state_dir}")

        # Статистика чанков
        chunks = status["chunk_stats"]["chunks"]
        typer.echo("\n📦 Чанки:")
        typer.echo(f"   Всего: {chunks['total']}")
        typer.echo(f"   Завершено: {chunks['completed']}")
        typer.echo(f"   Успешно: {chunks['successful']}")
        typer.echo(f"   Провалено: {chunks['failed']}")
        typer.echo(f"   В обработке: {chunks['in_progress']}")
        typer.echo(f"   Прогресс: {chunks['completion_percentage']:.1f}%")

        # Статистика файлов
        files = status["chunk_stats"]["files"]
        typer.echo("\n📄 Файлы:")
        typer.echo(f"   Всего: {files['total']}")
        typer.echo(f"   Обработано: {files['processed']}")
        typer.echo(f"   Ошибок: {files['failed']}")
        typer.echo(f"   Осталось: {files['remaining']}")
        typer.echo(f"   Прогресс: {files['completion_percentage']:.1f}%")

        # Recovery статус
        recovery = status["recovery_status"]
        if recovery["recovery_needed"]:
            typer.echo("\n🔄 Требуется recovery:")
            for rec in recovery["recommendations"]:
                typer.echo(f"   • {rec}")
        else:
            typer.echo("\n✅ Recovery не требуется")

        # Session статус
        session = status["session_stats"]
        if session["start_time"]:
            typer.echo("\n🎯 Текущая сессия:")
            typer.echo(f"   Начало: {session['start_time']}")
            if session["end_time"]:
                typer.echo(f"   Завершение: {session['end_time']}")
            typer.echo(f"   Обработано чанков: {session.get('chunks_processed', 0)}")
            typer.echo(f"   Файлов: {session.get('files_processed', 0)}/{session.get('files_failed', 0)}")

        if detailed and status.get("errors"):
            typer.echo("\n🚨 Ошибки:")
            for error in status["errors"][:5]:  # Первые 5 ошибок
                typer.echo(f"   • {error}")

    except Exception as e:
        typer.echo(f"❌ Ошибка получения статуса: {e}")
        raise typer.Exit(1)


@app.command("recover")
def recover_processing(
    state_dir: Path = typer.Argument(..., help="Директория с состоянием"),
    force_retry_failed: bool = typer.Option(False, help="Принудительно retry неудачные чанки"),
    verbose: bool = typer.Option(False, help="Подробный вывод")
):
    """Восстанавливает прерванную обработку."""
    import logging

    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        # Создаем classifier
        classifier = ChunkedClassifier(state_dir)

        # Выполняем recovery
        typer.echo("🔄 Выполняется recovery...")

        # Получаем recovery engine
        recovery_result = classifier.recovery_engine.perform_recovery(
            force_retry_failed=force_retry_failed
        )

        # Выводим результаты
        typer.echo("✅ Recovery завершен:")
        typer.echo(f"   🔄 Восстановлено чанков: {recovery_result.recovered_chunks}")
        typer.echo(f"   ⏱️  Время recovery: {recovery_result.recovery_time_seconds:.1f}s")

        if recovery_result.errors:
            typer.echo(f"🚨 Ошибок: {len(recovery_result.errors)}")
            for error in recovery_result.errors[:3]:
                typer.echo(f"   • {error}")

        # Проверяем валидность после recovery
        validation = classifier.recovery_engine.validate_recovery()
        if validation["is_valid"]:
            typer.echo("✅ Состояние валидно после recovery")
        else:
            typer.echo("⚠️  Обнаружены проблемы после recovery:")
            for issue in validation["issues"]:
                typer.echo(f"   • {issue}")

    except Exception as e:
        typer.echo(f"❌ Ошибка recovery: {e}")
        raise typer.Exit(1)


@app.command("reset")
def reset_processing(
    state_dir: Path = typer.Argument(..., help="Директория с состоянием"),
    confirm: bool = typer.Option(False, help="Подтвердить сброс (ОБЯЗАТЕЛЬНО)")
):
    """Сбрасывает состояние обработки (emergency reset)."""
    if not confirm:
        typer.echo("❌ Для сброса требуется --confirm")
        typer.echo("⚠️  Это приведет к потере прогресса обработки!")
        raise typer.Exit(1)

    try:
        # Создаем classifier
        classifier = ChunkedClassifier(state_dir)

        # Emergency reset
        typer.echo("🚨 Выполняется emergency reset...")
        success = classifier.recovery_engine.emergency_reset(confirm=True)

        if success:
            typer.echo("✅ Emergency reset завершен")
            typer.echo("🔄 Все чанки сброшены в состояние PENDING")
        else:
            typer.echo("❌ Emergency reset не выполнен")

    except Exception as e:
        typer.echo(f"❌ Ошибка reset: {e}")
        raise typer.Exit(1)
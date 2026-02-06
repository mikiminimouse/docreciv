"""
Pipeline - полный прогон preprocessing (3 цикла подряд).
"""
import os
import typer
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..engine.classifier import Classifier
from ..engine.converter import Converter
from ..engine.extractor import Extractor
from ..engine.merger import Merger
from ..core.config import get_cycle_paths, init_directory_structure, get_data_paths, DATA_BASE_DIR

app = typer.Typer(help="Полный прогон preprocessing")


def _determine_base_dir_from_input(input_dir: Path) -> Path:
    """
    Определяет базовую директорию из input_dir.

    Если input_dir заканчивается на "Input", возвращает родительскую директорию.
    Например: "/path/to/date/Input" -> "/path/to/date"

    Args:
        input_dir: Входная директория

    Returns:
        Базовая директория для Processing/Merge/Exceptions
    """
    input_path = Path(input_dir)
    if input_path.name == "Input":
        return input_path.parent
    # Проверяем по частям пути (для случаев когда Input в середине пути)
    parts = input_path.parts
    for i, part in enumerate(parts):
        if part == "Input" and i > 0:
            return Path(*parts[:i])
    # По умолчанию возвращаем родительскую директорию
    return input_path.parent


@app.command("run")
def run(
    input_dir: Path = typer.Argument(..., help="Входная директория (Input)"),
    output_dir: Path = typer.Argument(..., help="Выходная директория (Ready2Docling)"),
    max_cycles: int = typer.Option(3, "--max-cycles", help="Максимальное количество циклов"),
    stop_on_exception: bool = typer.Option(
        False, "--stop-on-exception", help="Останавливаться при ошибке"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим проверки"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
    enable_mongo: bool = typer.Option(False, "--enable-mongo", help="Включить запись в MongoDB"),
    enable_tracker: bool = typer.Option(True, "--enable-tracker/--no-enable-tracker", help="Интеграция с PipelineTracker"),
    tracker_run_id: str = typer.Option(None, "--tracker-run-id", help="Существующий run_id для resume"),
    test_run_id: str = typer.Option(None, "--test-run-id", help="ID тестового запуска для детальных метрик"),
):
    """
    Запускает полный цикл preprocessing (3 цикла подряд).

    Выполняет: classifier → processing → merge для каждого цикла.

    При включённом --enable-mongo записывает метрики в MongoDB.
    При включённом --enable-tracker создаёт PipelineRun через Docreciv PipelineTracker.
    При указании --test-run-id создаёт детальную статистику для тестов.
    """
    if verbose:
        typer.echo(f"Запуск полного pipeline: {input_dir} -> {output_dir}")

    if dry_run:
        typer.echo("🔍 РЕЖИМ DRY RUN - изменения не будут применены")

    # Инициализируем MongoDB клиент если запрошено
    db_client = None
    pipeline_id = None
    tracker_run_id = tracker_run_id  # Локальная переменная для run_id из PipelineTracker
    metrics_collector = None  # Для детальных метрик тестовых запусков

    if enable_mongo:
        from ..core.database import get_database

        db_client = get_database()

        if db_client.is_connected():
            typer.echo("🗄️  MongoDB подключена - метрики будут записываться")
        else:
            typer.echo("⚠️  MongoDB недоступна - работа без записи метрик", err=True)
            db_client = None

    # Инициализируем PipelineTracker если запрошено
    if enable_tracker and db_client is not None and db_client.is_connected():
        try:
            from docreciv.pipeline.events import Stage, RunStatus

            # Если run_id не передан, создаём новый PipelineRun
            if tracker_run_id is None:
                tracker_run_id = db_client.create_pipeline_run(
                    batch_date=protocol_date if 'protocol_date' in locals() else datetime.now().strftime("%Y-%m-%d"),
                    stage=Stage.DOCPREP,
                    config={
                        "max_cycles": max_cycles,
                        "input_dir": str(input_dir),
                        "output_dir": str(output_dir),
                        "dry_run": dry_run
                    }
                )
                if tracker_run_id:
                    typer.echo(f"🔗 PipelineTracker run_id: {tracker_run_id}")
            else:
                typer.echo(f"🔗 Используем существующий run_id: {tracker_run_id}")

        except ImportError:
            typer.echo("⚠️  PipelineTracker недоступен - работа без трекинга", err=True)
        except Exception as e:
            typer.echo(f"⚠️  Ошибка инициализации PipelineTracker: {e}", err=True)

    # Инициализируем MetricsCollector для тестовых запусков
    if test_run_id:
        from ..core.metrics import MetricsCollector

        # Извлекаем имя базы данных из MONGODB_URI или используем умолчание
        import re
        mongo_uri = os.getenv("MONGODB_URI", "")
        if mongo_uri:
            # Извлекаем имя базы из URI
            match = re.search(r'/([^/?]+)$', mongo_uri)
            db_name = match.group(1) if match else None
        else:
            db_name = None

        metrics_collector = MetricsCollector(
            test_run_id=test_run_id,
            db_name=db_name,
        )

        if verbose and metrics_collector.is_connected():
            typer.echo(f"📊 MetricsCollector инициализирован: {test_run_id}")

    # Определяем дату протокола из input_dir или используем текущую
    protocol_date = datetime.now().strftime("%Y-%m-%d")
    if "/" in str(input_dir) or "\\" in str(input_dir):
        # Пытаемся извлечь дату из пути
        parts = Path(input_dir).parts
        for part in parts:
            if part and len(part) == 10 and part[4] == "-" and part[7] == "-":
                protocol_date = part
                break

    typer.echo(f"📅 Дата протокола: {protocol_date}")

    # КРИТИЧНО: Определяем base_dir из input_dir для правильной структуры
    # Все директории (Processing, Merge, Exceptions) должны быть рядом с Input
    base_dir = _determine_base_dir_from_input(input_dir)
    typer.echo(f"📁 Базовая директория: {base_dir}")

    # Устанавливаем DATA_BASE_DIR для текущего процесса
    # Это нужно чтобы get_data_paths и другие функции использовали правильный путь
    os.environ["DATA_BASE_DIR"] = str(base_dir)

    # Перезагружаем модуль config для применения новой DATA_BASE_DIR
    import importlib
    from ..core import config as config_module
    importlib.reload(config_module)

    # Обновляем ссылки на функции из перезагруженного модуля
    from ..core.config import get_data_paths as get_data_paths_updated, get_cycle_paths as get_cycle_paths_updated

    # Регистрируем pipeline run в MongoDB
    if db_client is not None and db_client.is_connected():
        pipeline_id = db_client.start_pipeline(
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            protocol_date=protocol_date,
            max_cycles=max_cycles,
        )
        typer.echo(f"📊 Pipeline ID: {pipeline_id}")

    # Инициализируем структуру директорий с правильной датой
    # Используем base_dir напрямую, без вложенной даты (так как дата уже в пути)
    init_directory_structure(base_dir=base_dir, date=None)

    # Устанавливаем base_dir в Classifier для правильного определения путей
    Classifier.set_base_dir(base_dir)

    classifier_engine = Classifier()
    converter_engine = Converter()
    extractor_engine = Extractor()
    merger_engine = Merger()

    # ★ UnitEvents: передаём tracker_run_id во все engine классы
    if tracker_run_id and db_client is not None:
        Classifier.set_tracker_run_id(tracker_run_id, db_client)
        Converter.set_tracker_run_id(tracker_run_id, db_client)
        Extractor.set_tracker_run_id(tracker_run_id, db_client)
        Merger.set_tracker_run_id(tracker_run_id, db_client)
        if verbose:
            typer.echo("✓ UnitEvents интеграция активирована")

    # Запускаем циклы
    for cycle_num in range(1, max_cycles + 1):
        typer.echo(f"\n{'='*60}")
        typer.echo(f"🔄 ЦИКЛ {cycle_num} из {max_cycles}")
        typer.echo(f"{'='*60}")

        try:
            # Используем cycle_run для полного цикла
            from ..cli.cycle import cycle_run
            
            cycle_input_dir = input_dir if cycle_num == 1 else None
            
            cycle_run(
                cycle_num=cycle_num,
                input_dir=cycle_input_dir,
                protocol_date=None,  # None, так как base_dir уже содержит правильную структуру
                dry_run=dry_run,
                verbose=verbose,
                pipeline_id=pipeline_id,
                enable_mongo=enable_mongo,
            )

        except Exception as e:
            msg = f"Ошибка в цикле {cycle_num}: {e}"
            if stop_on_exception:
                typer.echo(f"❌ {msg}", err=True)
                raise
            else:
                typer.echo(f"⚠️  {msg} - пропуск цикла", err=True)
                continue

    # Финальный merge из всех Merge_N в Ready2Docling
    typer.echo(f"\n{'='*60}")
    typer.echo("🏁 ФИНАЛЬНЫЙ MERGE в Ready2Docling")
    typer.echo(f"{'='*60}")

    # Получаем правильные пути для merge директорий
    # НОВАЯ СТРУКТУРА v2:
    # - Merge/Direct/ - для прямых файлов готовых к Docling (без обработки)
    # - Merge/Processing_N/ - для обработанных units в цикле N

    # ИСПРАВЛЕНО: Передаём base_dir явно чтобы использовать правильные пути
    # После reload config_module, DATA_BASE_DIR должна быть обновлена,
    # но передаём base_dir явно для надёжности
    data_paths = get_data_paths_updated(date=None, base_dir=base_dir)
    merge_dirs = []

    # Добавляем Merge/Direct для direct файлов из цикла 1
    merge_direct = data_paths["merge"] / "Direct"
    if merge_direct.exists():
        merge_dirs.append(merge_direct)

    # Добавляем все Merge/Processed_N (1, 2, 3)
    for cycle_num in range(1, max_cycles + 1):
        cycle_paths = get_cycle_paths_updated(
            cycle_num,
            data_paths["processing"],
            data_paths["merge"],
            data_paths["exceptions"]
        )
        if cycle_paths["merge"].exists():
             merge_dirs.append(cycle_paths["merge"])

    typer.echo(f"🔍 Источники для Merge: {[d.name for d in merge_dirs]}")
    
    # Получаем er_merge_base для обработки ошибок финального merge
    er_merge_base = data_paths.get("er_merge")
    
    try:
        result = merger_engine.collect_units(merge_dirs, output_dir, cycle=None, er_merge_base=er_merge_base)
        typer.echo(f"✅ Успешно обработано: {result['units_processed']} UNITs")
        
        if result.get("errors"):
            typer.echo(f"⚠️  Ошибок: {len(result['errors'])}", err=True)
            if verbose:
                for error in result["errors"][:10]:
                    typer.echo(f"  ❌ {error.get('unit_id', 'unknown')}: {error.get('error')}", err=True)
        
        # Валидация результата
        ready_units = list(output_dir.rglob("UNIT_*")) if output_dir.exists() else []
        typer.echo(f"📁 UNITs в Ready2Docling: {len(ready_units)}")

        # Обновляем метрики в MongoDB
        if db_client is not None and db_client.is_connected() and pipeline_id:
            db_client.update_pipeline_metrics(pipeline_id, {
                "units_total": len(ready_units) + len(result.get("errors", [])),
                "units_success": len(ready_units),
                "units_failed": len(result.get("errors", [])),
            })

        # Очистка только при успешном завершении и если не dry_run
        if not dry_run and result['units_processed'] > 0:
            _cleanup_intermediate_dirs(merge_dirs, data_paths, max_cycles, typer)

        # Завершаем pipeline в MongoDB (успешно)
        if db_client is not None and db_client.is_connected() and pipeline_id:
            db_client.end_pipeline(pipeline_id, status="completed")

        # Обновляем PipelineTracker (успешно)
        if tracker_run_id and db_client is not None and db_client.is_connected():
            try:
                from docreciv.pipeline.events import RunStatus

                ready_units_list = list(output_dir.rglob("UNIT_*")) if output_dir.exists() else []
                final_metrics = {
                    "units_total": len(ready_units_list) + len(result.get("errors", [])),
                    "units_success": len(ready_units_list),
                    "units_failed": len(result.get("errors", [])),
                    "cycles_completed": max_cycles
                }

                db_client.update_pipeline_run(
                    run_id=tracker_run_id,
                    status=RunStatus.COMPLETED,
                    metrics=final_metrics
                )
                typer.echo("✅ PipelineTracker обновлён: COMPLETED")
            except Exception as e:
                typer.echo(f"⚠️  Ошибка обновления PipelineTracker: {e}", err=True)

        # Завершаем MetricsCollector с финальной статистикой
        if metrics_collector is not None:
            from ..utils.paths import find_units

            ready_units_list = list(find_units(output_dir)) if output_dir.exists() else []
            units_with_files = 0

            for unit_path in ready_units_list:
                files = list(unit_path.glob("*"))
                data_files = [
                    f for f in files
                    if f.is_file() and f.name not in ["manifest.json", "audit.log.jsonl"]
                ]
                if data_files:
                    units_with_files += 1

            final_stats = {
                "total_units": len(ready_units_list),
                "units_with_files": units_with_files,
                "output_dir": str(output_dir),
            }

            metrics_collector.end_test_run(
                units_success=units_with_files,
                units_failed=len(ready_units_list) - units_with_files,
                units_total=len(ready_units_list),
                final_stats=final_stats,
            )

            if verbose:
                typer.echo(f"📊 MetricsCollector завершён: {metrics_collector.test_run_id}")

    except Exception as e:
        typer.echo(f"❌ Критическая ошибка при финальном merge: {e}", err=True)

        # Завершаем pipeline в MongoDB (с ошибкой)
        if db_client is not None and db_client.is_connected() and pipeline_id:
            db_client.end_pipeline(pipeline_id, status="failed", errors=[{"error": str(e)}])

        # Обновляем PipelineTracker (с ошибкой)
        if tracker_run_id and db_client is not None and db_client.is_connected():
            try:
                from docreciv.pipeline.events import RunStatus

                db_client.update_pipeline_run(
                    run_id=tracker_run_id,
                    status=RunStatus.FAILED,
                    error=str(e)
                )
                typer.echo("⚠️  PipelineTracker обновлён: FAILED")
            except Exception as tracker_err:
                typer.echo(f"⚠️  Ошибка обновления PipelineTracker: {tracker_err}", err=True)

        # Записываем ошибку в MetricsCollector
        if metrics_collector is not None:
            metrics_collector.record_error(
                unit_id="pipeline",
                operation_type="final_merge",
                error_category="pipeline_error",
                error_message=str(e),
            )
            metrics_collector.end_test_run(
                units_success=0,
                units_failed=0,
                units_total=0,
                final_stats={"error": str(e)},
            )

        raise


def _cleanup_intermediate_dirs(merge_dirs, data_paths, max_cycles, typer_instance):
    """Очищает промежуточные директории после успешной обработки.

    ИСПРАВЛЕНО: Сохраняет extension директории с готовыми файлами.
    Удаляет только служебные файлы и временные поддиректории.
    """
    import shutil

    typer_instance.echo("🧹 Очистка временных файлов...")

    # Белый список директорий которые НЕ удаляются (готовые результаты)
    PRESERVED_EXTENSIONS = {
        "docx", "pdf", "xlsx", "pptx", "html", "xml", "txt",
        "jpg", "jpeg", "png", "tiff", "tif", "bmp", "gif", "webp",
        "json", "Mixed", "Direct"
    }

    # Очистка Merge директорий — только служебные файлы и временные поддиректории
    for merge_dir in merge_dirs:
        if merge_dir.exists():
            try:
                for item in merge_dir.iterdir():
                    if item.is_file():
                        # Удаляем только служебные файлы
                        if item.name in ["audit.log.jsonl", ".DS_Store", "Thumbs.db"]:
                            item.unlink()
                    elif item.is_dir():
                        # НЕ удаляем директории с расширениями (готовые файлы)
                        if item.name in PRESERVED_EXTENSIONS:
                            continue
                        # Удаляем только временные поддиректории (Processed_N, Converted, Extracted и т.д.)
                        shutil.rmtree(item)
            except Exception as e:
                typer_instance.echo(f"  ⚠️  Ошибка очистки {merge_dir}: {e}", err=True)

    # Очистка Processing директорий — полностью
    processing_base = data_paths["processing"]
    for cycle_num in range(1, max_cycles + 1):
        cycle_processing_dir = processing_base / f"Processing_{cycle_num}"
        if cycle_processing_dir.exists():
            try:
                shutil.rmtree(cycle_processing_dir)
                cycle_processing_dir.mkdir()
            except Exception as e:
                typer_instance.echo(f"  ⚠️  Ошибка очистки {cycle_processing_dir}: {e}", err=True)

    typer_instance.echo("✅ Очистка завершена")


if __name__ == "__main__":
    app()



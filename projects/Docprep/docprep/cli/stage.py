"""
Stage - этап внутри цикла (classifier, pending, merge).
"""
import time
import logging
from collections import Counter
import typer
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

from ..engine.classifier import Classifier
from ..engine.merger import Merger
from ..core.unit_processor import process_directory_units
from ..core.config import get_processing_paths, get_cycle_paths, PROCESSING_DIR, MERGE_DIR
from ..core.exceptions import StateTransitionError
from ..utils.paths import find_all_units

logger = logging.getLogger(__name__)

app = typer.Typer(name="stage", help="Этап внутри цикла")


@app.command("classifier")
def stage_classifier(
    cycle: int = typer.Option(..., "--cycle", help="Номер цикла (1, 2, 3)"),
    input_dir: Path = typer.Option(..., "--input", help="Входная директория"),
    protocol_date: Optional[str] = typer.Option(None, "--date", help="Дата протокола (YYYY-MM-DD)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим имитации"),
    copy_mode: bool = typer.Option(False, "--copy", help="Копировать вместо перемещения (сохраняет исходные файлы)"),
    pipeline_id: Optional[str] = typer.Option(None, "--pipeline-id", help="ID pipeline для статистики"),
    enable_mongo: bool = typer.Option(False, "--enable-mongo", help="Включить запись в MongoDB"),
):
    """Прогнать classifier по всей директории."""
    if not input_dir.exists():
        typer.echo(f"❌ Директория не найдена: {input_dir}", err=True)
        raise typer.Exit(1)

    # protocol_date может быть None если base_dir уже содержит правильную структуру
    # ИСПРАВЛЕНО: Не устанавливаем текущую дату если protocol_date=None и base_dir установлен
    from ..engine.classifier import Classifier
    if protocol_date is None:
        # Если base_dir установлен через Classifier.set_base_dir(), не используем дату
        if Classifier._override_base_dir is None:
            protocol_date = datetime.now().strftime("%Y-%m-%d")
        # else: оставляем protocol_date как None
    elif not protocol_date:
        protocol_date = datetime.now().strftime("%Y-%m-%d")

    typer.echo(f"🔍 Classifier цикла {cycle}: {input_dir}")
    if copy_mode:
        typer.echo("📋 Режим копирования включен (исходные файлы сохраняются)")

    # Инициализируем MongoDB клиент если требуется
    db_client = None
    if enable_mongo:
        from ..core.database import get_database
        db_client = get_database()

    # Для сбора статистики по категориям
    categories_counter: Counter = Counter()
    extension_counter: Counter = Counter()
    unit_results: list = []

    start_time = time.time()
    classifier = Classifier()

    def process_unit(unit_path: Path) -> dict:
        """Обработка одного UNIT классификатором."""
        # copy_mode передается явно или автоматически определяется для Input директории
        result = classifier.classify_unit(unit_path, cycle, protocol_date, None, dry_run, copy_mode=copy_mode)

        # Собираем статистику
        category = result.get('category', 'unknown')
        categories_counter[category] += 1

        # Собираем расширение из результата
        if 'extension' in result:
            extension_counter[result['extension']] += 1

        unit_results.append({
            "unit_id": unit_path.name,
            "category": category,
            "status": result.get('status', 'success'),
        })

        if verbose:
            typer.echo(f"  ✓ {unit_path.name}: {category}")
        return result

    results = process_directory_units(
        source_dir=input_dir,
        processor_func=process_unit,
        dry_run=dry_run,
    )

    processing_time_ms = int((time.time() - start_time) * 1000)

    typer.echo(f"\n✅ Обработано UNIT: {results['units_processed']}")
    if results['units_failed'] > 0:
        typer.echo(f"❌ Ошибок: {results['units_failed']}", err=True)
        if verbose:
            for error in results['errors']:
                typer.echo(f"  - {error['unit_id']}: {error['error']}", err=True)

    # Показываем статистику по категориям
    if categories_counter:
        typer.echo("\n📊 Распределение по категориям:")
        for category, count in categories_counter.most_common():
            typer.echo(f"   {category}: {count}")

    # Записываем статистику в MongoDB
    if enable_mongo and db_client and pipeline_id and not dry_run:
        try:
            stage_stats = {
                "total_units": results['units_processed'],
                "units_failed": results['units_failed'],
                "by_category": dict(categories_counter),
                "by_extension": dict(extension_counter),
                "processing_time_ms": processing_time_ms,
                "copy_mode": copy_mode,
            }
            db_client.write_stage_stats(
                pipeline_id=pipeline_id,
                cycle=cycle,
                stage="classifier",
                stats=stage_stats,
            )

            # Записываем trace для каждого UNIT
            for unit_res in unit_results:
                db_client.write_unit_trace(
                    unit_id=unit_res["unit_id"],
                    pipeline_id=pipeline_id,
                    cycle=cycle,
                    stage="classifier",
                    operation="classify",
                    duration_ms=0,  # Не замеряем для каждого UNIT отдельно
                    status=unit_res["status"],
                    metadata={"category": unit_res["category"]},
                )

            if verbose:
                typer.echo(f"\n💾 Статистика записана в MongoDB (pipeline_id: {pipeline_id})")
        except Exception as e:
            logger.warning(f"Failed to write stats to MongoDB: {e}")
            typer.echo(f"⚠️  Не удалось записать статистику: {e}", err=True)


@app.command("pending")
def stage_pending(
    cycle: int = typer.Option(..., "--cycle", help="Номер цикла (1, 2, 3)"),
    pending_dir: Path = typer.Option(..., "--pending", help="Processing директория"),
    protocol_date: Optional[str] = typer.Option(None, "--date", help="Дата протокола (YYYY-MM-DD)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим имитации"),
    pipeline_id: Optional[str] = typer.Option(None, "--pipeline-id", help="ID pipeline для статистики"),
    enable_mongo: bool = typer.Option(False, "--enable-mongo", help="Включить запись в MongoDB"),
):
    """
    Обработать все Processing_N.

    Обрабатывает все UNIT в поддиректориях Convert, Extract, Normalize.
    Примечание: Direct файлы не проходят через Processing, они идут напрямую в Merge/Direct/ после классификации.
    """
    if not pending_dir.exists():
        typer.echo(f"❌ Директория не найдена: {pending_dir}", err=True)
        raise typer.Exit(1)

    # ИСПРАВЛЕНО: Не устанавливаем текущую дату если base_dir уже установлен
    from ..engine.classifier import Classifier
    if not protocol_date:
        if Classifier._override_base_dir is None:
            protocol_date = datetime.now().strftime("%Y-%m-%d")

    typer.echo(f"⚙️  Обработка Processing_{cycle}: {pending_dir}")

    # Находим все UNIT в поддиректориях
    units = find_all_units(pending_dir)

    if not units:
        typer.echo("ℹ️  UNIT не найдены")
        return

    typer.echo(f"📦 Найдено UNIT: {len(units)}")

    # Определяем поддиректории Processing
    processing_paths = get_processing_paths(cycle, pending_dir.parent if pending_dir.name.startswith("Processing_") else pending_dir)

    # Обработка Convert
    convert_dir = processing_paths["Convert"]
    if convert_dir.exists():
        convert_units = find_all_units(convert_dir)
        if convert_units:
            typer.echo(f"\n  🔄 Конвертация ({len(convert_units)} UNIT)")
            from ..cli.substage import substage_convert_run
            try:
                substage_convert_run(
                    input_dir=convert_dir,
                    cycle=cycle,
                    protocol_date=protocol_date,
                    verbose=verbose,
                    dry_run=dry_run,
                    pipeline_id=pipeline_id,
                    enable_mongo=enable_mongo,
                )
            except Exception as e:
                typer.echo(f"  ⚠️  Ошибка конвертации: {e}", err=True)

    # Обработка Extract
    extract_dir = processing_paths["Extract"]
    if extract_dir.exists():
        extract_units = find_all_units(extract_dir)
        if extract_units:
            typer.echo(f"\n  📦 Разархивация ({len(extract_units)} UNIT)")
            from ..cli.substage import substage_extract_run
            try:
                substage_extract_run(
                    input_dir=extract_dir,
                    cycle=cycle,
                    protocol_date=protocol_date,
                    verbose=verbose,
                    dry_run=dry_run,
                    pipeline_id=pipeline_id,
                    enable_mongo=enable_mongo,
                )
            except Exception as e:
                typer.echo(f"  ⚠️  Ошибка разархивации: {e}", err=True)

    # Обработка Normalize
    normalize_dir = processing_paths["Normalize"]
    if normalize_dir.exists():
        normalize_units = find_all_units(normalize_dir)
        if normalize_units:
            typer.echo(f"\n  ✨ Нормализация ({len(normalize_units)} UNIT)")
            from ..cli.substage import substage_normalize_full
            try:
                substage_normalize_full(
                    input_dir=normalize_dir,
                    cycle=cycle,
                    protocol_date=protocol_date,
                    verbose=verbose,
                    dry_run=dry_run,
                    pipeline_id=pipeline_id,
                    enable_mongo=enable_mongo,
                )
            except Exception as e:
                typer.echo(f"  ⚠️  Ошибка нормализации: {e}", err=True)

    typer.echo(f"\n✅ Обработка Processing_{cycle} завершена")


@app.command("merge")
def stage_merge(
    cycle: int = typer.Option(..., "--cycle", help="Номер цикла (1, 2, 3)"),
    source_dir: Path = typer.Option(..., "--source", help="Исходная директория"),
    target_dir: Optional[Path] = typer.Option(None, "--target", help="Целевая директория (Ready2Docling)"),
    protocol_date: Optional[str] = typer.Option(None, "--date", help="Дата протокола (YYYY-MM-DD)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим имитации"),
):
    """Перенести готовые юниты в Merge_N."""
    if not source_dir.exists():
        typer.echo(f"❌ Директория не найдена: {source_dir}", err=True)
        raise typer.Exit(1)

    # ИСПРАВЛЕНО: Не устанавливаем текущую дату если base_dir уже установлен
    from ..engine.classifier import Classifier
    if not protocol_date:
        if Classifier._override_base_dir is None:
            protocol_date = datetime.now().strftime("%Y-%m-%d")

    # Определяем целевую директорию Merge с использованием get_data_paths
    from ..core.config import get_data_paths
    # ИСПРАВЛЕНО: Передаём base_dir для правильных путей
    if protocol_date:
        data_paths = get_data_paths(protocol_date, base_dir=Classifier._override_base_dir)
        merge_base = data_paths["merge"]
    else:
        merge_base = MERGE_DIR
    cycle_paths = get_cycle_paths(cycle, None, merge_base, None)
    
    # Определяем категорию Merge на основе исходной директории
    # НОВАЯ СТРУКТУРА v2:
    # - Merge/Direct: Direct файлы (уже там после классификации, merge не нужен)
    # - Merge/Processed_N: Converted, Extracted, Normalized (из Processing_N после обработки)

    # Определяем категорию по пути
    merge_category = None
    source_str = str(source_dir)
    if "Convert" in source_str:
        merge_category = "Converted"
    elif "Extract" in source_str:
        merge_category = "Extracted"
    elif "Normalize" in source_str:
        merge_category = "Normalized"
    elif "Direct" in source_str:
        # Direct файлы НЕ должны попадать в merge из Processing_N
        # Они идут напрямую в Merge/Direct/ после классификации в цикле 1
        # Если кто-то пытается сделать merge Direct файлов, это ошибка
        typer.echo(f"⚠️  Direct файлы не должны попадать в merge из Processing_N. Они идут напрямую в Merge/Direct/", err=True)
        return
    else:
        # Если не удалось определить, пытаемся из manifest
        merge_category = "Converted"  # Fallback
    
    if not target_dir:
        if merge_category:
            target_dir = cycle_paths["merge"] / merge_category
        else:
            typer.echo(f"❌ Не удалось определить категорию для {source_dir}", err=True)
            raise typer.Exit(1)

    typer.echo(f"🔀 Merge цикла {cycle}: {source_dir} -> {target_dir} (категория: {merge_category})")
    
    # Находим все UNIT в source_dir
    units = find_all_units(source_dir)
    
    if not units:
        typer.echo("ℹ️  UNIT не найдены")
        return

    typer.echo(f"📦 Найдено UNIT: {len(units)}")
    
    # Перемещаем UNIT в Merge_N
    from ..core.unit_processor import move_unit_to_target
    from ..core.state_machine import UnitState
    from ..core.manifest import load_manifest
    
    if not target_dir.exists():
        target_dir.mkdir(parents=True, exist_ok=True)
    
    moved_count = 0
    errors = []
    
    for unit_path in units:
        try:
            # Определяем расширение для сортировки
            from ..core.unit_processor import determine_unit_extension
            extension = determine_unit_extension(unit_path)
            
            # Пытаемся определить категорию из manifest для точности
            actual_category = merge_category
            manifest = None
            try:
                manifest = load_manifest(unit_path)
                operations = manifest.get("processing", {}).get("operations", [])
                
                # ВАЖНО: Проверяем, что операция действительно выполнена
                # Для Convert - проверяем наличие операции convert
                if merge_category == "Converted":
                    has_convert = any(
                        op.get("type") == "convert"
                        for op in operations
                    )
                    if not has_convert:
                        # UNIT не был конвертирован - пропускаем merge
                        typer.echo(f"  ⚠️  {unit_path.name}: Не был конвертирован, пропускаем merge", err=True)
                        continue
                
                # Для Extract - проверяем наличие операции extract
                elif merge_category == "Extracted":
                    has_extract = any(
                        op.get("type") == "extract"
                        for op in operations
                    )
                    if not has_extract:
                        # UNIT не был извлечен - пропускаем merge
                        typer.echo(f"  ⚠️  {unit_path.name}: Не был извлечен, пропускаем merge", err=True)
                        continue
                
                # Для Normalize - проверяем наличие операции normalize
                elif merge_category == "Normalized":
                    has_normalize = any(
                        op.get("type") == "normalize"
                        for op in operations
                    )
                    if not has_normalize:
                        # UNIT не был нормализован - пропускаем merge
                        typer.echo(f"  ⚠️  {unit_path.name}: Не был нормализован, пропускаем merge", err=True)
                        continue
                
                # Ищем последнюю операцию для определения категории
                for op in reversed(operations):
                    op_type = op.get("type")
                    if op_type == "convert":
                        actual_category = "Converted"
                        break
                    elif op_type == "extract":
                        actual_category = "Extracted"
                        break
                    elif op_type == "normalize":
                        actual_category = "Normalized"
                        break
            except Exception as e:
                typer.echo(f"  ⚠️  {unit_path.name}: Ошибка загрузки manifest: {e}", err=True)
                continue
            
            # Обновляем target_dir если категория изменилась
            if actual_category != merge_category:
                target_dir_for_unit = cycle_paths["merge"] / actual_category
            else:
                target_dir_for_unit = target_dir
            
            # Перемещаем UNIT в Merge_N с учетом расширения
            target_unit_path = move_unit_to_target(
                unit_dir=unit_path,
                target_base_dir=target_dir_for_unit,
                extension=extension,
                dry_run=dry_run,
            )
            
            # Определяем правильное состояние для merge на основе текущего состояния UNIT
            # Если UNIT находится в Processing_N, значит он уже был обработан
            # и должен быть в состоянии CLASSIFIED_2 или CLASSIFIED_3
            if not dry_run:
                from ..core.unit_processor import update_unit_state
                from ..core.state_machine import UnitStateMachine, UnitState
                
                manifest_path = target_unit_path / "manifest.json"
                merge_state = None
                
                if manifest_path.exists():
                    try:
                        state_machine = UnitStateMachine(target_unit_path.name, manifest_path)
                        current_state = state_machine.get_current_state()
                        
                        # Определяем целевое состояние на основе текущего состояния
                        # UNIT в Processing_N после обработки должны перейти в MERGED_PROCESSED
                        if current_state == UnitState.CLASSIFIED_2:
                            # UNIT из цикла 2 после обработки переходит в MERGED_PROCESSED
                            merge_state = UnitState.MERGED_PROCESSED
                        elif current_state == UnitState.CLASSIFIED_3:
                            # UNIT из цикла 3 переходит в MERGED_PROCESSED
                            merge_state = UnitState.MERGED_PROCESSED
                        elif current_state == UnitState.MERGED_PROCESSED:
                            # UNIT уже в MERGED_PROCESSED - это нормально, пропускаем обновление
                            merge_state = None
                        elif current_state == UnitState.PENDING_CONVERT:
                            # UNIT в PENDING_CONVERT - сначала переводим в CLASSIFIED_2
                            if state_machine.can_transition_to(UnitState.CLASSIFIED_2):
                                update_unit_state(
                                    unit_path=target_unit_path,
                                    new_state=UnitState.CLASSIFIED_2,
                                    cycle=cycle,
                                    operation={
                                        "type": "merge",
                                        "category": actual_category,
                                        "cycle": cycle,
                                        "transition": "PENDING_CONVERT -> CLASSIFIED_2",
                                    },
                                )
                                # Теперь переходим в MERGED_PROCESSED
                                state_machine = UnitStateMachine(target_unit_path.name, manifest_path)
                                merge_state = UnitState.MERGED_PROCESSED
                            else:
                                typer.echo(f"  ⚠️  {unit_path.name}: Cannot transition from PENDING_CONVERT to CLASSIFIED_2", err=True)
                                continue
                        elif current_state == UnitState.PENDING_EXTRACT:
                            # UNIT в PENDING_EXTRACT - сначала переводим в CLASSIFIED_2
                            if state_machine.can_transition_to(UnitState.CLASSIFIED_2):
                                update_unit_state(
                                    unit_path=target_unit_path,
                                    new_state=UnitState.CLASSIFIED_2,
                                    cycle=cycle,
                                    operation={
                                        "type": "merge",
                                        "category": actual_category,
                                        "cycle": cycle,
                                        "transition": "PENDING_EXTRACT -> CLASSIFIED_2",
                                    },
                                )
                                # Теперь переходим в MERGED_PROCESSED
                                state_machine = UnitStateMachine(target_unit_path.name, manifest_path)
                                merge_state = UnitState.MERGED_PROCESSED
                            else:
                                typer.echo(f"  ⚠️  {unit_path.name}: Cannot transition from PENDING_EXTRACT to CLASSIFIED_2", err=True)
                                continue
                        elif current_state == UnitState.PENDING_NORMALIZE:
                            # UNIT в PENDING_NORMALIZE - сначала переводим в CLASSIFIED_2
                            if state_machine.can_transition_to(UnitState.CLASSIFIED_2):
                                update_unit_state(
                                    unit_path=target_unit_path,
                                    new_state=UnitState.CLASSIFIED_2,
                                    cycle=cycle,
                                    operation={
                                        "type": "merge",
                                        "category": actual_category,
                                        "cycle": cycle,
                                        "transition": "PENDING_NORMALIZE -> CLASSIFIED_2",
                                    },
                                )
                                # Теперь переходим в MERGED_PROCESSED
                                state_machine = UnitStateMachine(target_unit_path.name, manifest_path)
                                merge_state = UnitState.MERGED_PROCESSED
                            else:
                                typer.echo(f"  ⚠️  {unit_path.name}: Cannot transition from PENDING_NORMALIZE to CLASSIFIED_2", err=True)
                                continue
                        elif current_state == UnitState.CLASSIFIED_1:
                            # UNIT в CLASSIFIED_1 не должен попадать в merge из Processing_N
                            # Это ошибка - пропускаем
                            typer.echo(f"  ⚠️  {unit_path.name}: UNIT в CLASSIFIED_1 не должен быть в Processing_N, пропускаем", err=True)
                            continue
                        else:
                            # Для других состояний пытаемся перейти в MERGED_PROCESSED, если разрешено
                            if state_machine.can_transition_to(UnitState.MERGED_PROCESSED):
                                merge_state = UnitState.MERGED_PROCESSED
                            else:
                                typer.echo(f"  ⚠️  {unit_path.name}: UNIT в состоянии {current_state.value} не может перейти в MERGED_PROCESSED, пропускаем", err=True)
                                continue
                    except Exception as e:
                        logger.warning(f"Failed to check state for {unit_path.name}: {e}")
                        typer.echo(f"  ⚠️  {unit_path.name}: Ошибка проверки состояния: {e}", err=True)
                        continue
                else:
                    # Нет manifest - пропускаем
                    typer.echo(f"  ⚠️  {unit_path.name}: Нет manifest.json, пропускаем", err=True)
                    continue
                
                # Обновляем состояние на merge_state
                if merge_state:
                    try:
                        # Перезагружаем state machine после возможных изменений
                        state_machine = UnitStateMachine(target_unit_path.name, manifest_path)
                        if state_machine.can_transition_to(merge_state):
                            update_unit_state(
                                unit_path=target_unit_path,
                                new_state=merge_state,
                                cycle=cycle,
                                operation={
                                    "type": "merge",
                                    "category": actual_category,
                                    "cycle": cycle,
                                },
                            )
                        else:
                            typer.echo(f"  ⚠️  {unit_path.name}: Cannot transition to {merge_state.value}, пропускаем", err=True)
                            continue
                    except StateTransitionError as e:
                        # Если переход не разрешен, логируем и пропускаем
                        logger.warning(f"Failed to update state for {unit_path.name}: {e}")
                        typer.echo(f"  ⚠️  {unit_path.name}: {e}", err=True)
                        continue
                    except Exception as e:
                        logger.warning(f"Failed to update state for {unit_path.name}: {e}")
                        typer.echo(f"  ⚠️  {unit_path.name}: Ошибка обновления состояния: {e}", err=True)
                        continue
            
            moved_count += 1
            if verbose:
                typer.echo(f"  ✓ {unit_path.name} -> {target_unit_path}")
        except Exception as e:
            errors.append({"unit_id": unit_path.name, "error": str(e)})
            typer.echo(f"  ❌ {unit_path.name}: {e}", err=True)
    
    typer.echo(f"\n✅ Перемещено UNIT: {moved_count}")
    if errors:
        typer.echo(f"❌ Ошибок: {len(errors)}", err=True)
        if verbose:
            for error in errors:
                typer.echo(f"  - {error['unit_id']}: {error['error']}", err=True)


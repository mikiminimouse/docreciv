"""
Cycle - управление отдельным циклом (1, 2, 3).
"""
import typer
from pathlib import Path
from typing import Optional
from datetime import datetime

from ..engine.classifier import Classifier
from ..engine.converter import Converter
from ..engine.extractor import Extractor
from ..engine.normalizers import NameNormalizer, ExtensionNormalizer
from ..core.config import get_cycle_paths, get_processing_paths, get_data_paths
from ..core.unit_processor import process_directory_units
from ..utils.paths import find_all_units

app = typer.Typer(name="cycle", help="Управление отдельным циклом")


@app.command("run")
def cycle_run(
    cycle_num: int = typer.Argument(..., help="Номер цикла (1, 2, 3)"),
    input_dir: Optional[Path] = typer.Option(None, "--input", help="Входная директория"),
    pending_dir: Optional[Path] = typer.Option(None, "--pending", help="Pending директория"),
    merge_dir: Optional[Path] = typer.Option(None, "--merge", help="Merge директория"),
    protocol_date: Optional[str] = typer.Option(None, "--date", help="Дата протокола (YYYY-MM-DD)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим проверки"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
    pipeline_id: Optional[str] = typer.Option(None, "--pipeline-id", help="ID pipeline для статистики"),
    enable_mongo: bool = typer.Option(False, "--enable-mongo", help="Включить запись в MongoDB"),
):
    """Запускает полный цикл обработки: classify → pending → merge."""
    if cycle_num < 1 or cycle_num > 3:
        typer.echo("❌ Цикл должен быть 1, 2 или 3", err=True)
        raise typer.Exit(1)

    # Проверяем, установлен ли override_base_dir в Classifier
    from ..engine.classifier import Classifier
    use_base_dir = Classifier._override_base_dir

    # ИСПРАВЛЕНО: Если base_dir установлен через pipeline, не добавляем дату к пути
    # ВАЖНО: Даже если protocol_date=None, не должны создавать новую директорию с текущей датой!
    if use_base_dir is not None:
        protocol_date_for_paths = None  # Не используем дату, base_dir уже содержит структуру
    elif protocol_date:
        protocol_date_for_paths = protocol_date
    else:
        # ИСПРАВЛЕНО: Если base_dir не установлен и protocol_date не указан,
        # возвращаем None чтобы использовать глобальные пути (не создавать новую дату!)
        protocol_date_for_paths = None

    typer.echo(f"🔄 Запуск цикла {cycle_num}")

    # ИСПРАВЛЕНО: Отладочный вывод для диагностики проблемы с дублирующей датой
    if verbose:
        typer.echo(f"  [DEBUG] use_base_dir: {use_base_dir}")
        typer.echo(f"  [DEBUG] protocol_date_for_paths: {protocol_date_for_paths}")

    # Определяем пути с использованием get_data_paths для правильной структуры
    data_paths = get_data_paths(protocol_date_for_paths, base_dir=use_base_dir)
    processing_base = data_paths["processing"]
    merge_base = data_paths["merge"]
    exceptions_base = data_paths["exceptions"]

    # ИСПРАВЛЕНО: Отладочный вывод для диагностики путей
    if verbose:
        typer.echo(f"  [DEBUG] data_paths['merge']: {data_paths['merge']}")
    
    cycle_paths = get_cycle_paths(cycle_num, processing_base, merge_base, exceptions_base)
    
    if not input_dir:
        if cycle_num == 1:
            input_dir = data_paths["input"]
        else:
            # Для циклов 2 и 3 входными данными являются результаты Merge предыдущего цикла
            prev_cycle_paths = get_cycle_paths(cycle_num - 1, processing_base, merge_base, exceptions_base)
            input_dir = prev_cycle_paths["merge"]
    
    if not pending_dir:
        pending_dir = cycle_paths["processing"]
    
    if not merge_dir:
        merge_dir = cycle_paths["merge"]
    
    # Для merge нужно передавать source_dir из Processing_N (после обработки)
    # В цикле 1 после обработки UNIT находятся в Processing_1/Convert, Extract, Normalize
    # Нужно собрать их и переместить в Merge_1/Converted, Extracted, Normalized
    # Для этого используем pending_dir как source_dir для merge

    # 1. Классификация
    typer.echo(f"\n📋 Шаг 1: Классификация")
    from ..cli.stage import stage_classifier
    try:
        stage_classifier(
            cycle=cycle_num,
            input_dir=input_dir,
            protocol_date=protocol_date_for_paths,  # Используем protocol_date_for_paths для путей
            verbose=verbose,
            dry_run=dry_run,
            pipeline_id=pipeline_id,
            enable_mongo=enable_mongo,
        )
    except Exception as e:
        typer.echo(f"❌ Ошибка классификации: {e}", err=True)
        if dry_run:
            raise

    # 2. Обработка Processing
    typer.echo(f"\n⚙️  Шаг 2: Обработка Processing_{cycle_num}")
    from ..cli.substage import (
        substage_convert_run,
        substage_extract_run,
        substage_normalize_full,
    )

    # Используем правильный processing_base из data_paths
    processing_paths = get_processing_paths(cycle_num, processing_base)

    # Обработка Convert
    convert_dir = processing_paths["Convert"]
    if convert_dir.exists() and find_all_units(convert_dir):
        typer.echo(f"  🔄 Конвертация: {convert_dir}")
        try:
            substage_convert_run(
                input_dir=convert_dir,
                cycle=cycle_num,
                protocol_date=protocol_date_for_paths,
                verbose=verbose,
                dry_run=dry_run,
                pipeline_id=pipeline_id,
                enable_mongo=enable_mongo,
            )
        except Exception as e:
            typer.echo(f"  ⚠️  Ошибка конвертации: {e}", err=True)

    # Обработка Extract
    extract_dir = processing_paths["Extract"]
    if extract_dir.exists() and find_all_units(extract_dir):
        typer.echo(f"  📦 Разархивация: {extract_dir}")
        try:
            substage_extract_run(
                input_dir=extract_dir,
                cycle=cycle_num,
                protocol_date=protocol_date_for_paths,
                verbose=verbose,
                dry_run=dry_run,
                pipeline_id=pipeline_id,
                enable_mongo=enable_mongo,
            )
        except Exception as e:
            typer.echo(f"  ⚠️  Ошибка разархивации: {e}", err=True)

    # Обработка Normalize
    normalize_dir = processing_paths["Normalize"]
    if normalize_dir.exists() and find_all_units(normalize_dir):
        typer.echo(f"  ✨ Нормализация: {normalize_dir}")
        try:
            substage_normalize_full(
                input_dir=normalize_dir,
                cycle=cycle_num,
                protocol_date=protocol_date_for_paths,
                verbose=verbose,
                dry_run=dry_run,
                pipeline_id=pipeline_id,
                enable_mongo=enable_mongo,
            )
        except Exception as e:
            typer.echo(f"  ⚠️  Ошибка нормализации: {e}", err=True)

    # 3. Merge - перемещаем обработанные UNIT из Processing_N в Merge_N
    # Для каждого типа обработки (Convert, Extract, Normalize) перемещаем в соответствующий Merge_N
    typer.echo(f"\n🔀 Шаг 3: Merge в Merge_{cycle_num}")
    from ..cli.stage import stage_merge
    
    # Используем уже полученные processing_paths из шага 2
    
    # Merge для каждого типа обработки
    merge_categories = {
        "Convert": "Converted",
        "Extract": "Extracted", 
        "Normalize": "Normalized",
    }
    
    for processing_category, merge_category in merge_categories.items():
        source_processing_dir = processing_paths[processing_category]
        if source_processing_dir.exists():
            units = find_all_units(source_processing_dir)
            if units:
                typer.echo(f"  🔀 Merge {processing_category} -> {merge_category}")
                try:
                    # Определяем целевую директорию для merge
                    target_merge_dir = cycle_paths["merge"] / merge_category
                    stage_merge(
                        cycle=cycle_num,
                        source_dir=source_processing_dir,
                        target_dir=target_merge_dir,
                        protocol_date=protocol_date_for_paths,
                        verbose=verbose,
                        dry_run=dry_run,
                    )
                except Exception as e:
                    typer.echo(f"  ⚠️  Ошибка merge {processing_category}: {e}", err=True)
                    if dry_run:
                        raise

    typer.echo(f"\n✅ Цикл {cycle_num} завершен")


@app.command("classify")
def cycle_classify(
    cycle_num: int = typer.Argument(..., help="Номер цикла (1, 2, 3)"),
    input_dir: Path = typer.Option(..., "--input", help="Входная директория"),
    protocol_date: Optional[str] = typer.Option(None, "--date", help="Дата протокола (YYYY-MM-DD)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим проверки"),
):
    """Выполняет только классификацию цикла."""
    if cycle_num < 1 or cycle_num > 3:
        typer.echo("❌ Цикл должен быть 1, 2 или 3", err=True)
        raise typer.Exit(1)

    from ..cli.stage import stage_classifier
    stage_classifier(
        cycle=cycle_num,
        input_dir=input_dir,
        protocol_date=protocol_date,
        verbose=verbose,
        dry_run=dry_run,
    )


@app.command("process")
def cycle_process(
    cycle_num: int = typer.Argument(..., help="Номер цикла (1, 2, 3)"),
    pending_dir: Path = typer.Option(..., "--pending", help="Processing директория"),
    protocol_date: Optional[str] = typer.Option(None, "--date", help="Дата протокола (YYYY-MM-DD)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим проверки"),
):
    """Выполняет только обработку Processing директории."""
    if cycle_num < 1 or cycle_num > 3:
        typer.echo("❌ Цикл должен быть 1, 2 или 3", err=True)
        raise typer.Exit(1)

    from ..cli.stage import stage_pending
    stage_pending(
        cycle=cycle_num,
        pending_dir=pending_dir,
        protocol_date=protocol_date,
        verbose=verbose,
        dry_run=dry_run,
    )


if __name__ == "__main__":
    app()


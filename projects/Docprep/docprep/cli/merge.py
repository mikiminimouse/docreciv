"""
Merge - ручной контроль merge операций.
"""
import typer
from pathlib import Path
from typing import Optional

from ..engine.merger import Merger
from ..core.config import get_cycle_paths

app = typer.Typer(name="merge", help="Управление merge операциями")


@app.command("collect")
def merge_collect(
    source_dir: Path = typer.Option(..., "--source", help="Исходная директория"),
    target_dir: Path = typer.Option(..., "--target", help="Целевая директория"),
    cycle: Optional[int] = typer.Option(None, "--cycle", help="Номер цикла"),
    from_all: bool = typer.Option(False, "--from-all", help="Собрать из всех Merge"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
):
    """
    Собирает UNIT из источников в целевую директорию.

    При --from-all собирает из всех Merge_1, Merge_2, Merge_3.
    """
    merger = Merger()

    # Определяем ErMerge базовую директорию
    # ИСПРАВЛЕНО: Передаём base_dir от Classifier для правильных путей
    from ..core.config import get_data_paths
    from ..engine.classifier import Classifier
    data_paths = get_data_paths(base_dir=Classifier._override_base_dir)
    er_merge_base = data_paths.get("er_merge")

    if from_all:
        # Собираем из всех Merge_N
        source_dirs = []
        processing_dir = source_dir.parent
        for cycle_num in range(1, 4):
            cycle_paths = get_cycle_paths(cycle_num, processing_dir)
            source_dirs.append(cycle_paths["merge"])
        typer.echo(f"Сборка из всех Merge: {target_dir}")
    else:
        source_dirs = [source_dir]
        typer.echo(f"Сборка: {source_dir} -> {target_dir}")

    result = merger.collect_units(source_dirs, target_dir, cycle, er_merge_base)
    typer.echo(f"Обработано UNIT: {result['units_processed']}")


@app.command("to-docling")
def merge_to_docling(
    source_dirs: list[Path] = typer.Argument(..., help="Исходные директории Merge/Direct/"),
    target_dir: Path = typer.Option(..., "--target", "-t", help="Целевая Ready2Docling директория"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
):
    """
    Финальный merge в Ready2Docling с сортировкой по типам файлов.

    Сортирует UNIT по поддиректориям (pdf/text, pdf/scan, docx, html, Mixed и т.д.)
    на основе типа файлов и needs_ocr из manifest.

    Пример:
        docprep merge to-docling Data/Merge/Direct/pdf Data/Merge/Direct/docx --target Data/Ready2Docling
    """
    merger = Merger()

    if verbose:
        typer.echo(f"Источники: {[str(d) for d in source_dirs]}")
        typer.echo(f"Цель: {target_dir}")

    result = merger.collect_to_ready2docling(source_dirs, target_dir)

    typer.echo(f"✅ Обработано UNIT: {result['units_processed']}")

    if result.get("by_type"):
        typer.echo("\nПо типам:")
        for type_name, count in sorted(result["by_type"].items()):
            typer.echo(f"  {type_name}: {count}")


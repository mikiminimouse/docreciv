"""
Интеграционный тест для комплексного тестирования всех циклов обработки final_receiver.

Запускает все 3 цикла обработки с сбором метрик на каждом этапе.

Перенесено из final_preprocessing/run_final_testing.py для интеграции в общую систему тестов.
"""
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

# Добавляем путь к модулю final_preprocessing (теперь receiver внутри final_preprocessing)
final_preprocessing_path = Path(__file__).parent.parent.parent  # final_preprocessing
sys.path.insert(0, str(final_preprocessing_path))

from docprep.core.config import DATA_BASE_DIR, init_directory_structure
from docprep.engine.classifier import Classifier
from docprep.engine.converter import Converter
from docprep.engine.extractor import Extractor
from docprep.engine.normalizers.extension import ExtensionNormalizer
from docprep.engine.merger import Merger
from docprep.core.unit_processor import find_all_units

app = typer.Typer()
console = Console()


def collect_metrics(cycle: int, date: str) -> Dict[str, Any]:
    """Собирает метрики для указанного цикла."""
    base_dir = DATA_BASE_DIR / date
    
    metrics = {
        "cycle": cycle,
        "processing": {},
        "merge": {},
        "exceptions": {},
    }
    
    # Processing
    proc_dir = base_dir / "Processing" / f"Processing_{cycle}"
    if proc_dir.exists():
        for category in ["Convert", "Extract", "Normalize"]:
            cat_dir = proc_dir / category
            if cat_dir.exists():
                units = list(cat_dir.rglob("UNIT_*"))
                unit_count = len([d for d in units if d.is_dir()])
                metrics["processing"][category.lower()] = unit_count
    
    # Merge
    merge_dir = base_dir / "Merge" / f"Merge_{cycle}"
    if merge_dir.exists():
        if cycle == 0:
            # Merge_0 имеет только Direct
            direct_dir = merge_dir / "Direct"
            if direct_dir.exists():
                units = list(direct_dir.rglob("UNIT_*"))
                unit_count = len([d for d in units if d.is_dir()])
                metrics["merge"]["direct"] = unit_count
        else:
            # Merge_1, Merge_2, Merge_3 имеют Converted, Extracted, Normalized
            for category in ["Converted", "Extracted", "Normalized"]:
                cat_dir = merge_dir / category
                if cat_dir.exists():
                    units = list(cat_dir.rglob("UNIT_*"))
                    unit_count = len([d for d in units if d.is_dir()])
                    metrics["merge"][category.lower()] = unit_count
    
    # Exceptions
    exc_dir = base_dir / "Exceptions" / f"Exceptions_{cycle}"
    if exc_dir.exists():
        for category in ["Special", "Mixed", "Ambiguous", "Empty"]:
            cat_dir = exc_dir / category
            if cat_dir.exists():
                units = list(cat_dir.rglob("UNIT_*"))
                unit_count = len([d for d in units if d.is_dir()])
                metrics["exceptions"][category.lower()] = unit_count
    
    return metrics


def print_metrics_table(metrics_list: list):
    """Выводит таблицу с метриками."""
    table = Table(title="Метрики обработки")
    table.add_column("Цикл", style="cyan")
    table.add_column("Processing", style="green")
    table.add_column("Merge", style="blue")
    table.add_column("Exceptions", style="yellow")
    
    for metrics in metrics_list:
        cycle = metrics["cycle"]
        
        # Processing
        proc_str = ", ".join([f"{k}: {v}" for k, v in metrics["processing"].items()])
        if not proc_str:
            proc_str = "-"
        
        # Merge
        merge_str = ", ".join([f"{k}: {v}" for k, v in metrics["merge"].items()])
        if not merge_str:
            merge_str = "-"
        
        # Exceptions
        exc_str = ", ".join([f"{k}: {v}" for k, v in metrics["exceptions"].items()])
        if not exc_str:
            exc_str = "-"
        
        table.add_row(str(cycle), proc_str, merge_str, exc_str)
    
    console.print(table)


@app.command()
def test(
    date: str = typer.Argument(..., help="Дата в формате YYYY-MM-DD"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Только показать что будет сделано"),
):
    """
    Запускает полное тестирование всех циклов обработки.
    
    Args:
        date: Дата в формате YYYY-MM-DD
        dry_run: Если True, только показывает что будет сделано
    """
    base_dir = DATA_BASE_DIR / date
    input_dir = base_dir / "Input"
    
    if not input_dir.exists():
        console.print(f"❌ Директория Input не существует: {input_dir}", style="red")
        raise typer.Exit(1)
    
    # Инициализируем структуру директорий
    console.print(f"\n📁 Инициализация структуры директорий для {date}...")
    init_directory_structure(date)
    
    # Инициализируем компоненты
    classifier = Classifier()
    converter = Converter()
    extractor = Extractor()
    normalizer = ExtensionNormalizer()
    merger = Merger()
    
    all_metrics = []
    
    # ЦИКЛ 1: Классификация из Input
    console.print("\n" + "="*60)
    console.print("🔄 ЦИКЛ 1: Классификация из Input", style="bold cyan")
    console.print("="*60)
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Классификация...", total=None)
        
        # Классификация
        units = find_all_units(input_dir)
        console.print(f"  Найдено UNIT в Input: {len(units)}")
        
        if not dry_run:
            for unit_dir in units:
                classifier.classify_unit(
                    unit_path=unit_dir,
                    cycle=1,
                    protocol_date=date,
                    dry_run=dry_run,
                    copy_mode=True,  # Копируем вместо перемещения для тестов
                )
        
        progress.update(task, completed=True)
    
    # Собираем метрики после цикла 1
    metrics_1 = collect_metrics(1, date)
    all_metrics.append(metrics_1)
    console.print(f"\n📊 Метрики после цикла 1:")
    print_metrics_table([metrics_1])
    
    # Обработка Processing_1
    console.print("\n" + "="*60)
    console.print("⚙️  ОБРАБОТКА Processing_1", style="bold green")
    console.print("="*60)
    
    proc_1_dir = base_dir / "Processing" / "Processing_1"
    
    if proc_1_dir.exists() and not dry_run:
        # Convert
        convert_dir = proc_1_dir / "Convert"
        if convert_dir.exists():
            units = find_all_units(convert_dir)
            console.print(f"  Convert: {len(units)} UNIT")
            for unit_dir in units:
                converter.convert_unit(
                    unit_path=unit_dir,
                    cycle=1,
                    protocol_date=date,
                    dry_run=dry_run,
                )
        
        # Extract
        extract_dir = proc_1_dir / "Extract"
        if extract_dir.exists():
            units = find_all_units(extract_dir)
            console.print(f"  Extract: {len(units)} UNIT")
            for unit_dir in units:
                extractor.extract_unit(
                    unit_path=unit_dir,
                    cycle=1,
                    protocol_date=date,
                    dry_run=dry_run,
                )
        
        # Normalize
        normalize_dir = proc_1_dir / "Normalize"
        if normalize_dir.exists():
            units = find_all_units(normalize_dir)
            console.print(f"  Normalize: {len(units)} UNIT")
            for unit_dir in units:
                normalizer.normalize_extensions(
                    unit_path=unit_dir,
                    cycle=1,
                    protocol_date=date,
                    dry_run=dry_run,
                )
    
    # ЦИКЛ 2: Классификация из Merge_1
    console.print("\n" + "="*60)
    console.print("🔄 ЦИКЛ 2: Классификация из Merge_1", style="bold cyan")
    console.print("="*60)
    
    merge_1_dir = base_dir / "Merge" / "Merge_1"
    if merge_1_dir.exists():
        units = find_all_units(merge_1_dir)
        console.print(f"  Найдено UNIT в Merge_1: {len(units)}")
        
        if not dry_run:
            for unit_dir in units:
                classifier.classify_unit(
                    unit_path=unit_dir,
                    cycle=2,
                    protocol_date=date,
                    dry_run=dry_run,
                )
    
    # Собираем метрики после цикла 2
    metrics_2 = collect_metrics(2, date)
    all_metrics.append(metrics_2)
    console.print(f"\n📊 Метрики после цикла 2:")
    print_metrics_table([metrics_2])
    
    # Обработка Processing_2
    console.print("\n" + "="*60)
    console.print("⚙️  ОБРАБОТКА Processing_2", style="bold green")
    console.print("="*60)
    
    proc_2_dir = base_dir / "Processing" / "Processing_2"
    
    if proc_2_dir.exists() and not dry_run:
        # Convert
        convert_dir = proc_2_dir / "Convert"
        if convert_dir.exists():
            units = find_all_units(convert_dir)
            console.print(f"  Convert: {len(units)} UNIT")
            for unit_dir in units:
                converter.convert_unit(
                    unit_path=unit_dir,
                    cycle=2,
                    protocol_date=date,
                    dry_run=dry_run,
                )
        
        # Extract
        extract_dir = proc_2_dir / "Extract"
        if extract_dir.exists():
            units = find_all_units(extract_dir)
            console.print(f"  Extract: {len(units)} UNIT")
            for unit_dir in units:
                extractor.extract_unit(
                    unit_path=unit_dir,
                    cycle=2,
                    protocol_date=date,
                    dry_run=dry_run,
                )
        
        # Normalize
        normalize_dir = proc_2_dir / "Normalize"
        if normalize_dir.exists():
            units = find_all_units(normalize_dir)
            console.print(f"  Normalize: {len(units)} UNIT")
            for unit_dir in units:
                normalizer.normalize_extensions(
                    unit_path=unit_dir,
                    cycle=2,
                    protocol_date=date,
                    dry_run=dry_run,
                )
    
    # ЦИКЛ 3: Классификация из Merge_2
    console.print("\n" + "="*60)
    console.print("🔄 ЦИКЛ 3: Классификация из Merge_2", style="bold cyan")
    console.print("="*60)
    
    merge_2_dir = base_dir / "Merge" / "Merge_2"
    if merge_2_dir.exists():
        units = find_all_units(merge_2_dir)
        console.print(f"  Найдено UNIT в Merge_2: {len(units)}")
        
        if not dry_run:
            for unit_dir in units:
                classifier.classify_unit(
                    unit_path=unit_dir,
                    cycle=3,
                    protocol_date=date,
                    dry_run=dry_run,
                )
    
    # Собираем метрики после цикла 3
    metrics_3 = collect_metrics(3, date)
    all_metrics.append(metrics_3)
    console.print(f"\n📊 Метрики после цикла 3:")
    print_metrics_table([metrics_3])
    
    # Обработка Processing_3 (если есть)
    console.print("\n" + "="*60)
    console.print("⚙️  ОБРАБОТКА Processing_3", style="bold green")
    console.print("="*60)
    
    proc_3_dir = base_dir / "Processing" / "Processing_3"
    
    if proc_3_dir.exists() and not dry_run:
        # Convert
        convert_dir = proc_3_dir / "Convert"
        if convert_dir.exists():
            units = find_all_units(convert_dir)
            console.print(f"  Convert: {len(units)} UNIT")
            for unit_dir in units:
                converter.convert_unit(
                    unit_path=unit_dir,
                    cycle=3,
                    protocol_date=date,
                    dry_run=dry_run,
                )
        
        # Extract
        extract_dir = proc_3_dir / "Extract"
        if extract_dir.exists():
            units = find_all_units(extract_dir)
            console.print(f"  Extract: {len(units)} UNIT")
            for unit_dir in units:
                extractor.extract_unit(
                    unit_path=unit_dir,
                    cycle=3,
                    protocol_date=date,
                    dry_run=dry_run,
                )
        
        # Normalize
        normalize_dir = proc_3_dir / "Normalize"
        if normalize_dir.exists():
            units = find_all_units(normalize_dir)
            console.print(f"  Normalize: {len(units)} UNIT")
            for unit_dir in units:
                normalizer.normalize_extensions(
                    unit_path=unit_dir,
                    cycle=3,
                    protocol_date=date,
                    dry_run=dry_run,
                )
    
    # ФИНАЛЬНЫЙ СБОР В Ready2Docling
    console.print("\n" + "="*60)
    console.print("📦 ФИНАЛЬНЫЙ СБОР В Ready2Docling", style="bold magenta")
    console.print("="*60)
    
    if not dry_run:
        # Собираем UNIT из всех Merge_N
        source_dirs = []
        
        # Merge_0/Direct
        merge_0_dir = base_dir / "Merge" / "Merge_0"
        if merge_0_dir.exists():
            source_dirs.append(merge_0_dir)
        
        # Merge_1, Merge_2, Merge_3
        for i in range(1, 4):
            merge_dir = base_dir / "Merge" / f"Merge_{i}"
            if merge_dir.exists():
                source_dirs.append(merge_dir)
        
        if source_dirs:
            ready2docling_dir = base_dir / "Ready2Docling"
            ready2docling_dir.mkdir(parents=True, exist_ok=True)
            
            result = merger.collect_units(
                source_dirs=source_dirs,
                target_dir=ready2docling_dir,
            )
            
            console.print(f"  ✅ Обработано UNIT: {result['units_processed']}")
            if result['errors']:
                console.print(f"  ⚠️  Ошибок: {len(result['errors'])}", style="yellow")
    
    # Финальные метрики
    console.print("\n" + "="*60)
    console.print("📊 ИТОГОВЫЕ МЕТРИКИ", style="bold")
    console.print("="*60)
    print_metrics_table(all_metrics)
    
    # Ready2Docling статистика
    ready2docling_dir = base_dir / "Ready2Docling"
    if ready2docling_dir.exists():
        units = list(ready2docling_dir.rglob("UNIT_*"))
        unit_count = len([d for d in units if d.is_dir()])
        console.print(f"\n✅ UNIT в Ready2Docling: {unit_count}")
    
    console.print("\n✅ Тестирование завершено!")


if __name__ == "__main__":
    app()


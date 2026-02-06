"""
Classifier - отдельный контроль классификатора для тестов.
"""
import typer
from pathlib import Path

from ..engine.classifier import Classifier

app = typer.Typer(name="classifier", help="Управление классификатором")


@app.command("run")
def classifier_run(
    input_dir: Path = typer.Option(..., "--input", help="Входная директория"),
    cycle: int = typer.Option(1, "--cycle", help="Номер цикла"),
    report_only: bool = typer.Option(False, "--report-only", help="Только отчёт"),
    confidence_threshold: float = typer.Option(0.9, "--confidence-threshold", help="Порог уверенности"),
    dump_json: bool = typer.Option(False, "--dump-json", help="Вывести JSON"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
):
    """Запускает классификатор."""
    from datetime import datetime
    from ..core.unit_processor import process_directory_units
    from ..utils.paths import find_all_units
    
    if not input_dir.exists():
        typer.echo(f"❌ Директория не найдена: {input_dir}", err=True)
        raise typer.Exit(1)
    
    typer.echo(f"🔍 Классификация: {input_dir} (цикл {cycle})")
    classifier = Classifier()
    
    units = find_all_units(input_dir)
    if not units:
        typer.echo("ℹ️  UNIT не найдены")
        return
    
    results = []
    categories = {}
    
    def process_unit(unit_path: Path) -> dict:
        """Обработка одного UNIT классификатором."""
        result = classifier.classify_unit(unit_path, cycle, protocol_date=None, dry_run=dry_run)
        category = result.get("category", "unknown")
        categories[category] = categories.get(category, 0) + 1
        results.append({
            "unit_id": unit_path.name,
            "category": category,
            "is_mixed": result.get("is_mixed", False),
            "target_directory": str(result.get("target_directory", "")),
        })
        return result
    
    process_results = process_directory_units(
        source_dir=input_dir,
        processor_func=process_unit,
        dry_run=dry_run,
    )
    
    # Формируем отчёт
    if report_only:
        typer.echo("\n📊 Отчёт по классификации:")
        typer.echo(f"  Всего UNIT: {len(results)}")
        typer.echo(f"  Категории:")
        for cat, count in sorted(categories.items()):
            typer.echo(f"    - {cat}: {count}")
    else:
        typer.echo(f"\n✅ Обработано UNIT: {process_results['units_processed']}")
        if process_results['units_failed'] > 0:
            typer.echo(f"❌ Ошибок: {process_results['units_failed']}", err=True)
    
    if dump_json:
        import json
        typer.echo("\n📄 JSON отчёт:")
        typer.echo(json.dumps({
            "summary": {
                "total_units": len(results),
                "categories": categories,
                "processed": process_results['units_processed'],
                "failed": process_results['units_failed'],
            },
            "units": results,
        }, indent=2, ensure_ascii=False))


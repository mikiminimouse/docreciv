"""
CLI команды для отображения статистики и метрик.
"""
import typer
from pathlib import Path
from typing import Optional
from collections import Counter

from ..utils.statistics import (
    collect_input_statistics,
    analyze_output_statistics,
    calculate_percentage_statistics,
    generate_statistics_report,
)

app = typer.Typer(
    name="stats",
    help="Статистика и метрики по UNIT",
)


@app.command("show")
def show_statistics(
    date: str = typer.Argument(..., help="Дата в формате YYYY-MM-DD"),
    input_dir: Optional[str] = typer.Option(None, "--input", help="Директория Input (опционально)"),
    output: Optional[str] = typer.Option(None, "--output", help="Сохранить отчет в файл"),
    detailed: bool = typer.Option(False, "--detailed", help="Показать детальную статистику"),
):
    """
    Показывает статистику по UNIT для указанной даты.
    
    Примеры:
        docprep stats show 2025-12-20
        docprep stats show 2025-12-20 --detailed
        docprep stats show 2025-12-20 --output report.txt
    """
    # Собираем статистику
    input_stats = None
    if input_dir:
        input_path = Path(input_dir)
        if input_path.exists():
            input_stats = collect_input_statistics(input_path)
    
    output_stats = analyze_output_statistics(date)
    
    if input_stats is None:
        input_path = Path(f"Data/{date}/Input")
        if not input_path.exists():
            input_path = Path(f"Data/{date}/input")
        if input_path.exists():
            input_stats = collect_input_statistics(input_path)
    
    if input_stats is None:
        input_stats = {
            "total_units": 0,
            "empty_units": 0,
            "units_with_files": 0,
            "file_extensions": Counter(),
        }
    
    # Вычисляем проценты
    percentages = calculate_percentage_statistics(input_stats, output_stats)
    
    # Генерируем отчет
    report = generate_statistics_report(date, input_stats, output_stats, percentages)
    
    # Выводим или сохраняем
    if output:
        output_path = Path(output)
        output_path.write_text(report, encoding="utf-8")
        typer.echo(f"✅ Отчет сохранен: {output_path}")
    else:
        typer.echo(report)
    
    # Детальная статистика
    if detailed:
        typer.echo("\n" + "=" * 80)
        typer.echo("ДЕТАЛЬНАЯ СТАТИСТИКА")
        typer.echo("=" * 80)
        
        # По категориям и расширениям
        typer.echo("\nРАСШИРЕНИЯ ПО КАТЕГОРИЯМ (ВХОД):")
        typer.echo("-" * 80)
        for category in sorted(output_stats['category_by_extension_input'].keys()):
            if output_stats['category_by_extension_input'][category]:
                typer.echo(f"\n{category.upper()}:")
                for ext, count in output_stats['category_by_extension_input'][category].most_common(10):
                    typer.echo(f"  .{ext}: {count}")
        
        # По локациям
        typer.echo("\n\nРАСПРЕДЕЛЕНИЕ ПО ЛОКАЦИЯМ:")
        typer.echo("-" * 80)
        for category in sorted(output_stats['category_by_location'].keys()):
            typer.echo(f"\n{category.upper()}:")
            for location, count in sorted(output_stats['category_by_location'][category].items())[:10]:
                typer.echo(f"  {location}: {count}")


@app.command("compare")
def compare_iterations(
    date: str = typer.Argument(..., help="Дата в формате YYYY-MM-DD"),
    cycle1: int = typer.Option(1, "--cycle1", help="Первый цикл для сравнения"),
    cycle2: int = typer.Option(2, "--cycle2", help="Второй цикл для сравнения"),
):
    """
    Сравнивает статистику между циклами обработки.
    
    Пример:
        docprep stats compare 2025-12-20 --cycle1 1 --cycle2 2
    """
    output_stats = analyze_output_statistics(date)
    
    typer.echo("=" * 80)
    typer.echo(f"СРАВНЕНИЕ ЦИКЛОВ {cycle1} И {cycle2}")
    typer.echo("=" * 80)
    
    # Фильтруем по циклам
    cycle1_locations = {loc: count for loc, count in output_stats['by_location'].items() if f"Processing_{cycle1}" in loc or f"Merge_{cycle1-1}" in loc}
    cycle2_locations = {loc: count for loc, count in output_stats['by_location'].items() if f"Processing_{cycle2}" in loc or f"Merge_{cycle2-1}" in loc}
    
    typer.echo(f"\nЦикл {cycle1}: {sum(cycle1_locations.values())} UNIT")
    typer.echo(f"Цикл {cycle2}: {sum(cycle2_locations.values())} UNIT")
    
    # Показываем изменения
    typer.echo("\nИзменения:")
    all_locations = set(cycle1_locations.keys()) | set(cycle2_locations.keys())
    for loc in sorted(all_locations):
        c1_count = cycle1_locations.get(loc, 0)
        c2_count = cycle2_locations.get(loc, 0)
        if c1_count != c2_count:
            diff = c2_count - c1_count
            typer.echo(f"  {loc}: {c1_count} → {c2_count} ({diff:+d})")


@app.command("export")
def export_statistics(
    date: str = typer.Argument(..., help="Дата в формате YYYY-MM-DD"),
    format: str = typer.Option("markdown", "--format", help="Формат экспорта (markdown, json)"),
    output: str = typer.Option(None, "--output", help="Путь к файлу вывода"),
):
    """
    Экспортирует статистику в файл.
    
    Примеры:
        docprep stats export 2025-12-20 --format markdown --output report.md
        docprep stats export 2025-12-20 --format json --output report.json
    """
    from ..utils.statistics import generate_statistics_report
    
    if format == "markdown":
        # Используем существующую функцию генерации отчета
        report = generate_statistics_report(date)
        
        if output:
            output_path = Path(output)
            output_path.write_text(report, encoding="utf-8")
            typer.echo(f"✅ Отчет сохранен: {output_path}")
        else:
            typer.echo(report)
    elif format == "json":
        import json
        
        input_stats = collect_input_statistics(Path(f"Data/{date}/Input"))
        output_stats = analyze_output_statistics(date)
        percentages = calculate_percentage_statistics(input_stats, output_stats)
        
        data = {
            "date": date,
            "input": dict(input_stats),
            "output": {k: dict(v) if isinstance(v, Counter) else v for k, v in output_stats.items()},
            "percentages": percentages,
        }
        
        if output:
            output_path = Path(output)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            typer.echo(f"✅ JSON сохранен: {output_path}")
        else:
            typer.echo(json.dumps(data, indent=2, ensure_ascii=False, default=str))
    else:
        typer.echo(f"❌ Неподдерживаемый формат: {format}")
        raise typer.Exit(1)


"""
Модуль для сбора и анализа статистики по UNIT.
"""
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, Any, Optional
from datetime import datetime

from ..core.manifest import load_manifest
from ..utils.paths import get_unit_files


def collect_input_statistics(input_dir: Path) -> Dict[str, Any]:
    """Собирает детальную статистику по входным UNIT."""
    stats = {
        "total_units": 0,
        "empty_units": 0,
        "units_with_files": 0,
        "file_extensions": Counter(),
        "files_by_extension": Counter(),
        "units_by_extension": defaultdict(set),
    }
    
    for unit_dir in input_dir.iterdir():
        if not unit_dir.is_dir() or not unit_dir.name.startswith("UNIT_"):
            continue
        
        stats["total_units"] += 1
        files = get_unit_files(unit_dir)
        
        if not files:
            stats["empty_units"] += 1
            continue
        
        stats["units_with_files"] += 1
        
        for file_path in files:
            ext = file_path.suffix.lower().lstrip(".")
            if not ext:
                ext = "no_extension"
            stats["file_extensions"][ext] += 1
            stats["files_by_extension"][ext] += 1
            stats["units_by_extension"][ext].add(unit_dir.name)
    
    return stats


def analyze_output_statistics(date: str) -> Dict[str, Any]:
    """Анализирует результаты классификации из manifest."""
    base_dir = Path(f"Data/{date}")
    results = {
        "total_classified": 0,
        "by_category": Counter(),
        "by_location": defaultdict(int),
        "empty_units": 0,
        "units_with_files": 0,
        "empty_by_category": Counter(),
        "with_files_by_category": Counter(),
        "file_extensions_input": Counter(),
        "file_extensions_output": Counter(),
        "category_by_extension_input": defaultdict(lambda: Counter()),
        "category_by_extension_output": defaultdict(lambda: Counter()),
        "category_by_location": defaultdict(lambda: Counter()),
        "route_distribution": Counter(),
        "route_by_extension": defaultdict(lambda: Counter()),
        "units_by_route_and_extension": defaultdict(lambda: Counter()),
        "errors": [],
    }
    
    # Собираем информацию из всех директорий
    # НОВАЯ СТРУКТУРА v2:
    # - Merge/Direct - для прямых файлов
    # - Merge/Processed_N - для обработанных units
    # - Exceptions/Direct - для исключений до обработки
    # - Exceptions/Processed_N - для исключений после обработки
    search_dirs = [
        base_dir / "Processing" / "Processing_1",
        base_dir / "Processing" / "Processing_2",
        base_dir / "Processing" / "Processing_3",
        base_dir / "Merge" / "Direct",
        base_dir / "Merge" / "Processed_1",
        base_dir / "Merge" / "Processed_2",
        base_dir / "Merge" / "Processed_3",
        base_dir / "Exceptions" / "Direct",
        base_dir / "Exceptions" / "Processed_1",
        base_dir / "Exceptions" / "Processed_2",
        base_dir / "Exceptions" / "Processed_3",
    ]
    
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        
        for unit_dir in search_dir.rglob("UNIT_*"):
            if not unit_dir.is_dir():
                continue
            
            manifest_path = unit_dir / "manifest.json"
            if not manifest_path.exists():
                continue
            
            try:
                manifest = load_manifest(unit_dir)
                results["total_classified"] += 1
                
                files = manifest.get("files", [])
                is_empty = len(files) == 0
                
                if is_empty:
                    results["empty_units"] += 1
                else:
                    results["units_with_files"] += 1
                
                # Категория из applied_operations
                operations = manifest.get("applied_operations", [])
                category = "unknown"
                if operations:
                    last_op = operations[-1]
                    category = last_op.get("category", "unknown")
                    results["by_category"][category] += 1
                    
                    if is_empty:
                        results["empty_by_category"][category] += 1
                    else:
                        results["with_files_by_category"][category] += 1
                
                # Определяем локацию
                location_parts = list(unit_dir.parts)
                location = "/".join(location_parts[location_parts.index(date)+1:]) if date in location_parts else "unknown"
                results["by_location"][location] += 1
                results["category_by_location"][category][location] += 1
                
                # Route
                route = manifest.get("processing", {}).get("route")
                if route:
                    results["route_distribution"][route] += 1
                
                # Собираем расширения файлов в UNIT
                unit_extensions = set()
                for file_info in files:
                    original_name = file_info.get("original_name", "")
                    if original_name:
                        ext = Path(original_name).suffix.lower().lstrip(".")
                        if not ext:
                            ext = "no_extension"
                        if ext:
                            unit_extensions.add(ext)
                            results["file_extensions_input"][ext] += 1
                            results["category_by_extension_input"][category][ext] += 1
                            # Связываем route с расширением файла
                            if route:
                                results["route_by_extension"][ext][route] += 1
                    
                    current_name = file_info.get("current_name", "")
                    if current_name:
                        ext = Path(current_name).suffix.lower().lstrip(".")
                        if not ext:
                            ext = "no_extension"
                        if ext:
                            results["file_extensions_output"][ext] += 1
                            results["category_by_extension_output"][category][ext] += 1
                            if route:
                                results["route_by_extension"][ext][route] += 1
                
                # Связываем route с UNIT по расширениям (route привязан к UNIT, а не к файлам)
                if route and unit_extensions:
                    # Для каждого расширения в UNIT увеличиваем счетчик UNIT с этим route
                    for ext in unit_extensions:
                        results["units_by_route_and_extension"][route][ext] += 1
                elif route:
                    # Если route есть, но расширения не определены, все равно учитываем
                    # Это может быть для пустых UNIT или UNIT с неопределенными расширениями
                    pass
                
            except Exception as e:
                results["errors"].append({
                    "unit": unit_dir.name,
                    "error": str(e),
                })
    
    return results


def calculate_percentage_statistics(
    input_stats: Dict[str, Any],
    output_stats: Dict[str, Any],
) -> Dict[str, Any]:
    """Вычисляет процентную статистику по расширениям."""
    total_with_files = input_stats["units_with_files"]
    if total_with_files == 0:
        return {}
    
    percentages = {
        "by_extension": {},
        "by_route": {},
        "by_category": {},
    }
    
    # Статистика по расширениям
    for ext, file_count in input_stats["file_extensions"].most_common():
        unit_count = len(input_stats["units_by_extension"][ext])
        percentages["by_extension"][ext] = {
            "files": file_count,
            "units": unit_count,
            "files_percent": (file_count * 100) / sum(input_stats["file_extensions"].values()) if input_stats["file_extensions"] else 0,
            "units_percent": (unit_count * 100) / total_with_files if total_with_files > 0 else 0,
        }
    
    # Статистика по route
    route_totals = Counter()
    for ext, routes in output_stats["route_by_extension"].items():
        for route, count in routes.items():
            route_totals[route] += count
    
    total_routed = sum(route_totals.values())
    for route, count in route_totals.most_common():
        percentages["by_route"][route] = {
            "count": count,
            "percent": (count * 100) / total_routed if total_routed > 0 else 0,
        }
    
    # Статистика по категориям
    total_categorized = sum(output_stats["by_category"].values())
    for category, count in output_stats["by_category"].most_common():
        with_files = output_stats["with_files_by_category"].get(category, 0)
        percentages["by_category"][category] = {
            "total": count,
            "with_files": with_files,
            "total_percent": (count * 100) / total_categorized if total_categorized > 0 else 0,
            "with_files_percent": (with_files * 100) / total_with_files if total_with_files > 0 else 0,
        }
    
    return percentages


def generate_statistics_report(
    date: str,
    input_stats: Optional[Dict[str, Any]] = None,
    output_stats: Optional[Dict[str, Any]] = None,
    percentages: Optional[Dict[str, Any]] = None,
) -> str:
    """Генерирует текстовый отчет со статистикой."""
    if output_stats is None:
        output_stats = analyze_output_statistics(date)
    
    if input_stats is None:
        input_dir = Path(f"Data/{date}/Input")
        if not input_dir.exists():
            input_dir = Path(f"Data/{date}/input")
        if input_dir.exists():
            input_stats = collect_input_statistics(input_dir)
        else:
            input_stats = {
                "total_units": 0,
                "empty_units": 0,
                "units_with_files": 0,
                "file_extensions": Counter(),
            }
    
    if percentages is None:
        percentages = calculate_percentage_statistics(input_stats, output_stats)
    
    report = []
    report.append("=" * 80)
    report.append("СТАТИСТИКА ПО UNIT")
    report.append("=" * 80)
    report.append(f"Дата: {date}")
    report.append(f"Время генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # Общая статистика
    report.append("ОБЩАЯ СТАТИСТИКА")
    report.append("-" * 80)
    report.append(f"Всего UNIT: {input_stats['total_units']}")
    report.append(f"Пустых UNIT: {input_stats['empty_units']} ({input_stats['empty_units']*100//input_stats['total_units'] if input_stats['total_units'] > 0 else 0}%)")
    report.append(f"UNIT с файлами: {input_stats['units_with_files']} ({input_stats['units_with_files']*100//input_stats['total_units'] if input_stats['total_units'] > 0 else 0}%)")
    report.append(f"Классифицировано: {output_stats['total_classified']}")
    report.append("")
    
    # Распределение по категориям
    report.append("РАСПРЕДЕЛЕНИЕ ПО КАТЕГОРИЯМ (БЕЗ ПУСТЫХ UNIT)")
    report.append("-" * 80)
    total_with_files = input_stats['units_with_files']
    for category, count in output_stats['by_category'].most_common():
        with_files = output_stats['with_files_by_category'].get(category, 0)
        percent = percentages.get("by_category", {}).get(category, {}).get("with_files_percent", 0)
        report.append(f"{category:15} {with_files:6} UNIT ({percent:5.1f}%)")
    report.append("")
    
    # Статистика по расширениям с процентами
    report.append("СТАТИСТИКА ПО РАСШИРЕНИЯМ (БЕЗ ПУСТЫХ UNIT)")
    report.append("-" * 80)
    report.append(f"{'Расширение':<15} {'Файлов':<10} {'UNIT':<10} {'% файлов':<12} {'% UNIT':<10}")
    report.append("-" * 80)
    
    for ext, data in sorted(percentages.get("by_extension", {}).items(), key=lambda x: x[1]["units"], reverse=True)[:30]:
        report.append(
            f".{ext:<14} {data['files']:<10} {data['units']:<10} "
            f"{data['files_percent']:>10.1f}% {data['units_percent']:>8.1f}%"
        )
    report.append("")
    
    # Детальная статистика по PDF с разбивкой по route
    if "pdf" in percentages.get("by_extension", {}):
        pdf_data = percentages["by_extension"]["pdf"]
        
        # Собираем статистику по PDF UNIT с route
        pdf_units_by_route = Counter()
        units_by_route_ext = output_stats.get("units_by_route_and_extension", {})
        for route, extensions in units_by_route_ext.items():
            if "pdf" in extensions:
                pdf_units_by_route[route] = extensions.get("pdf", 0)
        
        # Если нет данных по route, используем route_distribution как fallback
        if not pdf_units_by_route:
            for route, count in output_stats["route_distribution"].items():
                if "pdf" in route.lower() or route in ["pdf_text", "pdf_scan", "mixed"]:
                    pdf_units_by_route[route] = count
        
        if pdf_data['units'] > 0 or pdf_units_by_route:
            report.append("ДЕТАЛЬНАЯ СТАТИСТИКА PDF (БЕЗ ПУСТЫХ UNIT)")
            report.append("-" * 80)
            report.append(f"Всего PDF: {pdf_data['files']} файлов ({pdf_data['units']} UNIT)")
            report.append(f"Процент от всех файлов: {pdf_data['files_percent']:.1f}%")
            report.append(f"Процент от всех UNIT: {pdf_data['units_percent']:.1f}%")
            report.append("")
            
            # Процент от общего количества UNIT
            if total_with_files > 0:
                pdf_total_percent = pdf_data['units_percent']
                report.append(f"Процент PDF от всех UNIT (без пустых): {pdf_total_percent:.1f}%")
                
                if pdf_units_by_route:
                    report.append("\nРаспределение PDF UNIT по route:")
                    report.append("-" * 80)
                    for route, unit_count in pdf_units_by_route.most_common():
                        route_percent = (unit_count * 100) / total_with_files
                        report.append(f"  {route:20} {unit_count:6} UNIT ({route_percent:5.1f}%)")
                    
                    # Показываем формулу: PDF_total = PDF_text + PDF_scan
                    pdf_text_units = pdf_units_by_route.get("pdf_text", 0)
                    pdf_scan_units = pdf_units_by_route.get("pdf_scan", 0)
                    pdf_total_units = pdf_data['units']
                    if pdf_text_units > 0 or pdf_scan_units > 0:
                        report.append(f"\n  Формула: PDF_total ({pdf_total_units} UNIT, {pdf_total_percent:.1f}%) = ")
                        if pdf_text_units > 0:
                            text_percent = (pdf_text_units * 100) / total_with_files
                            report.append(f"    PDF_text ({pdf_text_units} UNIT, {text_percent:.1f}%) + ")
                        if pdf_scan_units > 0:
                            scan_percent = (pdf_scan_units * 100) / total_with_files
                            report.append(f"    PDF_scan ({pdf_scan_units} UNIT, {scan_percent:.1f}%)")
            report.append("")
    
    
    # Статистика по route
    if percentages.get("by_route"):
        report.append("РАСПРЕДЕЛЕНИЕ ПО ROUTE")
        report.append("-" * 80)
        for route, data in sorted(percentages["by_route"].items(), key=lambda x: x[1]["count"], reverse=True):
            report.append(f"{route:20} {data['count']:6} ({data['percent']:>5.1f}%)")
        report.append("")
    
    return "\n".join(report)


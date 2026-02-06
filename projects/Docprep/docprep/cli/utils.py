"""
Utils - сервисные команды.
"""
import typer
from pathlib import Path

from ..core.config import init_directory_structure
from ..utils.paths import find_units

app = typer.Typer(name="utils", help="Сервисные команды")


@app.command("check-deps")
def utils_check_deps(
    include_optional: bool = typer.Option(False, "--include-optional", "-o", help="Показать включая опциональные зависимости"),
    export_format: str = typer.Option(None, "--format", "-f", help="Формат экспорта (json, text)"),
):
    """
    Проверить все системные зависимости Docprep.

    Проверяет наличие системных утилит (LibreOffice, Xvfb, unrar, p7zip)
    и Python библиотек (rarfile, py7zr, pymongo, etc.).
    """
    from ..utils.dependencies import DependencyChecker

    typer.echo("🔍 Проверка системных зависимостей Docprep\n")

    result = DependencyChecker.check_all(include_optional=include_optional)

    # Форматирование вывода
    if export_format == "json":
        import json
        typer.echo(json.dumps(result, indent=2))
    else:
        # Системные утилиты
        typer.echo("📦 Системные утилиты:")
        for tool, info in result["system"].items():
            status = "✅" if info["available"] else "❌"
            cmd = info.get("command") or "not found"
            typer.echo(f"  {status} {tool}: {cmd}")

        # Python библиотеки
        typer.echo("\n🐍 Python библиотеки:")
        for lib, info in result["python"].items():
            status = "✅" if info["available"] else "❌"
            typer.echo(f"  {status} {lib}")

        # Поддержка архивов
        typer.echo("\n📁 Поддержка архивов:")
        archive_support = DependencyChecker.check_archive_support()
        for fmt, info in archive_support.items():
            status = "✅" if info["ready"] else "❌"
            details = []
            if info["python"]:
                details.append("py")
            if info["system"]:
                details.append("sys")
            typer.echo(f"  {status} .{fmt.upper()}: {', '.join(details) if details else 'none'}")

        # Поддержка конвертации
        typer.echo("\n📄 Поддержка конвертации:")
        conversion_support = DependencyChecker.check_conversion_support()
        lo_status = "✅" if conversion_support["libreoffice"] else "❌"
        xvfb_status = "✅" if conversion_support["xvfb"] else "❌"
        ready_status = "✅" if conversion_support["ready"] else "❌"
        typer.echo(f"  {lo_status} LibreOffice")
        typer.echo(f"  {xvfb_status} Xvfb (headless)")
        typer.echo(f"  {ready_status} Готовность к конвертации")

        # Критические проблемы
        if result["critical_issues"]:
            typer.echo("\n⚠️  Критические проблемы:")
            for issue in result["critical_issues"]:
                typer.echo(f"  ❌ {issue}")

        # Опциональные проблемы
        if include_optional and result["optional_issues"]:
            typer.echo("\n📝 Опциональные зависимости:")
            for issue in result["optional_issues"]:
                typer.echo(f"  ⚠️  {issue}")

        # Итог
        if result["all_ok"]:
            typer.echo("\n✅ Все критические зависимости установлены!")
        else:
            typer.echo("\n❌ Некоторые зависимости отсутствуют!")
            raise typer.Exit(1)


@app.command("init-date")
def utils_init_date(
    date: str = typer.Argument(..., help="Дата в формате YYYY-MM-DD"),
    data_dir: Path = typer.Option(None, "--data-dir", help="Базовая директория Data (по умолчанию ./Data)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
):
    """Создать структуру директорий для даты согласно PRD."""
    from ..core.config import DATA_BASE_DIR, init_directory_structure
    
    if data_dir is None:
        data_dir = DATA_BASE_DIR
    
    if verbose:
        typer.echo(f"Инициализация структуры для даты: {date}")
        typer.echo(f"Базовая директория: {data_dir}")
    
    init_directory_structure(data_dir, date)
    typer.echo(f"✓ Структура директорий создана для {date}")


@app.command("clean")
def utils_clean(
    directory: Path = typer.Argument(..., help="Директория для очистки"),
    confirm: bool = typer.Option(False, "--confirm", help="Подтверждение"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
):
    """Очистить директорию."""
    if not confirm:
        typer.echo("Используйте --confirm для подтверждения", err=True)
        raise typer.Exit(1)

    if not directory.exists():
        typer.echo(f"❌ Директория не найдена: {directory}", err=True)
        raise typer.Exit(1)
    
    typer.echo(f"🧹 Очистка директории: {directory}")
    
    # Безопасная очистка: только временные файлы и пустые директории
    import shutil
    from ..utils.paths import find_all_units
    
    # Проверяем, что это не критичная директория
    critical_dirs = ["Input", "Ready2Docling"]
    if any(critical in str(directory) for critical in critical_dirs):
        typer.echo("❌ Очистка критичных директорий запрещена!", err=True)
        raise typer.Exit(1)
    
    # Удаляем только пустые UNIT директории и временные файлы
    units = find_all_units(directory)
    removed_count = 0
    
    for unit_path in units:
        # Проверяем, что UNIT пустой (только служебные файлы)
        files = [f for f in unit_path.rglob("*") 
                if f.is_file() and f.name not in ["manifest.json", "audit.log.jsonl"]]
        
        if not files:
            if verbose:
                typer.echo(f"  🗑️  Удаление пустого UNIT: {unit_path.name}")
            shutil.rmtree(unit_path, ignore_errors=True)
            removed_count += 1
    
    typer.echo(f"✅ Удалено пустых UNIT: {removed_count}")


@app.command("stats")
def utils_stats(
    directory: Path = typer.Argument(..., help="Директория для статистики"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
):
    """Показать статистику по директории."""
    if not directory.exists():
        typer.echo(f"❌ Директория не найдена: {directory}", err=True)
        raise typer.Exit(1)
    
    typer.echo(f"📊 Статистика: {directory}\n")
    
    from ..utils.paths import find_all_units
    from ..core.manifest import load_manifest
    from collections import Counter
    
    units = find_all_units(directory)
    typer.echo(f"Всего UNIT: {len(units)}")
    
    if not units:
        return
    
    # Детальная статистика
    states = Counter()
    categories = Counter()
    file_types = Counter()
    cycles = Counter()
    total_files = 0
    
    for unit_path in units:
        manifest_path = unit_path / "manifest.json"
        if manifest_path.exists():
            try:
                manifest = load_manifest(unit_path)
                state = manifest.get("state_machine", {}).get("current_state", "unknown")
                states[state] += 1
                
                cycle = manifest.get("processing", {}).get("current_cycle", 0)
                cycles[cycle] += 1
                
                files = manifest.get("files", [])
                total_files += len(files)
                
                for file_info in files:
                    detected_type = file_info.get("detected_type", "unknown")
                    file_types[detected_type] += 1
            except Exception:
                states["error"] += 1
    
    typer.echo(f"\n📈 Детальная статистика:")
    typer.echo(f"  Всего файлов: {total_files}")
    
    if states:
        typer.echo(f"\n  Состояния UNIT:")
        for state, count in sorted(states.items()):
            typer.echo(f"    - {state}: {count}")
    
    if cycles:
        typer.echo(f"\n  Циклы:")
        for cycle, count in sorted(cycles.items()):
            typer.echo(f"    - Цикл {cycle}: {count}")
    
    if file_types:
        typer.echo(f"\n  Типы файлов:")
        for file_type, count in sorted(file_types.items(), key=lambda x: -x[1])[:10]:
            typer.echo(f"    - {file_type}: {count}")


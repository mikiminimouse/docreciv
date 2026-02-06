"""
Скрипт для очистки тестовых данных с сохранением Input.

Очищает все директории кроме Input для подготовки к чистым тестам.
"""
import shutil
from pathlib import Path
import typer

app = typer.Typer()


@app.command()
def cleanup(
    date: str = typer.Argument(..., help="Дата в формате YYYY-MM-DD"),
    confirm: bool = typer.Option(False, "--confirm", help="Подтверждение удаления"),
):
    """
    Очищает все директории кроме Input для указанной даты.
    
    Args:
        date: Дата в формате YYYY-MM-DD
        confirm: Подтверждение удаления (без этого флага только показывается что будет удалено)
    """
    from docprep.core.config import DATA_BASE_DIR
    
    base_dir = DATA_BASE_DIR / date
    
    if not base_dir.exists():
        typer.echo(f"❌ Директория {base_dir} не существует", err=True)
        raise typer.Exit(1)
    
    input_dir = base_dir / "Input"
    if not input_dir.exists():
        typer.echo(f"❌ Директория Input не существует в {base_dir}", err=True)
        raise typer.Exit(1)
    
    # Список директорий для удаления
    dirs_to_remove = [
        base_dir / "Processing",
        base_dir / "Merge",
        base_dir / "Exceptions",
        base_dir / "Ready2Docling",
    ]
    
    typer.echo(f"\n📋 План очистки для {date}:")
    typer.echo(f"  Сохранить: {input_dir}")
    
    for dir_path in dirs_to_remove:
        if dir_path.exists():
            # Подсчитываем UNIT
            units = list(dir_path.rglob("UNIT_*"))
            unit_count = len([d for d in units if d.is_dir()])
            typer.echo(f"  Удалить: {dir_path} ({unit_count} UNIT)")
        else:
            typer.echo(f"  Пропустить (не существует): {dir_path}")
    
    if not confirm:
        typer.echo("\n⚠️  Используйте --confirm для выполнения удаления")
        raise typer.Exit(0)
    
    # Выполняем удаление
    typer.echo("\n🗑️  Начинаем очистку...")
    removed_count = 0
    
    for dir_path in dirs_to_remove:
        if dir_path.exists():
            try:
                shutil.rmtree(dir_path)
                typer.echo(f"  ✅ Удалено: {dir_path}")
                removed_count += 1
            except Exception as e:
                typer.echo(f"  ❌ Ошибка при удалении {dir_path}: {e}", err=True)
    
    typer.echo(f"\n✅ Очистка завершена. Удалено директорий: {removed_count}")
    typer.echo(f"✅ Сохранено: {input_dir}")


if __name__ == "__main__":
    app()


"""
MongoDB CLI команды для DocPrep.

Предоставляет команды для:
- setup: Настройка MongoDB (создание индексов)
- export: Экспорт данных из файловой системы в MongoDB
- stats: Показать статистику из MongoDB
- cleanup: Удаление старых записей
"""
import typer
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="mongo",
    help="Команды для работы с MongoDB",
    add_completion=False,
)


# ========================================================================
# Опции
# ========================================================================

db_option = typer.Option(
    "mongodb://localhost:27017",
    "--db", "-d",
    help="MongoDB connection string (или MONGODB_URI env var)"
)

db_name_option = typer.Option(
    "docprep",
    "--db-name",
    help="Имя базы данных MongoDB"
)


# ========================================================================
# Команда: setup
# ========================================================================

@app.command("setup")
def mongo_setup(
    db: str = db_option,
    db_name: str = db_name_option,
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    Инициализирует MongoDB для DocPrep.

    Создаёт все необходимые коллекции и индексы.
    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    from docprep.core.database import DocPrepDatabase

    typer.echo(f"🔧 Connecting to MongoDB: {db}/{db_name}")

    try:
        database = DocPrepDatabase(connection_string=db, db_name=db_name)

        if not database.is_connected():
            typer.echo(
                "❌ Failed to connect to MongoDB. "
                "Make sure MongoDB is running and pymongo is installed.",
                err=True
            )
            raise typer.Exit(1)

        typer.echo("✅ Connected to MongoDB")
        typer.echo(f"   Database: {db_name}")
        typer.echo(f"   Collections:")
        typer.echo(f"     - pipeline_runs")
        typer.echo(f"     - unit_states")
        typer.echo(f"     - document_metadata")
        typer.echo(f"     - processing_metrics")
        typer.echo("")
        typer.echo("✅ Indexes created successfully")
        typer.echo("")
        typer.echo("💡 Hint: Set MONGODB_URI environment variable:")
        typer.echo(f"   export MONGODB_URI={db}/{db_name}")
        typer.echo("   export MONGODB_ENABLED=true")

    except Exception as e:
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(1)


# ========================================================================
# Команда: status
# ========================================================================

@app.command("status")
def mongo_status(
    db: str = db_option,
    db_name: str = db_name_option,
) -> None:
    """
    Проверяет статус подключения к MongoDB.
    """
    from docprep.core.database import DocPrepDatabase

    typer.echo(f"🔍 Checking MongoDB connection: {db}/{db_name}")

    database = DocPrepDatabase(connection_string=db, db_name=db_name)

    if not database.is_connected():
        typer.echo("❌ MongoDB is NOT available", err=True)
        typer.echo("")
        typer.echo("💡 Hint: Install pymongo:")
        typer.echo("   pip install pymongo")
        typer.echo("")
        typer.echo("   Or with extras:")
        typer.echo("   pip install -e '.[mongodb]'")
        raise typer.Exit(1)

    typer.echo("✅ MongoDB is connected")
    typer.echo("")

    # Показываем статистику коллекций
    from bson import json_util

    try:
        collections = database._collections
        total_docs = 0

        typer.echo("📊 Collection statistics:")
        for name, collection in collections.items():
            count = collection.estimated_document_count()
            total_docs += count
            typer.echo(f"   {name}: {count:,} documents")

        typer.echo(f"   Total: {total_docs:,} documents")

        # Показываем последние pipeline runs
        recent = database.list_pipelines(limit=5)
        if recent:
            typer.echo("")
            typer.echo("📋 Recent pipeline runs:")
            for p in recent:
                status_emoji = {
                    "completed": "✅",
                    "failed": "❌",
                    "running": "🔄",
                }.get(p.get("status", "unknown"), "❓")

                typer.echo(
                    f"   {status_emoji} {p['_id']} "
                    f"({p.get('protocol_date', 'N/A')}) "
                    f"- {p.get('units_total', 0)} units"
                )

    except Exception as e:
        typer.echo(f"⚠️  Could not retrieve statistics: {e}")


# ========================================================================
# Команда: stats
# ========================================================================

@app.command("stats")
def mongo_stats(
    pipeline_id: Optional[str] = typer.Option(None, "--pipeline", "-p", help="ID конкретного pipeline"),
    protocol_date: Optional[str] = typer.Option(None, "--date", "-d", help="Фильтр по дате протокола"),
    limit: int = typer.Option(10, "--limit", "-l", help="Максимум результатов"),
    db: str = db_option,
    db_name: str = db_name_option,
) -> None:
    """
    Показывает статистику из MongoDB.
    """
    from docprep.core.database import DocPrepDatabase
    import json

    database = DocPrepDatabase(connection_string=db, db_name=db_name)

    if not database.is_connected():
        typer.echo("❌ MongoDB is not connected", err=True)
        raise typer.Exit(1)

    if pipeline_id:
        # Детальная информация по pipeline
        summary = database.get_pipeline_summary(pipeline_id)

        if not summary:
            typer.echo(f"❌ Pipeline '{pipeline_id}' not found", err=True)
            raise typer.Exit(1)

        typer.echo(f"📋 Pipeline: {pipeline_id}")
        typer.echo(json.dumps(summary, indent=2, default=str))
    else:
        # Список pipeline runs
        pipelines = database.list_pipelines(
            protocol_date=protocol_date,
            limit=limit
        )

        if not pipelines:
            typer.echo("No pipelines found")
            return

        typer.echo(f"📋 Pipeline runs (showing {len(pipelines)}):")
        typer.echo("")

        for p in pipelines:
            status_emoji = {
                "completed": "✅",
                "failed": "❌",
                "running": "🔄",
            }.get(p.get("status", "unknown"), "❓")

            duration = p.get("duration_seconds")
            duration_str = f"{duration:.1f}s" if duration else "N/A"

            typer.echo(
                f"{status_emoji} {p['_id']}: "
                f"{p.get('protocol_date', 'N/A')} | "
                f"{p.get('units_total', 0)} units | "
                f"{p.get('metrics', {}).get('success_rate', 0):.1%} success | "
                f"{duration_str}"
            )


# ========================================================================
# Команда: compare
# ========================================================================

@app.command("compare")
def mongo_compare(
    pipeline1: str = typer.Argument(..., help="ID первого pipeline"),
    pipeline2: str = typer.Argument(..., help="ID второго pipeline"),
    db: str = db_option,
    db_name: str = db_name_option,
) -> None:
    """
    Сравнивает два pipeline run.
    """
    from docprep.core.database import DocPrepDatabase
    import json

    database = DocPrepDatabase(connection_string=db, db_name=db_name)

    if not database.is_connected():
        typer.echo("❌ MongoDB is not connected", err=True)
        raise typer.Exit(1)

    comparison = database.compare_pipelines(pipeline1, pipeline2)

    if not comparison:
        typer.echo("❌ Could not compare pipelines (one or both not found)", err=True)
        raise typer.Exit(1)

    typer.echo(f"📊 Comparing pipelines:")
    typer.echo("")
    typer.echo(f"Pipeline 1: {pipeline1}")
    p1 = comparison["pipeline1"]
    typer.echo(f"  Date: {p1['protocol_date']}")
    typer.echo(f"  Units: {p1['units_total']}")
    typer.echo(f"  Success rate: {p1['success_rate']:.1%}")
    typer.echo(f"  Duration: {p1['duration_seconds']}")
    typer.echo("")
    typer.echo(f"Pipeline 2: {pipeline2}")
    p2 = comparison["pipeline2"]
    typer.echo(f"  Date: {p2['protocol_date']}")
    typer.echo(f"  Units: {p2['units_total']}")
    typer.echo(f"  Success rate: {p2['success_rate']:.1%}")
    typer.echo(f"  Duration: {p2['duration_seconds']}")
    typer.echo("")
    typer.echo("Difference:")
    diff = comparison["diff"]
    typer.echo(f"  Units: {diff['units_total']:+,}")
    typer.echo(f"  Success rate: {diff['success_rate']:+.1%}")
    typer.echo(f"  Duration: {diff['duration_seconds']:+.1f}s")


# ========================================================================
# Команда: export
# ========================================================================

@app.command("export")
def mongo_export(
    input_dir: str = typer.Argument(..., help="Директория с обработанными данными (Ready2Docling)"),
    db: str = db_option,
    db_name: str = db_name_option,
    protocol_date: Optional[str] = typer.Option(None, "--date", "-d", help="Дата протокола"),
    pipeline_id: Optional[str] = typer.Option(None, "--pipeline-id", "-p", help="ID pipeline для связи"),
) -> None:
    """
    Экспортирует данные из файловой системы в MongoDB.

    Читает manifest.json из всех UNIT директорий и записывает в MongoDB.
    """
    from docprep.core.database import DocPrepDatabase
    from docprep.utils.paths import find_all_units
    from docprep.core.manifest import load_manifest
    import json

    typer.echo(f"📤 Exporting to MongoDB: {db}/{db_name}")

    database = DocPrepDatabase(connection_string=db, db_name=db_name)

    if not database.is_connected():
        typer.echo("❌ MongoDB is not connected", err=True)
        raise typer.Exit(1)

    input_path = Path(input_dir)

    if not input_path.exists():
        typer.echo(f"❌ Directory not found: {input_dir}", err=True)
        raise typer.Exit(1)

    # Извлекаем protocol_date если не указан
    if not protocol_date:
        import re
        match = re.search(r'\d{4}-\d{2}-\d{2}', str(input_path))
        protocol_date = match.group(0) if match else None

    # Создаём pipeline run если не указан
    if not pipeline_id:
        pipeline_id = database.start_pipeline(
            input_dir=str(input_path),
            output_dir=str(input_path),
            protocol_date=protocol_date,
        )
        typer.echo(f"✅ Created pipeline: {pipeline_id}")

    # Находим все UNIT
    typer.echo("🔍 Finding UNIT directories...")
    unit_paths = list(find_all_units(input_path))
    total_units = len(unit_paths)

    if total_units == 0:
        typer.echo("⚠️  No UNIT directories found")
        return

    typer.echo(f"📁 Found {total_units} UNIT directories")
    typer.echo("")

    # Экспортируем manifest'ы
    success_count = 0
    error_count = 0

    from tqdm import tqdm

    for unit_path in tqdm(unit_paths, desc="Exporting"):
        try:
            manifest = load_manifest(unit_path)

            # Записываем состояние UNIT
            database.write_unit_state(manifest)

            # Записываем метаданные файлов
            files = manifest.get("files", [])
            database.write_document_metadata(
                unit_id=manifest["unit_id"],
                files=files,
                pipeline_id=pipeline_id,
            )

            success_count += 1

        except Exception as e:
            logger.warning(f"Failed to export {unit_path.name}: {e}")
            error_count += 1

    # Обновляем метрики pipeline
    database.update_pipeline_metrics(pipeline_id, {
        "units_total": total_units,
        "units_success": success_count,
        "units_failed": error_count,
    })

    database.end_pipeline(pipeline_id, status="completed")

    typer.echo("")
    typer.echo(f"✅ Export completed:")
    typer.echo(f"   Total: {total_units} UNIT")
    typer.echo(f"   Success: {success_count}")
    typer.echo(f"   Errors: {error_count}")
    typer.echo(f"   Pipeline: {pipeline_id}")


# ========================================================================
# Команда: cleanup
# ========================================================================

@app.command("cleanup")
def mongo_cleanup(
    days: int = typer.Option(90, "--days", "-d", help="Удалить записи старее N дней"),
    db: str = db_option,
    db_name: str = db_name_option,
    confirm: bool = typer.Option(False, "--yes", "-y", help="Подтверждение без вопроса"),
) -> None:
    """
    Удаляет старые записи из MongoDB (retention policy).
    """
    from docprep.core.database import DocPrepDatabase

    if not confirm:
        typer.confirm(
            f"Delete all pipeline runs older than {days} days?",
            abort=True
        )

    database = DocPrepDatabase(connection_string=db, db_name=db_name)

    if not database.is_connected():
        typer.echo("❌ MongoDB is not connected", err=True)
        raise typer.Exit(1)

    typer.echo(f"🧹 Cleaning up records older than {days} days...")

    deleted_count = database.cleanup_old_records(days=days)

    typer.echo(f"✅ Deleted {deleted_count} pipeline runs and associated data")


# ========================================================================
# Команда: delete
# ========================================================================

@app.command("delete")
def mongo_delete(
    pipeline_id: str = typer.Argument(..., help="ID pipeline для удаления"),
    db: str = db_option,
    db_name: str = db_name_option,
    confirm: bool = typer.Option(False, "--yes", "-y", help="Подтверждение без вопроса"),
) -> None:
    """
    Удаляет pipeline и все связанные данные.
    """
    from docprep.core.database import DocPrepDatabase

    if not confirm:
        typer.confirm(
            f"Delete pipeline '{pipeline_id}' and all associated data?",
            abort=True
        )

    database = DocPrepDatabase(connection_string=db, db_name=db_name)

    if not database.is_connected():
        typer.echo("❌ MongoDB is not connected", err=True)
        raise typer.Exit(1)

    typer.echo(f"🗑️  Deleting pipeline: {pipeline_id}")

    if database.delete_pipeline(pipeline_id):
        typer.echo(f"✅ Pipeline '{pipeline_id}' deleted")
    else:
        typer.echo(f"❌ Failed to delete pipeline '{pipeline_id}'", err=True)
        raise typer.Exit(1)

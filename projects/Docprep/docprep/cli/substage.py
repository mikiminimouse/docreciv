"""
Substage - атомарные операции (convert, extract, normalize).
"""
import time
import logging
import typer
try:
    from typer.models import OptionInfo
except ImportError:
    # Typer < 0.4.0 uses typer.models.OptionInfo
    # Typer >= 0.4.0 might put it elsewhere or it is accessible
    # This is a safe fallback if imports change
    OptionInfo = typer.models.OptionInfo

from pathlib import Path
from typing import Optional, Any
from datetime import datetime

from ..engine.converter import Converter
from ..engine.extractor import Extractor
from ..engine.normalizers import NameNormalizer, ExtensionNormalizer
from ..core.unit_processor import process_directory_units
from ..utils.paths import find_all_units

logger = logging.getLogger(__name__)

app = typer.Typer(name="substage", help="Атомарные операции")


def _unwrap(val: Any) -> Any:
    """Извлекает значение по умолчанию из OptionInfo, если оно передано."""
    if isinstance(val, OptionInfo):
        return val.default
    return val


@app.command("convert")
def substage_convert_run(
    input_dir: Path = typer.Option(..., "--input", help="Входная директория"),
    cycle: int = typer.Option(1, "--cycle", help="Номер цикла (1, 2, 3)"),
    from_format: Optional[str] = typer.Option(None, "--from", help="Исходный формат"),
    to_format: Optional[str] = typer.Option(None, "--to", help="Целевой формат"),
    engine: str = typer.Option("libreoffice", "--engine", help="Движок конвертации"),
    use_headless: bool = typer.Option(False, "--use-headless", help="Использовать headless конвертер (решает проблемы с X11)"),
    mock_mode: bool = typer.Option(False, "--mock-mode", help="Режим симуляции для тестирования"),
    protocol_date: Optional[str] = typer.Option(None, "--date", help="Дата протокола (YYYY-MM-DD)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим имитации"),
    pipeline_id: Optional[str] = typer.Option(None, "--pipeline-id", help="ID pipeline для статистики"),
    enable_mongo: bool = typer.Option(False, "--enable-mongo", help="Включить запись в MongoDB"),
):
    """Конвертация форматов (doc→docx и т.д.)."""
    # Unwrap arguments if called programmatically
    input_dir = _unwrap(input_dir)
    cycle = _unwrap(cycle)
    from_format = _unwrap(from_format)
    to_format = _unwrap(to_format)
    engine = _unwrap(engine)
    use_headless = _unwrap(use_headless)
    mock_mode = _unwrap(mock_mode)
    protocol_date = _unwrap(protocol_date)
    verbose = _unwrap(verbose)
    dry_run = _unwrap(dry_run)
    pipeline_id = _unwrap(pipeline_id)
    enable_mongo = _unwrap(enable_mongo)

    if not input_dir.exists():
        typer.echo(f"❌ Директория не найдена: {input_dir}", err=True)
        raise typer.Exit(1)

    # ИСПРАВЛЕНО: Не устанавливаем текущую дату если base_dir уже установлен
    from ..engine.classifier import Classifier
    if not protocol_date:
        if Classifier._override_base_dir is None:
            protocol_date = datetime.now().strftime("%Y-%m-%d")

    typer.echo(f"🔄 Конвертация: {input_dir} (цикл {cycle})")

    # Инициализируем MongoDB клиент если требуется
    db_client = None
    if enable_mongo:
        from ..core.database import get_database
        db_client = get_database()

    # Для сбора статистики
    conversion_counter: dict = {}
    unit_results: list = []
    start_time = time.time()

    converter = Converter(use_headless=use_headless, mock_mode=mock_mode)

    def process_unit(unit_path: Path) -> dict:
        """Обработка одного UNIT конвертером."""
        result = converter.convert_unit(
            unit_path=unit_path,
            cycle=cycle,
            from_format=from_format,
            to_format=to_format,
            engine=engine,
            protocol_date=protocol_date,
            dry_run=dry_run,
        )
        files_converted = result.get('files_converted', 0)
        from_ext = result.get('from_extension', 'unknown')
        to_ext = result.get('to_extension', 'unknown')

        # Собираем статистику
        key = f"{from_ext}->{to_ext}"
        conversion_counter[key] = conversion_counter.get(key, 0) + files_converted

        unit_results.append({
            "unit_id": unit_path.name,
            "files_converted": files_converted,
            "from_extension": from_ext,
            "to_extension": to_ext,
            "status": result.get('status', 'success'),
        })

        if verbose:
            typer.echo(f"  ✓ {unit_path.name}: {files_converted} файлов ({from_ext}->{to_ext})")
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

    # Записываем статистику в MongoDB
    if enable_mongo and db_client and pipeline_id and not dry_run:
        try:
            total_files_converted = sum(conversion_counter.values())
            stage_stats = {
                "total_units": results['units_processed'],
                "units_failed": results['units_failed'],
                "total_files_converted": total_files_converted,
                "by_conversion": conversion_counter,
                "processing_time_ms": processing_time_ms,
                "engine": engine,
            }
            db_client.write_stage_stats(
                pipeline_id=pipeline_id,
                cycle=cycle,
                stage="convert",
                stats=stage_stats,
            )

            # Записываем trace для каждого UNIT
            for unit_res in unit_results:
                db_client.write_unit_trace(
                    unit_id=unit_res["unit_id"],
                    pipeline_id=pipeline_id,
                    cycle=cycle,
                    stage="convert",
                    operation="convert",
                    duration_ms=0,
                    status=unit_res["status"],
                    metadata={
                        "files_converted": unit_res["files_converted"],
                        "from_extension": unit_res["from_extension"],
                        "to_extension": unit_res["to_extension"],
                    },
                )

            if verbose:
                typer.echo(f"💾 Статистика конвертации записана в MongoDB")
        except Exception as e:
            logger.warning(f"Failed to write convert stats to MongoDB: {e}")
            typer.echo(f"⚠️  Не удалось записать статистику: {e}", err=True)


@app.command("extract")
def substage_extract_run(
    input_dir: Path = typer.Option(..., "--input", help="Входная директория"),
    cycle: int = typer.Option(1, "--cycle", help="Номер цикла (1, 2, 3)"),
    max_depth: int = typer.Option(2, "--max-depth", help="Максимальная глубина"),
    keep_archive: bool = typer.Option(False, "--keep-archive", help="Сохранять архив"),
    flatten: bool = typer.Option(False, "--flatten", help="Размещать все в одной директории"),
    protocol_date: Optional[str] = typer.Option(None, "--date", help="Дата протокола (YYYY-MM-DD)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим имитации"),
    pipeline_id: Optional[str] = typer.Option(None, "--pipeline-id", help="ID pipeline для статистики"),
    enable_mongo: bool = typer.Option(False, "--enable-mongo", help="Включить запись в MongoDB"),
):
    """Разархивация архивов."""
    # Unwrap arguments
    input_dir = _unwrap(input_dir)
    cycle = _unwrap(cycle)
    max_depth = _unwrap(max_depth)
    keep_archive = _unwrap(keep_archive)
    flatten = _unwrap(flatten)
    protocol_date = _unwrap(protocol_date)
    verbose = _unwrap(verbose)
    dry_run = _unwrap(dry_run)
    pipeline_id = _unwrap(pipeline_id)
    enable_mongo = _unwrap(enable_mongo)

    if not input_dir.exists():
        typer.echo(f"❌ Директория не найдена: {input_dir}", err=True)
        raise typer.Exit(1)

    # ИСПРАВЛЕНО: Не устанавливаем текущую дату если base_dir уже установлен
    from ..engine.classifier import Classifier
    if not protocol_date:
        if Classifier._override_base_dir is None:
            protocol_date = datetime.now().strftime("%Y-%m-%d")

    typer.echo(f"📦 Разархивация: {input_dir} (цикл {cycle})")

    # Инициализируем MongoDB клиент если требуется
    db_client = None
    if enable_mongo:
        from ..core.database import get_database
        db_client = get_database()

    # Для сбора статистики
    archive_type_counter: dict = {}
    unit_results: list = []
    start_time = time.time()

    extractor = Extractor()

    def process_unit(unit_path: Path) -> dict:
        """Обработка одного UNIT экстрактором."""
        result = extractor.extract_unit(
            unit_path=unit_path,
            cycle=cycle,
            max_depth=max_depth,
            keep_archive=keep_archive,
            flatten=flatten,
            protocol_date=protocol_date,
            dry_run=dry_run,
        )
        files_extracted = result.get('files_extracted', 0)
        archive_type = result.get('archive_type', 'unknown')

        # Собираем статистику
        archive_type_counter[archive_type] = archive_type_counter.get(archive_type, 0) + files_extracted

        unit_results.append({
            "unit_id": unit_path.name,
            "files_extracted": files_extracted,
            "archive_type": archive_type,
            "status": result.get('status', 'success'),
        })

        if verbose:
            typer.echo(f"  ✓ {unit_path.name}: {files_extracted} файлов ({archive_type})")
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

    # Записываем статистику в MongoDB
    if enable_mongo and db_client and pipeline_id and not dry_run:
        try:
            total_files_extracted = sum(archive_type_counter.values())
            stage_stats = {
                "total_units": results['units_processed'],
                "units_failed": results['units_failed'],
                "total_files_extracted": total_files_extracted,
                "by_archive_type": archive_type_counter,
                "processing_time_ms": processing_time_ms,
                "max_depth": max_depth,
            }
            db_client.write_stage_stats(
                pipeline_id=pipeline_id,
                cycle=cycle,
                stage="extract",
                stats=stage_stats,
            )

            # Записываем trace для каждого UNIT
            for unit_res in unit_results:
                db_client.write_unit_trace(
                    unit_id=unit_res["unit_id"],
                    pipeline_id=pipeline_id,
                    cycle=cycle,
                    stage="extract",
                    operation="extract",
                    duration_ms=0,
                    status=unit_res["status"],
                    metadata={
                        "files_extracted": unit_res["files_extracted"],
                        "archive_type": unit_res["archive_type"],
                    },
                )

            if verbose:
                typer.echo(f"💾 Статистика извлечения записана в MongoDB")
        except Exception as e:
            logger.warning(f"Failed to write extract stats to MongoDB: {e}")
            typer.echo(f"⚠️  Не удалось записать статистику: {e}", err=True)


@app.command("normalize")
def substage_normalize_name(
    input_dir: Path = typer.Option(..., "--input", help="Входная директория"),
    cycle: int = typer.Option(1, "--cycle", help="Номер цикла (1, 2, 3)"),
    protocol_date: Optional[str] = typer.Option(None, "--date", help="Дата протокола (YYYY-MM-DD)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим имитации"),
):
    """Нормализация имени файла (ТОЛЬКО имя)."""
    # Unwrap arguments
    input_dir = _unwrap(input_dir)
    cycle = _unwrap(cycle)
    protocol_date = _unwrap(protocol_date)
    verbose = _unwrap(verbose)
    dry_run = _unwrap(dry_run)

    if not input_dir.exists():
        typer.echo(f"❌ Директория не найдена: {input_dir}", err=True)
        raise typer.Exit(1)

    # ИСПРАВЛЕНО: Не устанавливаем текущую дату если base_dir уже установлен
    from ..engine.classifier import Classifier
    if not protocol_date:
        if Classifier._override_base_dir is None:
            protocol_date = datetime.now().strftime("%Y-%m-%d")

    typer.echo(f"📝 Нормализация имен: {input_dir} (цикл {cycle})")
    
    normalizer = NameNormalizer()
    
    def process_unit(unit_path: Path) -> dict:
        """Обработка одного UNIT нормализатором имен."""
        result = normalizer.normalize_names(
            unit_path=unit_path,
            cycle=cycle,
            protocol_date=protocol_date,
            dry_run=dry_run,
        )
        if verbose:
            typer.echo(f"  ✓ {unit_path.name}: {result.get('files_normalized', 0)} файлов")
        return result

    results = process_directory_units(
        source_dir=input_dir,
        processor_func=process_unit,
        dry_run=dry_run,
    )

    typer.echo(f"\n✅ Обработано UNIT: {results['units_processed']}")
    if results['units_failed'] > 0:
        typer.echo(f"❌ Ошибок: {results['units_failed']}", err=True)


@app.command("normalize-extension")
def substage_normalize_extension(
    input_dir: Path = typer.Option(..., "--input", help="Входная директория"),
    cycle: int = typer.Option(1, "--cycle", help="Номер цикла (1, 2, 3)"),
    protocol_date: Optional[str] = typer.Option(None, "--date", help="Дата протокола (YYYY-MM-DD)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим имитации"),
):
    """Нормализация расширения (по сигнатурам)."""
    # Unwrap arguments
    input_dir = _unwrap(input_dir)
    cycle = _unwrap(cycle)
    protocol_date = _unwrap(protocol_date)
    verbose = _unwrap(verbose)
    dry_run = _unwrap(dry_run)

    if not input_dir.exists():
        typer.echo(f"❌ Директория не найдена: {input_dir}", err=True)
        raise typer.Exit(1)

    # ИСПРАВЛЕНО: Не устанавливаем текущую дату если base_dir уже установлен
    from ..engine.classifier import Classifier
    if not protocol_date:
        if Classifier._override_base_dir is None:
            protocol_date = datetime.now().strftime("%Y-%m-%d")

    typer.echo(f"🔧 Нормализация расширений: {input_dir} (цикл {cycle})")
    
    normalizer = ExtensionNormalizer()
    
    def process_unit(unit_path: Path) -> dict:
        """Обработка одного UNIT нормализатором расширений."""
        result = normalizer.normalize_extensions(
            unit_path=unit_path,
            cycle=cycle,
            protocol_date=protocol_date,
            dry_run=dry_run,
        )
        if verbose:
            typer.echo(f"  ✓ {unit_path.name}: {result.get('files_normalized', 0)} файлов")
        return result

    results = process_directory_units(
        source_dir=input_dir,
        processor_func=process_unit,
        dry_run=dry_run,
    )

    typer.echo(f"\n✅ Обработано UNIT: {results['units_processed']}")
    if results['units_failed'] > 0:
        typer.echo(f"❌ Ошибок: {results['units_failed']}", err=True)


@app.command("normalize-full")
def substage_normalize_full(
    input_dir: Path = typer.Option(..., "--input", help="Входная директория"),
    cycle: int = typer.Option(1, "--cycle", help="Номер цикла (1, 2, 3)"),
    protocol_date: Optional[str] = typer.Option(None, "--date", help="Дата протокола (YYYY-MM-DD)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Режим имитации"),
    pipeline_id: Optional[str] = typer.Option(None, "--pipeline-id", help="ID pipeline для статистики"),
    enable_mongo: bool = typer.Option(False, "--enable-mongo", help="Включить запись в MongoDB"),
):
    """Полная нормализация (имя + расширение)."""
    # Unwrap arguments
    input_dir = _unwrap(input_dir)
    cycle = _unwrap(cycle)
    protocol_date = _unwrap(protocol_date)
    verbose = _unwrap(verbose)
    dry_run = _unwrap(dry_run)
    pipeline_id = _unwrap(pipeline_id)
    enable_mongo = _unwrap(enable_mongo)

    if not input_dir.exists():
        typer.echo(f"❌ Директория не найдена: {input_dir}", err=True)
        raise typer.Exit(1)

    # ИСПРАВЛЕНО: Не устанавливаем текущую дату если base_dir уже установлен
    from ..engine.classifier import Classifier
    if not protocol_date:
        if Classifier._override_base_dir is None:
            protocol_date = datetime.now().strftime("%Y-%m-%d")

    typer.echo(f"✨ Полная нормализация: {input_dir} (цикл {cycle})")

    # Инициализируем MongoDB клиент если требуется
    db_client = None
    if enable_mongo:
        from ..core.database import get_database
        db_client = get_database()

    # Для сбора статистики
    names_normalized = 0
    extensions_normalized = 0
    unit_results: list = []
    start_time = time.time()

    name_normalizer = NameNormalizer()
    ext_normalizer = ExtensionNormalizer()

    def process_unit(unit_path: Path) -> dict:
        """Обработка одного UNIT полной нормализацией."""
        # Сначала нормализуем имена
        name_result = name_normalizer.normalize_names(
            unit_path=unit_path,
            cycle=cycle,
            protocol_date=protocol_date,
            dry_run=dry_run,
        )
        # Затем нормализуем расширения (на обновленном пути)
        updated_path = Path(name_result.get("target_directory", unit_path))
        ext_result = ext_normalizer.normalize_extensions(
            unit_path=updated_path,
            cycle=cycle,
            protocol_date=protocol_date,
            dry_run=dry_run,
        )

        names_count = name_result.get('files_normalized', 0)
        ext_count = ext_result.get('files_normalized', 0)

        # Собираем статистику
        nonlocal names_normalized, extensions_normalized
        names_normalized += names_count
        extensions_normalized += ext_count

        unit_results.append({
            "unit_id": unit_path.name,
            "names_normalized": names_count,
            "extensions_normalized": ext_count,
            "status": name_result.get('status', 'success'),
        })

        if verbose:
            typer.echo(f"  ✓ {unit_path.name}: {names_count} имен, {ext_count} расширений")
        return {
            "name_normalization": name_result,
            "extension_normalization": ext_result,
        }

    results = process_directory_units(
        source_dir=input_dir,
        processor_func=process_unit,
        dry_run=dry_run,
    )

    processing_time_ms = int((time.time() - start_time) * 1000)

    typer.echo(f"\n✅ Обработано UNIT: {results['units_processed']}")
    if results['units_failed'] > 0:
        typer.echo(f"❌ Ошибок: {results['units_failed']}", err=True)

    # Записываем статистику в MongoDB
    if enable_mongo and db_client and pipeline_id and not dry_run:
        try:
            stage_stats = {
                "total_units": results['units_processed'],
                "units_failed": results['units_failed'],
                "total_names_normalized": names_normalized,
                "total_extensions_normalized": extensions_normalized,
                "processing_time_ms": processing_time_ms,
            }
            db_client.write_stage_stats(
                pipeline_id=pipeline_id,
                cycle=cycle,
                stage="normalize",
                stats=stage_stats,
            )

            # Записываем trace для каждого UNIT
            for unit_res in unit_results:
                db_client.write_unit_trace(
                    unit_id=unit_res["unit_id"],
                    pipeline_id=pipeline_id,
                    cycle=cycle,
                    stage="normalize",
                    operation="normalize_full",
                    duration_ms=0,
                    status=unit_res["status"],
                    metadata={
                        "names_normalized": unit_res["names_normalized"],
                        "extensions_normalized": unit_res["extensions_normalized"],
                    },
                )

            if verbose:
                typer.echo(f"💾 Статистика нормализации записана в MongoDB")
        except Exception as e:
            logger.warning(f"Failed to write normalize stats to MongoDB: {e}")
            typer.echo(f"⚠️  Не удалось записать статистику: {e}", err=True)



"""
Converter - конвертация файлов между форматами (doc→docx, xls→xlsx и т.д.).

Оптимизирован для параллельной конвертации на многоядерных системах.
"""
import subprocess
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List

from ..core.manifest import load_manifest, save_manifest, update_manifest_operation
from ..core.audit import get_audit_logger
from ..core.exceptions import OperationError, QuarantineError
from ..core.state_machine import UnitState
from ..core.unit_processor import (
    move_unit_to_target,
    update_unit_state,
    determine_unit_extension,
    get_extension_subdirectory,
)
from ..core.config import get_cycle_paths, MERGE_DIR, get_data_paths
from ..core.libreoffice_converter import RobustDocumentConverter
from ..core.parallel import calculate_optimal_workers, get_parallel_config
from ..utils.file_ops import detect_file_type

logger = logging.getLogger(__name__)


class Converter:
    """Конвертер файлов через LibreOffice."""

    # Поддерживаемые конвертации (source_format -> target_format)
    CONVERSION_MAP = {
        "doc": "docx",
        "xls": "xlsx",
        "ppt": "pptx",
        "rtf": "docx",  # RTF конвертируем в DOCX
    }

    # Маппинг форматов для LibreOffice (target_format -> LibreOffice format string)
    # LibreOffice использует формат в виде расширения для --convert-to
    LIBREOFFICE_FORMAT_MAP = {
        "docx": "docx",
        "xlsx": "xlsx",
        "pptx": "pptx",
    }

    # Константы для расчёта динамического timeout
    # Формула: BASE + (file_size_mb * PER_MB), max = MAX
    TIMEOUT_BASE_SECONDS = 60       # Базовый timeout для любого файла
    TIMEOUT_PER_MB_SECONDS = 30     # Дополнительные секунды на каждый MB
    TIMEOUT_MAX_SECONDS = 600       # Максимальный timeout (10 минут)

    # UnitEvents интеграция
    _tracker_run_id: Optional[str] = None
    _db_client: Optional[Any] = None

    @classmethod
    def set_tracker_run_id(cls, run_id: str, db_client: Optional[Any] = None) -> None:
        """
        Устанавливает tracker_run_id для UnitEvents записи.

        Args:
            run_id: PipelineTracker run_id
            db_client: Опциональный DocPrepDatabase клиент
        """
        cls._tracker_run_id = run_id
        cls._db_client = db_client

    @classmethod
    def clear_tracker_run_id(cls) -> None:
        """Очищает tracker_run_id."""
        cls._tracker_run_id = None
        cls._db_client = None

    def _get_registration_number(self, unit_path: Path) -> str:
        """
        Извлекает registrationNumber из unit.meta.json или manifest.json.

        Args:
            unit_path: Путь к директории UNIT

        Returns:
            Registration number или пустая строка если не найден
        """
        import json

        # 1. Пытаемся прочитать unit.meta.json
        meta_path = unit_path / "unit.meta.json"
        if meta_path.exists():
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                    reg_num = meta.get("registrationNumber") or meta.get("registration_number", "")
                    if reg_num:
                        return reg_num
            except (json.JSONDecodeError, IOError):
                pass

        # 2. Fallback: manifest.json
        manifest_path = unit_path / "manifest.json"
        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
                    reg_num = manifest.get("registrationNumber") or manifest.get("registration_number", "")
                    if reg_num:
                        return reg_num
            except (json.JSONDecodeError, IOError):
                pass

        return ""  # Пустая строка если не найден

    def _record_convert_event(
        self,
        unit_id: str,
        registration_number: str,
        source_format: str,
        target_format: str,
        files_converted: int,
        duration_ms: int,
        status: str = "success",
        error: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Записывает UnitEvent для конвертации в PipelineTracker.

        Args:
            unit_id: Идентификатор UNIT
            registration_number: Регистрационный номер (primary trace ID)
            source_format: Исходный формат
            target_format: Целевой формат
            files_converted: Количество конвертированных файлов
            duration_ms: Длительность операции в миллисекундах
            status: Статус операции ("success" или "failed")
            error: Описание ошибки (если есть)
            metrics: Дополнительные метрики
        """
        if not Converter._tracker_run_id or not Converter._db_client:
            return

        try:
            from docreciv.pipeline.events import EventType, EventStatus, Stage

            event_metrics = {
                "source_format": source_format,
                "target_format": target_format,
                "files_converted": files_converted,
            }
            if metrics:
                event_metrics.update(metrics)

            Converter._db_client.record_unit_event(
                unit_id=unit_id,
                run_id=Converter._tracker_run_id,
                registration_number=registration_number,
                event_type=EventType.PROCESSED,  # PROCESSED вместо CONVERTED (нет такого типа в EventType)
                stage=Stage.DOCPREP,
                status=EventStatus.COMPLETED if status == "success" else EventStatus.FAILED,
                metrics=event_metrics,
                error=error,
                duration_ms=duration_ms
            )
        except ImportError as e:
            logger.debug(f"PipelineTracker events not available: {e}")
        except Exception as e:
            logger.warning(f"Failed to record convert event for {unit_id}: {e}")

    def __init__(self, libreoffice_path: str = "libreoffice", use_headless: bool = True, mock_mode: bool = False):
        """
        Инициализирует Converter.

        Args:
            libreoffice_path: Путь к LibreOffice (по умолчанию "libreoffice")
            use_headless: Использовать headless конвертер с Xvfb (по умолчанию True).
                         Решает проблемы с X11/dconf в серверном окружении.
            mock_mode: Режим симуляции для тестирования
        """
        self.libreoffice_path = libreoffice_path
        self.use_headless = use_headless
        self.mock_mode = mock_mode

        # Инициализируем headless конвертер если нужно
        if use_headless:
            from ..core.libreoffice_converter import RobustDocumentConverter
            self.headless_converter = RobustDocumentConverter(mock_mode=mock_mode)

        self.audit_logger = get_audit_logger()

    def convert_unit(
        self,
        unit_path: Path,
        cycle: int,
        from_format: Optional[str] = None,
        to_format: Optional[str] = None,
        engine: str = "libreoffice",
        protocol_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Конвертирует все файлы в UNIT, перемещает UNIT в целевую директорию и обновляет state.

        Args:
            unit_path: Путь к директории UNIT
            cycle: Номер цикла (1, 2, 3)
            from_format: Исходный формат (опционально, определяется автоматически)
            to_format: Целевой формат (опционально, определяется автоматически)
            engine: Движок конвертации (по умолчанию "libreoffice")
            protocol_date: Дата протокола для организации по датам (опционально)
            dry_run: Если True, только показывает что будет сделано

        Returns:
            Словарь с результатами конвертации:
            - unit_id: идентификатор UNIT
            - files_converted: количество конвертированных файлов
            - files_failed: количество ошибок
            - converted_files: список конвертированных файлов
            - errors: список ошибок
            - moved_to: путь к новой директории UNIT (после перемещения)
        """
        unit_id = unit_path.name
        correlation_id = self.audit_logger.get_correlation_id()

        # UnitEvents: начинаем отслеживание времени
        import time
        start_time = time.time()
        registration_number = self._get_registration_number(unit_path)

        # Загружаем manifest
        manifest_path = unit_path / "manifest.json"
        try:
            manifest = load_manifest(unit_path)
            current_cycle = manifest.get("processing", {}).get("current_cycle", cycle)
            if not protocol_date:
                protocol_date = manifest.get("protocol_date")
        except FileNotFoundError:
            manifest = None
            current_cycle = cycle
            logger.warning(f"Manifest not found for unit {unit_id}, using cycle {cycle}")

        # Находим файлы для конвертации
        files_to_convert = []
        all_files = [
            f for f in unit_path.rglob("*") if f.is_file() and f.name not in ["manifest.json", "audit.log.jsonl"]
        ]

        for file_path in all_files:
            detection = detect_file_type(file_path)
            detected_type = detection.get("detected_type")

            # Определяем формат конвертации
            if from_format is None:
                source_format = detected_type
                # Fallback на расширение, если detected_type не поддерживается
                if source_format not in self.CONVERSION_MAP:
                    ext = file_path.suffix.lower().lstrip(".")
                    if ext in self.CONVERSION_MAP:
                        source_format = ext

            else:
                source_format = from_format

            if source_format in self.CONVERSION_MAP:

                target_format = to_format or self.CONVERSION_MAP[source_format]
                files_to_convert.append((file_path, source_format, target_format))

        if not files_to_convert:
            logger.warning(f"No files to convert in unit {unit_id} - moving to Exceptions")
            
            # Определяем целевую директорию в Exceptions
            # ИСПРАВЛЕНО: Получаем base_dir от Classifier для правильных путей
            from ..engine.classifier import Classifier
            effective_base_dir = Classifier._override_base_dir
            if protocol_date:
                data_paths = get_data_paths(protocol_date, base_dir=effective_base_dir)
                exceptions_base = data_paths["exceptions"]
            else:
                from ..core.config import EXCEPTIONS_DIR
                exceptions_base = EXCEPTIONS_DIR
            
            # НОВАЯ СТРУКТУРА v2: Exceptions/Direct для цикла 1, Exceptions/Processing_N для остальных
            if current_cycle == 1:
                target_base_dir = exceptions_base / "Direct" / "NoProcessableFiles"
            else:
                target_base_dir = exceptions_base / f"Processing_{current_cycle}" / "NoProcessableFiles"
            
            # Перемещаем в Exceptions
            target_dir = move_unit_to_target(
                unit_dir=unit_path,
                target_base_dir=target_base_dir,
                extension=None,
                dry_run=dry_run,
            )
            
            # Обновляем состояние (только если не dry_run)
            if not dry_run:
                exception_state_map = {
                    1: UnitState.EXCEPTION_1,
                    2: UnitState.EXCEPTION_2,
                    3: UnitState.EXCEPTION_3,
                }
                new_state = exception_state_map.get(current_cycle, UnitState.EXCEPTION_1)

                update_unit_state(
                    unit_path=target_dir,
                    new_state=new_state,
                    cycle=current_cycle,
                    operation={
                        "type": "convert",
                        "status": "skipped",
                        "reason": "no_processable_files",
                    },
                )

            # UnitEvents: записываем событие для случая без файлов
            duration_ms = int((time.time() - start_time) * 1000)
            self._record_convert_event(
                unit_id=unit_id,
                registration_number=registration_number,
                source_format="unknown",
                target_format="unknown",
                files_converted=0,
                duration_ms=duration_ms,
                status="skipped",
                error="No files found that require conversion"
            )

            return {
                "unit_id": unit_id,
                "files_converted": 0,
                "files_failed": 0,
                "converted_files": [],
                "errors": [{"error": "No files found that require conversion"}],
                "moved_to": str(target_dir),
            }

        converted_files = []
        errors = []
        target_format_used = None

        # МНОГОПОТОЧНАЯ КОНВЕРТАЦИЯ
        if dry_run:
            # В dry_run режиме обрабатываем последовательно (без многопоточности)
            for file_path, source_format, target_format in files_to_convert:
                logger.info(f"[DRY RUN] Would convert {file_path.name} from {source_format} to {target_format}")
                converted_files.append({
                    "original_file": str(file_path),
                    "output_path": str(file_path.parent / (file_path.stem + "." + target_format)),
                    "source_format": source_format,
                    "target_format": target_format,
                    "success": True,
                })
                target_format_used = target_format
        else:
            # Реальная конвертация с многопоточностью
            from concurrent.futures import ThreadPoolExecutor, as_completed

            # ОПТИМИЗАЦИЯ: Динамический расчёт workers на основе CPU и памяти
            # На 64-ядерном сервере с 16GB RAM: до 6 workers
            # Каждый LibreOffice процесс потребляет 500MB-1GB RAM
            config = get_parallel_config()
            if config.enabled:
                optimal_workers = calculate_optimal_workers("converter")
            else:
                optimal_workers = 1
            max_workers = min(optimal_workers, len(files_to_convert))
            logger.info(f"Starting parallel conversion with {max_workers} workers for {len(files_to_convert)} files")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Создаем задачи для каждого файла
                future_to_file = {
                    executor.submit(
                        self._convert_file,
                        file_path,
                        source_format,
                        target_format,
                        engine
                    ): (file_path, source_format, target_format)
                    for file_path, source_format, target_format in files_to_convert
                }

                # Обрабатываем результаты по мере завершения
                for future in as_completed(future_to_file):
                    file_path, source_format, target_format = future_to_file[future]
                    try:
                        result = future.result()
                        if result.get("success"):
                            converted_files.append(result)
                            target_format_used = target_format

                            # Обновляем manifest
                            if manifest:
                                operation = {
                                    "type": "convert",
                                    "status": "success",
                                    "from": source_format,
                                    "to": target_format,
                                    "cycle": current_cycle,
                                    "tool": engine,
                                    "original_file": str(file_path.name),
                                    "converted_file": str(Path(result.get("output_path")).name),
                                    "validated": result.get("validated", False),
                                    "validated_type": result.get("validated_type"),
                                }
                                manifest = update_manifest_operation(manifest, operation)

                                # Обновляем информацию о файле в manifest
                                files = manifest.get("files", [])
                                for file_info in files:
                                    if file_info.get("original_name") == file_path.name or file_info.get("current_name") == file_path.name:
                                        # Обновляем current_name на конвертированный файл
                                        file_info["current_name"] = Path(result.get("output_path")).name
                                        file_info["detected_type"] = target_format
                                        # Добавляем информацию о трансформации
                                        if "transformations" not in file_info:
                                            file_info["transformations"] = []
                                        file_info["transformations"].append({
                                            "type": "convert",
                                            "from": source_format,
                                            "to": target_format,
                                            "cycle": current_cycle,
                                        })
                                        break
                        else:
                            errors.append({"file": str(file_path), "error": "Conversion returned success=False"})
                    except Exception as e:
                        error_str = str(e)
                        error_details = None

                        # ПРОВЕРКА: Если ошибка из-за неверного расширения, пробуем нормализовать
                        if "wrong extension" in error_str or "Requires normalization" in error_str:
                            logger.info(f"File {file_path.name} needs normalization instead of conversion")

                            # Пытаемся получить детали ошибки из OperationError
                            if hasattr(e, 'operation_details'):
                                error_details = e.operation_details

                            # Перенаправляем на нормализацию расширения
                            try:
                                # Выполняем нормализацию (переименование)
                                detection = detect_file_type(file_path)
                                correct_ext = detection.get("correct_extension", ".html")
                                current_ext = file_path.suffix

                                if current_ext != correct_ext:
                                    # Переименовываем файл
                                    new_path = file_path.parent / (file_path.stem + correct_ext)
                                    file_path.rename(new_path)
                                    logger.info(f"Normalized extension: {file_path.name} -> {new_path.name}")

                                    # Обновляем manifest
                                    if manifest:
                                        operation = {
                                            "type": "normalize",
                                            "status": "success",
                                            "subtype": "extension",
                                            "original_extension": current_ext,
                                            "correct_extension": correct_ext,
                                            "detected_type": detection.get("detected_type"),
                                            "cycle": current_cycle,
                                            "fallback_from_convert": True,
                                        }
                                        manifest = update_manifest_operation(manifest, operation)

                                        # Обновляем информацию о файле в manifest
                                        for file_info in manifest.get("files", []):
                                            if file_info.get("original_name") == file_path.name or file_info.get("current_name") == file_path.name:
                                                file_info["current_name"] = new_path.name
                                                file_info["detected_type"] = detection.get("detected_type")
                                                if "transformations" not in file_info:
                                                    file_info["transformations"] = []
                                                file_info["transformations"].append({
                                                    "type": "normalize",
                                                    "from": current_ext,
                                                    "to": correct_ext,
                                                    "cycle": current_cycle,
                                                })
                                                break

                                    # После нормализации проверяем - требуется ли теперь конвертация?
                                    # (например, если файл был .xls → .xlsx, а не .doc → .html)
                                    if detection.get("detected_type") in self.CONVERSION_MAP:
                                        # Файл всё ещё требует конвертации - пробуем снова
                                        new_source_format = detection.get("detected_type")
                                        new_target_format = self.CONVERSION_MAP[new_source_format]
                                        logger.info(f"After normalization, converting {new_path.name} from {new_source_format} to {new_target_format}")

                                        try:
                                            result = self._convert_file(new_path, new_source_format, new_target_format, engine)
                                            if result.get("success"):
                                                converted_files.append({
                                                    "original_file": str(file_path),  # Оригинальный путь
                                                    "output_path": str(Path(result.get("output_path"))),
                                                    "source_format": new_source_format,
                                                    "target_format": new_target_format,
                                                    "success": True,
                                                    "normalized_first": True,
                                                })
                                                target_format_used = new_target_format

                                                # Обновляем manifest
                                                if manifest:
                                                    operation = {
                                                        "type": "convert",
                                                        "status": "success",
                                                        "from": new_source_format,
                                                        "to": new_target_format,
                                                        "cycle": current_cycle,
                                                        "tool": engine,
                                                        "original_file": str(file_path.name),
                                                        "converted_file": str(Path(result.get("output_path")).name),
                                                        "validated": result.get("validated", False),
                                                        "validated_type": result.get("validated_type"),
                                                    }
                                                    manifest = update_manifest_operation(manifest, operation)
                                        except Exception as convert_error:
                                            # Конвертация после нормализации тоже не удалась
                                            errors.append({
                                                "file": str(file_path),
                                                "error": f"Normalize succeeded but conversion failed: {convert_error}"
                                            })
                                    else:
                                        # После нормализации файл готов (например HTML)
                                        converted_files.append({
                                            "original_file": str(file_path),
                                            "output_path": str(new_path),
                                            "source_format": current_ext,
                                            "target_format": correct_ext,
                                            "success": True,
                                            "normalized_only": True,
                                        })
                                        logger.info(f"File {file_path.name} normalized to {correct_ext}, no conversion needed")
                                else:
                                    errors.append({"file": str(file_path), "error": "Normalization failed: extension already correct"})
                            except Exception as normalize_error:
                                errors.append({"file": str(file_path), "error": f"Normalize failed: {normalize_error}"})
                                logger.error(f"Failed to normalize {file_path}: {normalize_error}")
                        else:
                            # Обычная ошибка конвертации (не связана с неверным расширением)
                            errors.append({"file": str(file_path), "error": str(e)})
                        logger.error(f"Failed to convert {file_path}: {e}")

        # Если не было успешных конвертаций, перемещаем в Exceptions
        if not converted_files and not dry_run:
            logger.warning(f"No files were successfully converted in unit {unit_id} - moving to Exceptions")
            
            # Определяем целевую директорию в Exceptions
            # ИСПРАВЛЕНО: Получаем base_dir от Classifier для правильных путей
            from ..engine.classifier import Classifier
            effective_base_dir = Classifier._override_base_dir
            if protocol_date:
                data_paths = get_data_paths(protocol_date, base_dir=effective_base_dir)
                exceptions_base = data_paths["exceptions"]
            else:
                from ..core.config import EXCEPTIONS_DIR
                exceptions_base = EXCEPTIONS_DIR
            
            # НОВАЯ СТРУКТУРА v2: Exceptions/Direct для цикла 1, Exceptions/Processing_N для остальных
            if current_cycle == 1:
                target_base_dir = exceptions_base / "Direct" / "ErConvert"
            else:
                target_base_dir = exceptions_base / f"Processing_{current_cycle}" / "ErConvert"
            
            # Перемещаем в Exceptions
            target_dir = move_unit_to_target(
                unit_dir=unit_path,
                target_base_dir=target_base_dir,
                extension=None,
                dry_run=dry_run,
            )
            
            # Обновляем состояние в EXCEPTION_N
            exception_state_map = {
                1: UnitState.EXCEPTION_1,
                2: UnitState.EXCEPTION_2,
                3: UnitState.EXCEPTION_3,
            }
            new_state = exception_state_map.get(current_cycle, UnitState.EXCEPTION_1)
            
            update_unit_state(
                unit_path=target_dir,
                new_state=new_state,
                cycle=current_cycle,
                operation={
                    "type": "convert",
                    "status": "failed",
                    "errors": errors,
                },
            )

            # UnitEvents: записываем событие для неудачной конвертации
            duration_ms = int((time.time() - start_time) * 1000)
            error_msg = "; ".join([e.get("error", "unknown") for e in errors]) if errors else "Conversion failed"
            self._record_convert_event(
                unit_id=unit_id,
                registration_number=registration_number,
                source_format="unknown",
                target_format="unknown",
                files_converted=0,
                duration_ms=duration_ms,
                status="failed",
                error=error_msg
            )

            return {
                "unit_id": unit_id,
                "files_converted": 0,
                "files_failed": len(errors),
                "converted_files": [],
                "errors": errors,
                "moved_to": str(target_dir),
            }

        # Сохраняем обновленный manifest
        if manifest:
            save_manifest(unit_path, manifest)

        # Определяем следующий цикл (после конвертации переходим к следующему циклу)
        next_cycle = min(current_cycle + 1, 3)

        # Определяем расширение для сортировки (используем целевой формат после конвертации)
        # Для Mixed units используем "Mixed" вместо расширения файла
        if manifest and manifest.get("is_mixed", False):
            extension = "Mixed"
        else:
            extension = target_format_used if target_format_used else determine_unit_extension(unit_path)

        # Перемещаем НАПРЯМУЮ в Merge_N/Converted/ (без Processing_N+1/Direct/)
        # Правильный путь: Data/YYYY-MM-DD/Merge, а не Data/Merge/YYYY-MM-DD
        # ИСПРАВЛЕНО: Получаем base_dir от Classifier для правильных путей
        from ..engine.classifier import Classifier
        effective_base_dir = Classifier._override_base_dir

        if protocol_date and effective_base_dir is None:
            # Если указана дата но base_dir не установлен, используем структуру Data/date/Merge
            from ..core.config import DATA_BASE_DIR
            merge_base = DATA_BASE_DIR / protocol_date / "Merge"
        else:
            # Используем base_dir если установлен, иначе MERGE_DIR
            if effective_base_dir is not None:
                merge_base = effective_base_dir / "Merge"
            else:
                merge_base = MERGE_DIR

        cycle_paths = get_cycle_paths(current_cycle, None, merge_base, None)
        target_base_dir = cycle_paths["merge"] / "Converted"

        # Определяем новое состояние ПЕРЕД перемещением
        # Проверяем текущее состояние из manifest
        from ..core.state_machine import UnitStateMachine
        state_machine = UnitStateMachine(unit_id, manifest_path)
        current_state = state_machine.get_current_state()
        
        # Определяем целевое состояние
        if current_state == UnitState.CLASSIFIED_1:
            # Из CLASSIFIED_1 переходим в PENDING_CONVERT, затем в CLASSIFIED_2
            # Сначала переводим в PENDING_CONVERT (если не dry_run)
            if not dry_run:
                update_unit_state(
                    unit_path=unit_path,
                    new_state=UnitState.PENDING_CONVERT,
                    cycle=current_cycle,
                    operation={
                        "type": "convert",
                        "status": "pending",
                    },
                )
            # Целевое состояние после конвертации
            new_state = UnitState.CLASSIFIED_2
        elif current_state == UnitState.PENDING_CONVERT:
            # Уже в PENDING_CONVERT, переводим в CLASSIFIED_2
            new_state = UnitState.CLASSIFIED_2
        elif current_cycle == 2:
            # Для цикла 2 переходим в CLASSIFIED_3
            new_state = UnitState.CLASSIFIED_3
        else:
            # Для цикла 3 или выше - финальное состояние
            new_state = UnitState.MERGED_PROCESSED

        # Перемещаем UNIT в целевую директорию с учетом расширения
        target_dir = move_unit_to_target(
            unit_dir=unit_path,
            target_base_dir=target_base_dir,
            extension=extension,
            dry_run=dry_run,
        )

        # Обновляем state machine после перемещения (если не dry_run)
        if not dry_run:
            # Перезагружаем state machine из нового местоположения
            new_manifest_path = target_dir / "manifest.json"
            state_machine = UnitStateMachine(unit_id, new_manifest_path)
            current_state_after_move = state_machine.get_current_state()
            
            # Переходим в целевое состояние
            update_unit_state(
                unit_path=target_dir,
                new_state=new_state,
                cycle=next_cycle,
                operation={
                    "type": "convert",
                    "files_converted": len(converted_files),
                    "target_format": target_format_used,
                },
            )

        # Логируем операцию
        self.audit_logger.log_event(
            unit_id=unit_id,
            event_type="operation",
            operation="convert",
            details={
                "cycle": current_cycle,
                "files_converted": len(converted_files),
                "files_failed": len(errors),
                "target_format": target_format_used,
                "extension": extension,
                "target_directory": str(target_dir),
                "errors": errors,
            },
            state_before=manifest.get("state_machine", {}).get("current_state") if manifest else None,
            state_after=new_state.value,
            unit_path=target_dir,
        )

        # UnitEvents: записываем событие успешной конвертации
        duration_ms = int((time.time() - start_time) * 1000)
        source_format_used = files_to_convert[0][1] if files_to_convert else "unknown"
        self._record_convert_event(
            unit_id=unit_id,
            registration_number=registration_number,
            source_format=source_format_used,
            target_format=target_format_used or "unknown",
            files_converted=len(converted_files),
            duration_ms=duration_ms,
            status="success",
            metrics={"files_failed": len(errors), "extension": extension}
        )

        return {
            "unit_id": unit_id,
            "files_converted": len(converted_files),
            "files_failed": len(errors),
            "converted_files": converted_files,
            "errors": errors,
            "moved_to": str(target_dir),
            "next_cycle": next_cycle,
            "extension": extension,
        }

    def _calculate_timeout(self, file_size_mb: float) -> int:
        """
        Вычисляет динамический timeout на основе размера файла.

        Формула: TIMEOUT_BASE_SECONDS + (file_size_mb * TIMEOUT_PER_MB_SECONDS)
        Минимум: TIMEOUT_BASE_SECONDS, Максимум: TIMEOUT_MAX_SECONDS

        Args:
            file_size_mb: Размер файла в мегабайтах

        Returns:
            Timeout в секундах
        """
        timeout = self.TIMEOUT_BASE_SECONDS + int(file_size_mb * self.TIMEOUT_PER_MB_SECONDS)
        return min(timeout, self.TIMEOUT_MAX_SECONDS)

    def _convert_file(
        self, file_path: Path, source_format: str, target_format: str, engine: str
    ) -> Dict[str, Any]:
        """
        Конвертирует один файл.

        Args:
            file_path: Путь к исходному файлу
            source_format: Исходный формат
            target_format: Целевой формат
            engine: Движок конвертации

        Returns:
            Словарь с результатами конвертации

        Raises:
            OperationError: Если конвертация не удалась
        """
        if engine != "libreoffice":
            raise OperationError(f"Unsupported conversion engine: {engine}", operation="convert")

        # Используем headless конвертер если включен
        if self.use_headless and hasattr(self, 'headless_converter'):
            logger.info(f"🔄 Using headless converter for {file_path.name}")
            output_path = self.headless_converter.convert_document(file_path, file_path.parent)

            if output_path and output_path.exists():
                return {
                    "original_file": str(file_path),
                    "output_path": str(output_path),
                    "source_format": source_format,
                    "target_format": target_format,
                    "success": True,
                }
            else:
                raise OperationError(
                    f"Headless conversion failed for {file_path}",
                    operation="convert_headless"
                )

        # Определяем формат для LibreOffice
        # LibreOffice использует формат в виде расширения (без точки)
        libreoffice_format = self.LIBREOFFICE_FORMAT_MAP.get(target_format, target_format)

        # Определяем выходной путь
        output_dir = file_path.parent
        output_name = file_path.stem + "." + target_format
        output_path = output_dir / output_name

        # Конвертация через LibreOffice в headless режиме
        try:
            # Вычисляем динамический timeout на основе размера файла
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            dynamic_timeout = self._calculate_timeout(file_size_mb)
            logger.info(f"Converting {file_path.name} ({file_size_mb:.2f} MB) with timeout {dynamic_timeout}s")

            cmd = [
                self.libreoffice_path,
                "--headless",
                "--convert-to",
                libreoffice_format,  # Используем правильный формат для LibreOffice
                "--outdir",
                str(output_dir),
                str(file_path),
            ]

            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=dynamic_timeout
            )

            if result.returncode != 0:
                raise OperationError(
                    f"LibreOffice conversion failed: {result.stderr}",
                    operation="convert",
                    operation_details={"returncode": result.returncode, "stderr": result.stderr},
                )

            # Проверяем, что выходной файл создан
            # LibreOffice может создавать файл с другим именем (например, с пробелами)
            if not output_path.exists():
                # Пробуем найти файл с другим именем в той же директории
                output_dir_files = list(output_dir.glob(f"{file_path.stem}.*"))
                # Исключаем исходный файл
                output_dir_files = [f for f in output_dir_files if f.suffix.lower() != file_path.suffix.lower()]
                if output_dir_files:
                    # Берем первый найденный файл с правильным расширением
                    for found_file in output_dir_files:
                        if found_file.suffix.lower() == f".{target_format}":
                            output_path = found_file
                            break
                    else:
                        # Если не нашли с правильным расширением, берем первый
                        output_path = output_dir_files[0]
                else:
                    # Дополнительная проверка - ищем любой файл с целевым расширением
                    all_target_files = list(output_dir.glob(f"*.{target_format}"))
                    if all_target_files:
                        # Берем первый найденный файл с целевым расширением
                        output_path = all_target_files[0]
                    else:
                        raise OperationError(
                            f"Converted file not found: {output_path}. LibreOffice stdout: {result.stdout[:200] if result.stdout else 'empty'}",
                            operation="convert",
                        )

            # ВАЛИДАЦИЯ конвертированного файла через magic bytes
            validation_result = detect_file_type(output_path)
            validated_type = validation_result.get("detected_type", "")

            # Определяем ожидаемые типы для каждого целевого формата
            expected_types = {
                "docx": ["docx"],
                "xlsx": ["xlsx"],
                "pptx": ["pptx"],
                "pdf": ["pdf"],
            }
            expected_list = expected_types.get(target_format, [target_format])

            if validated_type not in expected_list:
                # Валидация провалилась - удаляем некорректный файл
                if output_path.exists():
                    try:
                        output_path.unlink()
                        logger.warning(f"Deleted invalid converted file: {output_path}")
                    except Exception as e:
                        logger.error(f"Failed to delete invalid file {output_path}: {e}")

                raise OperationError(
                    f"Converted file validation failed: expected {target_format}, got {validated_type}",
                    operation="convert",
                    operation_details={
                        "expected_format": target_format,
                        "detected_format": validated_type,
                        "validation_result": validation_result,
                        "file_path": str(output_path),
                    }
                )

            logger.info(f"✅ Validation passed: {output_path.name} is valid {validated_type}")

            # Удаляем исходный файл после успешной конвертации и валидации
            if file_path.exists() and output_path.exists():
                try:
                    file_path.unlink()
                except Exception as e:
                    logger.warning(f"Failed to remove original file {file_path}: {e}")

            return {
                "original_file": str(file_path),
                "output_path": str(output_path),
                "source_format": source_format,
                "target_format": target_format,
                "success": True,
                "validated": True,
                "validated_type": validated_type,
            }

        except subprocess.TimeoutExpired:
            raise OperationError(
                f"Conversion timeout for {file_path}",
                operation="convert",
            )
        except OperationError:
            # ПРОВЕРКА: Если конвертация не удалась, проверяем реальный тип файла
            # Возможно это HTML/XML/TXT с неверным расширением (например HTML в .doc)
            logger.warning(f"Conversion failed for {file_path.name}, checking real file type...")

            post_error_detection = detect_file_type(file_path)
            real_type = post_error_detection.get("detected_type")
            classification = post_error_detection.get("classification")
            extension_mismatch = post_error_detection.get("extension_matches_content", True)

            # Если файл имеет неверное расширение и требует нормализации
            if (classification == "normalize" and
                real_type in ["html", "xml", "txt"] and
                not extension_mismatch):
                # Файл имеет неверное расширение - возвращаем специальную ошибку
                # с флагом для запуска нормализации вместо конвертации
                raise OperationError(
                    f"File has wrong extension: .{source_format} but detected as {real_type}. "
                    f"Requires normalization instead of conversion.",
                    operation="convert",
                    operation_details={
                        "original_format": source_format,
                        "detected_type": real_type,
                        "correct_extension": post_error_detection.get("correct_extension"),
                        "suggested_action": "normalize",
                        "requires_normalization": True,
                    }
                )
            # Иначе пробрасываем оригинальную ошибку
            raise
        except Exception as e:
            raise OperationError(
                f"Conversion error: {str(e)}",
                operation="convert",
                operation_details={"exception": type(e).__name__},
            )

    def convert_unit_headless(
        self,
        unit_path: Path,
        cycle: int,
        protocol_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Конвертирует UNIT с использованием headless LibreOffice.

        Решает проблему dconf permission denied в headless окружении.

        Args:
            unit_path: Путь к директории UNIT
            cycle: Номер цикла
            protocol_date: Дата протокола
            dry_run: Режим проверки

        Returns:
            Результаты конвертации
        """
        unit_id = unit_path.name
        audit_logger = get_audit_logger()

        logger.info(f"🔄 Converting UNIT {unit_id} with headless LibreOffice")

        # ИСПРАВЛЕНО: Получаем целевую директорию для цикла с учётом base_dir
        from ..engine.classifier import Classifier
        effective_base_dir = Classifier._override_base_dir
        if effective_base_dir is not None:
            merge_base = effective_base_dir / "Merge"
        else:
            from ..core.config import MERGE_DIR
            merge_base = MERGE_DIR
        cycle_paths = get_cycle_paths(cycle, None, merge_base, None)
        target_base_dir = cycle_paths["merge"]

        # Определяем исходный формат файлов в UNIT
        unit_files = list(unit_path.glob("*"))
        if not unit_files:
            raise OperationError(
                f"No files found in UNIT {unit_id}",
                operation="convert_headless",
            )

        # Инициализируем конвертер
        doc_converter = RobustDocumentConverter()

        converted_files = []
        failed_files = []
        total_converted = 0

        # Обрабатываем каждый файл
        for file_path in unit_files:
            if file_path.is_file():
                file_ext = file_path.suffix.lower()

                # Проверяем, нужно ли конвертировать
                if file_ext in ['.doc', '.xls', '.ppt', '.rtf']:
                    logger.info(f"📄 Converting {file_path.name} to PDF")

                    if not dry_run:
                        try:
                            # Конвертируем в PDF
                            output_pdf = doc_converter.convert_document(
                                file_path,
                                output_dir=file_path.parent
                            )

                            if output_pdf:
                                logger.info(f"✅ Converted {file_path.name} -> {output_pdf.name}")

                                # Удаляем оригинальный файл
                                try:
                                    file_path.unlink()
                                    logger.debug(f"🗑️ Removed original file: {file_path.name}")
                                except Exception as e:
                                    logger.warning(f"Failed to remove {file_path.name}: {e}")

                                converted_files.append({
                                    "original": str(file_path),
                                    "converted": str(output_pdf),
                                    "format": f"{file_ext[1:]}->pdf"
                                })

                                total_converted += 1
                            else:
                                logger.error(f"❌ Failed to convert {file_path.name}")
                                failed_files.append(str(file_path))

                        except Exception as e:
                            logger.error(f"❌ Conversion error for {file_path.name}: {e}")
                            failed_files.append(str(file_path))
                    else:
                        logger.info(f"[DRY RUN] Would convert {file_path.name} to PDF")
                        converted_files.append({
                            "original": str(file_path),
                            "converted": str(file_path.parent / f"{file_path.stem}.pdf"),
                            "format": f"{file_ext[1:]}->pdf"
                        })
                        total_converted += 1

        # Обновляем manifest
        try:
            manifest = load_manifest(unit_path)
            update_manifest_operation(
                manifest,
                "convert_headless",
                {
                    "converted_files": converted_files,
                    "failed_files": failed_files,
                    "total_converted": total_converted,
                    "total_failed": len(failed_files)
                }
            )
            save_manifest(unit_path, manifest)
        except Exception as e:
            logger.warning(f"Failed to update manifest: {e}")

        # Логируем в audit
        audit_logger.log_operation(
            operation="convert_headless",
            unit_id=unit_id,
            cycle=cycle,
            success=total_converted > 0,
            operation_details={
                "converted_count": total_converted,
                "failed_count": len(failed_files),
                "converted_files": converted_files,
                "failed_files": failed_files
            }
        )

        # Определяем результат
        success = len(failed_files) == 0 and total_converted > 0

        if success:
            logger.info(f"✅ UNIT {unit_id} converted successfully ({total_converted} files)")
        else:
            logger.warning(f"⚠️ UNIT {unit_id} conversion completed with issues ({len(failed_files)} failed)")

        return {
            "unit_id": unit_id,
            "success": success,
            "converted_files": converted_files,
            "failed_files": failed_files,
            "total_converted": total_converted,
            "total_failed": len(failed_files),
            "target_directory": str(target_base_dir),
        }


"""
Extractor - безопасная разархивация архивов (ZIP, RAR, 7Z).

Оптимизирован для многопоточной обработки:
- Параллельная детекция типов файлов
- Параллельная распаковка нескольких архивов
- Параллельная обработка вложенных архивов
"""
import os
import zipfile
import subprocess
import logging
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from ..core.constants import MAX_EXTRACT_DEPTH
from ..core.parallel import calculate_optimal_workers, parallel_map_threads, get_parallel_config
from ..core.manifest import load_manifest, save_manifest, update_manifest_operation, get_is_mixed
from ..core.audit import get_audit_logger
from ..core.exceptions import OperationError, QuarantineError
from ..core.state_machine import UnitState
from ..core.unit_processor import (
    move_unit_to_target,
    update_unit_state,
    determine_unit_extension,
)
from ..core.config import get_cycle_paths, MERGE_DIR, get_data_paths, EXCEPTION_SUBDIRS
from ..utils.file_ops import detect_file_type, sanitize_filename
from ..utils.paths import get_unit_files

logger = logging.getLogger(__name__)

# Проверка доступности библиотек для архивов
try:
    import rarfile

    RARFILE_AVAILABLE = True
except ImportError:
    RARFILE_AVAILABLE = False

try:
    import py7zr

    PY7ZR_AVAILABLE = True
except ImportError:
    PY7ZR_AVAILABLE = False


class Extractor:
    """
    Извлекатель архивов с защитой от zip bomb.

    Ограничения:
    - Максимальный размер распаковки
    - Максимальное количество файлов
    - Максимальная глубина вложенности

    ОПТИМИЗАЦИЯ: Лимиты конфигурируются через environment variables:
    - MAX_EXTRACT_SIZE_MB: максимальный размер архива в МБ (по умолчанию 500)
    - MAX_FILES_IN_ARCHIVE: максимальное количество файлов (по умолчанию 1000)
    """

    # Лимиты безопасности (конфигурируемые через environment variables)
    MAX_UNPACK_SIZE_MB = int(os.getenv("MAX_EXTRACT_SIZE_MB", "500"))
    MAX_FILES_IN_ARCHIVE = int(os.getenv("MAX_FILES_IN_ARCHIVE", "1000"))
    MAX_DEPTH = 10

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

    def __init__(self):
        """Инициализирует Extractor."""
        self.audit_logger = get_audit_logger()

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

    def _record_extract_event(
        self,
        unit_id: str,
        registration_number: str,
        archives_processed: int,
        files_extracted: int,
        duration_ms: int,
        status: str = "success",
        error: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Записывает UnitEvent для извлечения в PipelineTracker.

        Args:
            unit_id: Идентификатор UNIT
            registration_number: Регистрационный номер (primary trace ID)
            archives_processed: Количество обработанных архивов
            files_extracted: Количество извлеченных файлов
            duration_ms: Длительность операции в миллисекундах
            status: Статус операции ("success", "failed" или "skipped")
            error: Описание ошибки (если есть)
            metrics: Дополнительные метрики
        """
        if not Extractor._tracker_run_id or not Extractor._db_client:
            return

        try:
            from docreciv.pipeline.events import EventType, EventStatus, Stage

            event_metrics = {
                "archives_processed": archives_processed,
                "files_extracted": files_extracted,
            }
            if metrics:
                event_metrics.update(metrics)

            Extractor._db_client.record_unit_event(
                unit_id=unit_id,
                run_id=Extractor._tracker_run_id,
                registration_number=registration_number,
                event_type=EventType.PROCESSED,  # PROCESSED вместо EXTRACTED (нет такого типа в EventType)
                stage=Stage.DOCPREP,
                status=EventStatus.COMPLETED if status == "success" else (
                    EventStatus.FAILED if status == "failed" else EventStatus.SKIPPED
                ),
                metrics=event_metrics,
                error=error,
                duration_ms=duration_ms
            )
        except ImportError as e:
            logger.debug(f"PipelineTracker events not available: {e}")
        except Exception as e:
            logger.warning(f"Failed to record extract event for {unit_id}: {e}")

    def extract_unit(
        self,
        unit_path: Path,
        cycle: int,
        max_depth: int = MAX_EXTRACT_DEPTH,
        keep_archive: bool = False,
        flatten: bool = False,
        protocol_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Извлекает все архивы в UNIT, перемещает UNIT в целевую директорию и обновляет state.

        Args:
            unit_path: Путь к директории UNIT
            cycle: Номер цикла (1, 2, 3)
            max_depth: Максимальная глубина рекурсивной распаковки
            keep_archive: Сохранять ли исходный архив
            flatten: Размещать все файлы в одной директории
            protocol_date: Дата протокола для организации по датам (опционально)
            dry_run: Если True, только показывает что будет сделано

        Returns:
            Словарь с результатами извлечения:
            - unit_id: идентификатор UNIT
            - archives_processed: количество обработанных архивов
            - files_extracted: количество извлеченных файлов
            - extracted_files: список извлеченных файлов
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

        # Находим архивы (параллельная детекция)
        archive_files = []
        all_files = [
            f for f in unit_path.rglob("*") if f.is_file() and f.name not in ["manifest.json", "audit.log.jsonl"]
        ]

        # Параллельная детекция типов файлов
        archive_extensions = {".zip", ".rar", ".7z"}

        config = get_parallel_config()
        if config.enabled and len(all_files) > 3:
            # Используем параллельную детекцию для > 3 файлов
            detections = parallel_map_threads(
                detect_file_type,
                all_files,
                operation_type="extractor",
                desc="Archive detection",
            )

            for file_path, detection in zip(all_files, detections):
                extension = file_path.suffix.lower()
                if (detection.get("is_archive") or
                    detection.get("detected_type") in ["zip_archive", "rar_archive", "7z_archive"] or
                    extension in archive_extensions):
                    archive_files.append(file_path)
        else:
            # Последовательная обработка для небольшого количества файлов
            for file_path in all_files:
                detection = detect_file_type(file_path)
                extension = file_path.suffix.lower()

                if (detection.get("is_archive") or
                    detection.get("detected_type") in ["zip_archive", "rar_archive", "7z_archive"] or
                    extension in archive_extensions):
                    archive_files.append(file_path)


        # Проверяем, есть ли уже извлеченные файлы (директории *_extracted)
        has_extracted_dirs = any(
            f.is_dir() and "_extracted" in f.name 
            for f in unit_path.iterdir() 
            if f.name not in ["manifest.json", "audit.log.jsonl"]
        )
        
        # Если нет архивов для извлечения и нет извлеченных директорий, UNIT не должен перемещаться
        if not archive_files and not has_extracted_dirs:
            logger.warning(f"No archives to extract in unit {unit_id} - moving to Exceptions")
            
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
            if cycle == 1:
                target_base_dir = exceptions_base / "Direct" / "NoProcessableFiles"
            else:
                target_base_dir = exceptions_base / f"Processing_{cycle}" / "NoProcessableFiles"
            
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
                new_state = exception_state_map.get(cycle, UnitState.EXCEPTION_1)

                update_unit_state(
                    unit_path=target_dir,
                    new_state=new_state,
                    cycle=cycle,
                    operation={
                        "type": "extract",
                        "status": "skipped",
                        "reason": "no_processable_files",
                    },
                )

            # UnitEvents: записываем событие для случая без архивов
            duration_ms = int((time.time() - start_time) * 1000)
            self._record_extract_event(
                unit_id=unit_id,
                registration_number=registration_number,
                archives_processed=0,
                files_extracted=0,
                duration_ms=duration_ms,
                status="skipped",
                error="No archives found that require extraction"
            )

            return {
                "unit_id": unit_id,
                "archives_processed": 0,
                "files_extracted": 0,
                "extracted_files": [],
                "errors": [{"error": "No archives found that require extraction"}],
                "moved_to": str(target_dir),
            }

        extracted_files = []
        errors = []

        # Параллельная распаковка архивов
        config = get_parallel_config()
        use_parallel = config.enabled and len(archive_files) > 1

        if use_parallel:
            # Параллельная распаковка нескольких архивов
            max_workers = min(
                calculate_optimal_workers("extractor"),
                len(archive_files)
            )
            logger.info(f"Extracting {len(archive_files)} archives in parallel with {max_workers} workers")

            # Результаты распаковки: (archive_path, result/exception)
            extraction_results: List[tuple] = []

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        self._extract_archive,
                        archive_path, unit_path, max_depth, keep_archive, flatten
                    ): archive_path
                    for archive_path in archive_files
                }

                for future in as_completed(futures):
                    archive_path = futures[future]
                    try:
                        result = future.result()
                        extraction_results.append((archive_path, "success", result))
                    except QuarantineError as e:
                        extraction_results.append((archive_path, "quarantined", e))
                    except Exception as e:
                        extraction_results.append((archive_path, "failed", e))

            # Обработка результатов (последовательно для thread-safety manifest)
            for archive_path, status, result_or_error in extraction_results:
                if status == "success":
                    extracted_files.extend(result_or_error.get("files", []))
                    if manifest:
                        operation = {
                            "type": "extract",
                            "status": "success",
                            "archive_path": str(archive_path),
                            "extracted_count": len(result_or_error.get("files", [])),
                            "cycle": current_cycle,
                        }
                        manifest = update_manifest_operation(manifest, operation)
                elif status == "quarantined":
                    e = result_or_error
                    errors.append(
                        {"file": str(archive_path), "error": str(e), "quarantined": True}
                    )
                    logger.error(f"Quarantined archive {archive_path}: {e}")
                    if manifest:
                        operation = {
                            "type": "extract",
                            "status": "quarantined",
                            "archive_path": str(archive_path.name),
                            "error": str(e),
                            "files_extracted": 0,
                            "cycle": current_cycle,
                        }
                        manifest = update_manifest_operation(manifest, operation)
                else:  # failed
                    e = result_or_error
                    errors.append({"file": str(archive_path), "error": str(e)})
                    logger.error(f"Failed to extract {archive_path}: {e}")
                    if manifest:
                        operation = {
                            "type": "extract",
                            "status": "failed",
                            "archive_path": str(archive_path.name),
                            "error": str(e),
                            "files_extracted": 0,
                            "cycle": current_cycle,
                        }
                        manifest = update_manifest_operation(manifest, operation)
        else:
            # Последовательная распаковка (один архив или отключена параллелизация)
            for archive_path in archive_files:
                try:
                    result = self._extract_archive(
                        archive_path, unit_path, max_depth, keep_archive, flatten
                    )
                    extracted_files.extend(result.get("files", []))

                    # Обновляем manifest
                    if manifest:
                        operation = {
                            "type": "extract",
                            "status": "success",
                            "archive_path": str(archive_path),
                            "extracted_count": len(result.get("files", [])),
                            "cycle": current_cycle,
                        }
                        manifest = update_manifest_operation(manifest, operation)
                except QuarantineError as e:
                    errors.append(
                        {"file": str(archive_path), "error": str(e), "quarantined": True}
                    )
                    logger.error(f"Quarantined archive {archive_path}: {e}")
                    if manifest:
                        operation = {
                            "type": "extract",
                            "status": "quarantined",
                            "archive_path": str(archive_path.name),
                            "error": str(e),
                            "files_extracted": 0,
                            "cycle": current_cycle,
                        }
                        manifest = update_manifest_operation(manifest, operation)
                except Exception as e:
                    errors.append({"file": str(archive_path), "error": str(e)})
                    logger.error(f"Failed to extract {archive_path}: {e}")
                    if manifest:
                        operation = {
                            "type": "extract",
                            "status": "failed",
                            "archive_path": str(archive_path.name),
                            "error": str(e),
                            "files_extracted": 0,
                            "cycle": current_cycle,
                        }
                        manifest = update_manifest_operation(manifest, operation)

        # Проверяем, есть ли уже извлеченные файлы (директории *_extracted)
        has_extracted_dirs = any(
            f.is_dir() and "_extracted" in f.name 
            for f in unit_path.iterdir() 
            if f.name not in ["manifest.json", "audit.log.jsonl"]
        )
        
        # Если не было успешных извлечений и нет уже извлеченных директорий, перемещаем в Exceptions
        if not extracted_files and not has_extracted_dirs and not dry_run:
            logger.warning(f"No archives were successfully extracted in unit {unit_id} - moving to Exceptions")
            
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
                target_base_dir = exceptions_base / "Direct" / EXCEPTION_SUBDIRS["ErExtract"]
            else:
                target_base_dir = exceptions_base / f"Processing_{current_cycle}" / EXCEPTION_SUBDIRS["ErExtract"]
            
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
                    "type": "extract",
                    "status": "failed",
                    "errors": errors,
                },
            )

            # UnitEvents: записываем событие для неудачного извлечения
            duration_ms = int((time.time() - start_time) * 1000)
            error_msg = "; ".join([e.get("error", "unknown") for e in errors]) if errors else "Extraction failed"
            self._record_extract_event(
                unit_id=unit_id,
                registration_number=registration_number,
                archives_processed=len(archive_files),
                files_extracted=0,
                duration_ms=duration_ms,
                status="failed",
                error=error_msg
            )

            return {
                "unit_id": unit_id,
                "archives_processed": len(archive_files),
                "files_extracted": 0,
                "extracted_files": [],
                "errors": errors,
                "moved_to": str(target_dir),
            }

        # Сохраняем обновленный manifest
        if manifest:
            save_manifest(unit_path, manifest)

        # Определяем расширение для сортировки из извлеченных файлов
        # ВАЖНО: Проверяем Mixed статус из manifest ПЕРВЫМ
        is_mixed = get_is_mixed(manifest) if manifest else False

        if is_mixed:
            # Mixed UNIT сортируются в Mixed/ директорию
            extension = "Mixed"
        else:
            # Для не-mixed UNIT определяем расширение из файлов
            extension = determine_unit_extension(unit_path)
            # Если расширение не определено, пытаемся определить из извлеченных файлов
            if not extension and extracted_files:
                for ef in extracted_files:
                    ext_path = Path(ef.get("extracted_path", ""))
                    if ext_path.suffix:
                        extension = ext_path.suffix.lower().lstrip('.')
                        break

        # Определяем следующий цикл (после извлечения переходим к следующему циклу)
        next_cycle = min(current_cycle + 1, 3)

        # Перемещаем НАПРЯМУЮ в Merge_N/Extracted/ (без Processing_N+1/Direct/)
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
        target_base_dir = cycle_paths["merge"] / "Extracted"

        # Определяем новое состояние ПЕРЕД перемещением
        # Проверяем текущее состояние из manifest
        from ..core.state_machine import UnitStateMachine
        state_machine = UnitStateMachine(unit_id, manifest_path)
        current_state = state_machine.get_current_state()
        
        # Определяем целевое состояние
        if current_state == UnitState.CLASSIFIED_1:
            # Из CLASSIFIED_1 переходим в PENDING_EXTRACT, затем в CLASSIFIED_2
            # Сначала переводим в PENDING_EXTRACT (если не dry_run)
            if not dry_run:
                update_unit_state(
                    unit_path=unit_path,
                    new_state=UnitState.PENDING_EXTRACT,
                    cycle=current_cycle,
                    operation={
                        "type": "extract",
                        "status": "pending",
                    },
                )
            # Целевое состояние после извлечения
            new_state = UnitState.CLASSIFIED_2
        elif current_state == UnitState.PENDING_EXTRACT:
            # Уже в PENDING_EXTRACT, переводим в CLASSIFIED_2
            new_state = UnitState.CLASSIFIED_2
        elif current_state == UnitState.CLASSIFIED_2 and current_cycle == 2:
            # Для UNITов в CLASSIFIED_2 с циклом 2 переходим в CLASSIFIED_3
            new_state = UnitState.CLASSIFIED_3
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
            
            # Переходим в целевое состояние
            update_unit_state(
                unit_path=target_dir,
                new_state=new_state,
                cycle=next_cycle,
                operation={
                    "type": "extract",
                    "archives_processed": len(archive_files),
                    "files_extracted": len(extracted_files),
                },
            )

        # Логируем операцию
        self.audit_logger.log_event(
            unit_id=unit_id,
            event_type="operation",
            operation="extract",
            details={
                "cycle": current_cycle,
                "archives_processed": len(archive_files),
                "files_extracted": len(extracted_files),
                "extension": extension,
                "target_directory": str(target_dir),
                "errors": errors,
            },
            state_before=manifest.get("state_machine", {}).get("current_state") if manifest else None,
            state_after=new_state.value,
            unit_path=target_dir,
        )

        # UnitEvents: записываем событие успешного извлечения
        duration_ms = int((time.time() - start_time) * 1000)
        self._record_extract_event(
            unit_id=unit_id,
            registration_number=registration_number,
            archives_processed=len(archive_files),
            files_extracted=len(extracted_files),
            duration_ms=duration_ms,
            status="success",
            metrics={"errors_count": len(errors), "extension": extension}
        )

        return {
            "unit_id": unit_id,
            "archives_processed": len(archive_files),
            "files_extracted": len(extracted_files),
            "extracted_files": extracted_files,
            "errors": errors,
            "moved_to": str(target_dir),
            "next_cycle": next_cycle,
            "extension": extension,
        }

    def _extract_archive(
        self,
        archive_path: Path,
        extract_to: Path,
        max_depth: int,
        keep_archive: bool,
        flatten: bool,
    ) -> Dict[str, Any]:
        """
        Извлекает один архив с рекурсивной распаковкой вложенных архивов.

        Args:
            archive_path: Путь к архиву
            extract_to: Директория для извлечения
            max_depth: Максимальная глубина рекурсии
            keep_archive: Сохранять ли архив
            flatten: Размещать все в одной директории

        Returns:
            Словарь с результатами извлечения

        Raises:
            QuarantineError: Если архив опасен (zip bomb)
            OperationError: Если извлечение не удалось
        """
        # Используем set для отслеживания обработанных архивов (защита от циклов)
        processed_hashes: Set[str] = set()

        # Вызываем рекурсивную функцию с глубиной 0
        extracted_files = self._extract_archive_recursive(
            archive_path=archive_path,
            extract_to=extract_to,
            current_depth=0,
            max_depth=max_depth,
            keep_archive=keep_archive,
            flatten=flatten,
            processed_hashes=processed_hashes,
        )

        return {"files": extracted_files, "extract_dir": str(extract_to / f"{archive_path.stem}_extracted")}

    def _extract_archive_recursive(
        self,
        archive_path: Path,
        extract_to: Path,
        current_depth: int,
        max_depth: int,
        keep_archive: bool,
        flatten: bool,
        processed_hashes: Set[str],
    ) -> List[Dict[str, Any]]:
        """
        Рекурсивно извлекает архив и вложенные архивы.

        Args:
            archive_path: Путь к архиву
            extract_to: Директория для извлечения
            current_depth: Текущая глубина рекурсии
            max_depth: Максимальная глубина рекурсии
            keep_archive: Сохранять ли архив
            flatten: Размещать все в одной директории
            processed_hashes: Set хэшей обработанных архивов (защита от циклов)

        Returns:
            Список извлеченных файлов (включая из вложенных архивов)

        Raises:
            QuarantineError: Если архив опасен (zip bomb)
            OperationError: Если извлечение не удалось
        """
        # Проверяем глубину
        if current_depth > max_depth:
            logger.warning(f"Max depth {max_depth} reached, stopping recursion for {archive_path}")
            return []

        # Вычисляем SHA256 хэш архива для обнаружения дубликатов
        try:
            archive_hash = self._calculate_file_hash(archive_path)
            if archive_hash in processed_hashes:
                logger.warning(f"Circular dependency detected: {archive_path} already processed")
                return []
            processed_hashes.add(archive_hash)
        except Exception as e:
            logger.warning(f"Failed to calculate hash for {archive_path}: {e}")
            # Продолжаем без проверки на дубликаты

        detection = detect_file_type(archive_path)
        archive_type = detection.get("detected_type")

        # Создаем директорию для извлечения
        extract_dir = extract_to / f"{archive_path.stem}_extracted"
        extract_dir.mkdir(parents=True, exist_ok=True)

        max_size = self.MAX_UNPACK_SIZE_MB * 1024 * 1024
        extracted_files = []

        try:
            # Извлекаем текущий архив
            if archive_type == "zip_archive" or archive_path.suffix.lower() == ".zip":
                extracted_files = self._extract_zip(
                    archive_path, extract_dir, max_size, max_depth, flatten
                )
            elif archive_type == "rar_archive" or archive_path.suffix.lower() == ".rar":
                if not RARFILE_AVAILABLE:
                    raise OperationError(
                        "RAR extraction requires rarfile library. Install: pip install rarfile",
                        operation="extract",
                        operation_details={"missing": "python_lib", "lib": "rarfile"}
                    )
                # Проверяем системную утилиту unrar
                from ..utils.dependencies import DependencyChecker
                unrar_cmd = DependencyChecker.check_system_tool("unrar")
                if not unrar_cmd:
                    raise OperationError(
                        "RAR extraction requires unrar system tool. Install: sudo apt-get install unrar",
                        operation="extract",
                        operation_details={"missing": "system_tool", "tool": "unrar"}
                    )
                extracted_files = self._extract_rar(
                    archive_path, extract_dir, max_size, max_depth, flatten
                )
            elif archive_path.suffix.lower() == ".7z":
                if not PY7ZR_AVAILABLE:
                    raise OperationError(
                        "7z extraction requires py7zr library. Install: pip install py7zr",
                        operation="extract",
                        operation_details={"missing": "python_lib", "lib": "py7zr"}
                    )
                # py7zr не требует системной утилиты 7z (чистая Python реализация)
                extracted_files = self._extract_7z(
                    archive_path, extract_dir, max_size, max_depth, flatten
                )
            else:
                raise OperationError(
                    f"Unsupported archive type: {archive_type}",
                    operation="extract",
                )

            # РЕКУРСИВНАЯ ЛОГИКА: Сканируем извлеченные файлы на наличие вложенных архивов
            if current_depth < max_depth:
                nested_archives = []
                archive_extensions = {".zip", ".rar", ".7z"}

                # Фильтруем только существующие файлы для проверки
                candidate_files = [
                    Path(file_info["extracted_path"])
                    for file_info in extracted_files
                    if Path(file_info["extracted_path"]).exists()
                    and Path(file_info["extracted_path"]).is_file()
                ]

                # Параллельная детекция вложенных архивов
                config = get_parallel_config()
                if config.enabled and len(candidate_files) > 3:
                    detections = parallel_map_threads(
                        detect_file_type,
                        candidate_files,
                        operation_type="extractor",
                        desc="Nested archive detection",
                    )
                    for file_path, file_detection in zip(candidate_files, detections):
                        file_extension = file_path.suffix.lower()
                        if (file_detection.get("is_archive") or
                            file_detection.get("detected_type") in ["zip_archive", "rar_archive", "7z_archive"] or
                            file_extension in archive_extensions):
                            nested_archives.append(file_path)
                else:
                    # Последовательная детекция
                    for file_path in candidate_files:
                        file_detection = detect_file_type(file_path)
                        file_extension = file_path.suffix.lower()
                        if (file_detection.get("is_archive") or
                            file_detection.get("detected_type") in ["zip_archive", "rar_archive", "7z_archive"] or
                            file_extension in archive_extensions):
                            nested_archives.append(file_path)

                # Рекурсивно извлекаем вложенные архивы
                if nested_archives:
                    logger.info(f"Found {len(nested_archives)} nested archives at depth {current_depth}")

                    # Параллельная распаковка вложенных архивов (если > 1)
                    if config.enabled and len(nested_archives) > 1:
                        max_workers = min(
                            calculate_optimal_workers("extractor"),
                            len(nested_archives)
                        )

                        # Создаем thread-safe lock для processed_hashes
                        hash_lock = threading.Lock()

                        def extract_nested_safe(nested_archive: Path) -> List[Dict[str, Any]]:
                            """Thread-safe извлечение вложенного архива."""
                            try:
                                return self._extract_archive_recursive(
                                    archive_path=nested_archive,
                                    extract_to=extract_dir,
                                    current_depth=current_depth + 1,
                                    max_depth=max_depth,
                                    keep_archive=keep_archive,
                                    flatten=flatten,
                                    processed_hashes=processed_hashes,
                                )
                            except Exception as e:
                                logger.error(f"Failed to extract nested archive {nested_archive}: {e}")
                                return []

                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            futures = {
                                executor.submit(extract_nested_safe, arch): arch
                                for arch in nested_archives
                            }
                            for future in as_completed(futures):
                                nested_archive = futures[future]
                                try:
                                    nested_files = future.result()
                                    extracted_files.extend(nested_files)
                                    if nested_files:
                                        logger.info(
                                            f"Extracted {len(nested_files)} files from nested archive {nested_archive.name}"
                                        )
                                except Exception as e:
                                    logger.error(f"Failed to extract nested archive {nested_archive}: {e}")
                    else:
                        # Последовательная распаковка
                        for nested_archive in nested_archives:
                            try:
                                nested_files = self._extract_archive_recursive(
                                    archive_path=nested_archive,
                                    extract_to=extract_dir,
                                    current_depth=current_depth + 1,
                                    max_depth=max_depth,
                                    keep_archive=keep_archive,
                                    flatten=flatten,
                                    processed_hashes=processed_hashes,
                                )
                                extracted_files.extend(nested_files)
                                logger.info(
                                    f"Extracted {len(nested_files)} files from nested archive {nested_archive.name}"
                                )
                            except Exception as e:
                                logger.error(f"Failed to extract nested archive {nested_archive}: {e}")

            # Удаляем исходный архив если не нужно сохранять
            if not keep_archive:
                archive_path.unlink()

            return extracted_files

        except QuarantineError:
            raise
        except Exception as e:
            raise OperationError(
                f"Extraction error: {str(e)}",
                operation="extract",
                operation_details={"exception": type(e).__name__},
            )

    def _calculate_file_hash(self, file_path: Path) -> str:
        """
        Вычисляет SHA256 хэш файла.

        Args:
            file_path: Путь к файлу

        Returns:
            Hex-строка SHA256 хэша
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Читаем файл блоками для экономии памяти
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _extract_zip(
        self, archive_path: Path, extract_dir: Path, max_size: int, max_depth: int, flatten: bool
    ) -> List[Dict[str, Any]]:
        """Извлекает ZIP архив с валидацией."""

        # ВАЛИДАЦИЯ 1: Проверка что файл действительно является ZIP
        if not zipfile.is_zipfile(archive_path):
            raise OperationError(
                f"File {archive_path.name} has .zip extension but is not a valid ZIP archive",
                operation="extract",
                operation_details={"file": str(archive_path), "invalid_zip": True}
            )

        extracted_files = []
        total_size = 0
        file_count = 0

        # ВАЛИДАЦИЯ 2: Проверка целостности архива через testzip()
        try:
            with zipfile.ZipFile(archive_path, "r") as test_ref:
                bad_file = test_ref.testzip()
                if bad_file is not None:
                    raise OperationError(
                        f"ZIP archive is corrupted: bad file '{bad_file}'",
                        operation="extract",
                        operation_details={"file": str(archive_path), "corrupted": True, "bad_file": bad_file}
                    )
        except Exception as e:
            if isinstance(e, OperationError):
                raise
            raise OperationError(
                f"ZIP archive validation failed: {str(e)}",
                operation="extract",
                operation_details={"file": str(archive_path), "validation_error": str(e)}
            )

        with zipfile.ZipFile(archive_path, "r") as zip_ref:
            for member in zip_ref.namelist():
                # Проверка на zip bomb
                file_info = zip_ref.getinfo(member)
                total_size += file_info.file_size
                file_count += 1

                if total_size > max_size:
                    raise QuarantineError(
                        f"Archive exceeds size limit: {total_size} > {max_size}",
                        reason="zip_bomb_size",
                    )

                if file_count > self.MAX_FILES_IN_ARCHIVE:
                    raise QuarantineError(
                        f"Archive exceeds file count limit: {file_count} > {self.MAX_FILES_IN_ARCHIVE}",
                        reason="zip_bomb_count",
                    )

                # Санитизируем имя файла
                safe_name = sanitize_filename(member)

                # ВАЛИДАЦИЯ 3: Truncate для слишком длинных имён (максимум 255 БАЙТ в UTF-8)
                # ВАЖНО: Используем байтовую длину, а не количество символов!
                # Кириллица в UTF-8 занимает 2+ байта на символ, так что len() недостаточно
                max_filename_bytes = 255
                safe_name_bytes = len(safe_name.encode('utf-8'))

                if safe_name_bytes > max_filename_bytes:
                    # Сохраняем расширение, обрезаем имя по байтам
                    name_parts = safe_name.rsplit('.', 1)  # rsplit чтобы отделить только последнее расширение
                    if len(name_parts) > 1:
                        ext = '.' + name_parts[-1]
                        base = name_parts[0]
                        # Вычисляем доступное место для base в байтах
                        ext_bytes = len(ext.encode('utf-8'))
                        max_base_bytes = max_filename_bytes - ext_bytes

                        # Постепенно укорачиваем base пока не влезем в лимит байтов
                        while len(base.encode('utf-8')) > max_base_bytes and len(base) > 10:
                            base = base[:-1]  # Убираем по одному символу с конца

                        safe_name = base + ext
                    else:
                        # Без расширения - просто обрезаем до нужного количества байт
                        while len(safe_name.encode('utf-8')) > max_filename_bytes and len(safe_name) > 10:
                            safe_name = safe_name[:-1]

                    logger.debug(f"Truncated long filename: {len(member.encode('utf-8'))} bytes -> {len(safe_name.encode('utf-8'))} bytes")

                if flatten:
                    # Размещаем все в одной директории
                    target_path = extract_dir / Path(safe_name).name
                else:
                    # Сохраняем структуру
                    target_path = extract_dir / safe_name

                # Извлекаем файл
                # ВАЖНО: Всегда извлекаем в target_path (с безопасным именем),
                # читая содержимое напрямую из архива чтобы избежать проблем с длинными именами
                try:
                    # Читаем содержимое напрямую из архива и пишем в файл с безопасным именем
                    with zip_ref.open(member) as source:
                        target_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(target_path, 'wb') as dest:
                            # Читаем и пишем блоками для больших файлов
                            while True:
                                chunk = source.read(8192)
                                if not chunk:
                                    break
                                dest.write(chunk)

                    if len(member) > 255:
                        logger.debug(f"Extracted long filename: {len(member)} chars -> {len(safe_name)} chars")

                    if target_path.exists() and target_path.is_file():
                        # ИСПРАВЛЕНО: Проверка на пустые файлы (0 байт)
                        if target_path.stat().st_size == 0:
                            logger.warning(f"Empty file extracted, skipping: {target_path.name}")
                            target_path.unlink()  # Удаляем пустой файл
                            continue  # Пропускаем добавление в extracted_files

                        # Валидация извлеченного файла
                        try:
                            validation_result = detect_file_type(target_path)
                            validated_type = validation_result.get("detected_type", "unknown")
                            validation_passed = validated_type != "corrupted"
                        except Exception as e:
                            logger.warning(f"Validation failed for {target_path}: {e}")
                            validated_type = "unknown"
                            validation_passed = False

                        extracted_files.append(
                            {
                                "original_name": member,
                                "extracted_path": str(target_path),
                                "size": file_info.file_size,
                                "validated_type": validated_type,
                                "validation_passed": validation_passed,
                            }
                        )
                except Exception as extract_err:
                    # Логируем ошибку извлечения отдельного файла, но продолжаем
                    logger.warning(f"Failed to extract {member[:50]}... from ZIP: {extract_err}")

        return extracted_files

    def _extract_rar(
        self, archive_path: Path, extract_dir: Path, max_size: int, max_depth: int, flatten: bool
    ) -> List[Dict[str, Any]]:
        """Извлекает RAR архив с валидацией."""

        # ВАЛИДАЦИЯ 1: Проверка что файл действительно является RAR
        if RARFILE_AVAILABLE:
            if not rarfile.is_rarfile(archive_path):
                raise OperationError(
                    f"File {archive_path.name} has .rar extension but is not a valid RAR archive",
                    operation="extract",
                    operation_details={"file": str(archive_path), "invalid_rar": True}
                )
        else:
            # Fallback: проверка по magic bytes если библиотека недоступна
            with open(archive_path, "rb") as f:
                header = f.read(7)
                if not header.startswith(b"Rar!\x1a\x07"):
                    raise OperationError(
                        f"File {archive_path.name} has .rar extension but missing RAR magic bytes",
                        operation="extract",
                        operation_details={"file": str(archive_path), "invalid_rar": True, "header": header.hex()}
                    )

        extracted_files = []
        total_size = 0
        file_count = 0

        with rarfile.RarFile(archive_path, "r") as rar_ref:
            for member in rar_ref.infolist():
                # Проверка на zip bomb
                total_size += member.file_size
                file_count += 1

                if total_size > max_size:
                    raise QuarantineError(
                        f"Archive exceeds size limit: {total_size} > {max_size}",
                        reason="zip_bomb_size",
                    )

                if file_count > self.MAX_FILES_IN_ARCHIVE:
                    raise QuarantineError(
                        f"Archive exceeds file count limit: {file_count} > {self.MAX_FILES_IN_ARCHIVE}",
                        reason="zip_bomb_count",
                    )

                # Санитизируем имя файла
                safe_name = sanitize_filename(member.filename)

                # ВАЛИДАЦИЯ 2: Truncate для слишком длинных имён (максимум 255 символов)
                max_filename_length = 255
                if len(safe_name) > max_filename_length:
                    name_parts = safe_name.split('.')
                    if len(name_parts) > 1:
                        ext = '.' + name_parts[-1]
                        base = '.'.join(name_parts[:-1])
                        max_base_length = max_filename_length - len(ext)
                        safe_name = base[:max_base_length] + ext
                    else:
                        safe_name = safe_name[:max_filename_length]
                    logger.debug(f"Truncated long RAR filename: {member.filename} -> {safe_name}")

                if flatten:
                    # Размещаем все в одной директории
                    target_path = extract_dir / Path(safe_name).name
                else:
                    # Сохраняем структуру
                    target_path = extract_dir / safe_name

                # Извлекаем файл
                rar_ref.extract(member, extract_dir)
                if target_path.exists() and target_path.is_file():
                    # ИСПРАВЛЕНО: Проверка на пустые файлы (0 байт)
                    if target_path.stat().st_size == 0:
                        logger.warning(f"Empty file extracted from RAR, skipping: {target_path.name}")
                        target_path.unlink()  # Удаляем пустой файл
                        continue  # Пропускаем добавление в extracted_files

                    # Валидация извлеченного файла
                    try:
                        validation_result = detect_file_type(target_path)
                        validated_type = validation_result.get("detected_type", "unknown")
                        validation_passed = validated_type != "corrupted"
                    except Exception as e:
                        logger.warning(f"Validation failed for {target_path}: {e}")
                        validated_type = "unknown"
                        validation_passed = False

                    extracted_files.append(
                        {
                            "original_name": member.filename,
                            "extracted_path": str(target_path),
                            "size": member.file_size,
                            "validated_type": validated_type,
                            "validation_passed": validation_passed,
                        }
                    )

        return extracted_files

    def _extract_7z(
        self, archive_path: Path, extract_dir: Path, max_size: int, max_depth: int, flatten: bool
    ) -> List[Dict[str, Any]]:
        """Извлекает 7z архив с валидацией."""

        # ВАЛИДАЦИЯ 1: Проверка magic bytes для 7z
        # 7z magic bytes: 37 7A BC AF 27 1C
        with open(archive_path, "rb") as f:
            header = f.read(6)
            if header != b'7z\xbc\xaf\x27\x1c':
                raise OperationError(
                    f"File {archive_path.name} has .7z extension but is not a valid 7z archive",
                    operation="extract",
                    operation_details={"file": str(archive_path), "invalid_7z": True, "header": header.hex()}
                )

        extracted_files = []
        total_size = 0
        file_count = 0

        with py7zr.SevenZipFile(archive_path, "r") as sz_ref:
            # Получаем информацию о файлах
            file_list = sz_ref.list()

            for file_info in file_list:
                # Проверка на zip bomb
                # Для 7z используем uncompressed_size если доступен
                file_size = getattr(file_info, 'uncompressed', 0) or getattr(file_info, 'size', 0)
                total_size += file_size
                file_count += 1

                if total_size > max_size:
                    raise QuarantineError(
                        f"Archive exceeds size limit: {total_size} > {max_size}",
                        reason="zip_bomb_size",
                    )

                if file_count > self.MAX_FILES_IN_ARCHIVE:
                    raise QuarantineError(
                        f"Archive exceeds file count limit: {file_count} > {self.MAX_FILES_IN_ARCHIVE}",
                        reason="zip_bomb_count",
                    )

            # Извлекаем все файлы
            sz_ref.extractall(path=str(extract_dir))

            # Получаем список извлеченных файлов
            for file_info in file_list:
                filename = file_info.filename

                # ВАЛИДАЦИЯ 2: Truncate для слишком длинных имён (максимум 255 символов)
                safe_name = sanitize_filename(filename)
                max_filename_length = 255
                if len(safe_name) > max_filename_length:
                    name_parts = safe_name.split('.')
                    if len(name_parts) > 1:
                        ext = '.' + name_parts[-1]
                        base = '.'.join(name_parts[:-1])
                        max_base_length = max_filename_length - len(ext)
                        safe_name = base[:max_base_length] + ext
                    else:
                        safe_name = safe_name[:max_filename_length]
                    # Переименовываем если нужно
                    original_path = extract_dir / filename
                    truncated_path = extract_dir / safe_name
                    if original_path.exists() and original_path != truncated_path:
                        original_path.rename(truncated_path)
                        file_path = truncated_path
                        logger.debug(f"Truncated long 7z filename: {filename} -> {safe_name}")
                    else:
                        file_path = extract_dir / filename
                else:
                    file_path = extract_dir / filename

                if file_path.exists() and file_path.is_file():
                    # ИСПРАВЛЕНО: Проверка на пустые файлы (0 байт)
                    file_size = file_path.stat().st_size
                    if file_size == 0:
                        logger.warning(f"Empty file extracted from 7z, skipping: {file_path.name}")
                        file_path.unlink()  # Удаляем пустой файл
                        continue  # Пропускаем добавление в extracted_files

                    # Валидация извлеченного файла
                    try:
                        validation_result = detect_file_type(file_path)
                        validated_type = validation_result.get("detected_type", "unknown")
                        validation_passed = validated_type != "corrupted"
                    except Exception as e:
                        logger.warning(f"Validation failed for {file_path}: {e}")
                        validated_type = "unknown"
                        validation_passed = False

                    extracted_files.append(
                        {
                            "original_name": filename,
                            "extracted_path": str(file_path),
                            "size": file_size,
                            "validated_type": validated_type,
                            "validation_passed": validation_passed,
                        }
                    )

        return extracted_files


"""
ExtensionNormalizer - нормализация расширений файлов по сигнатурам.

Проверяет magic bytes и MIME, переименовывает без конвертации.
"""
import logging
from pathlib import Path
from typing import Dict, Any, Optional

from ...core.manifest import load_manifest, save_manifest, update_manifest_operation
from ...core.audit import get_audit_logger
from ...core.state_machine import UnitState
from ...core.unit_processor import (
    move_unit_to_target,
    update_unit_state,
    determine_unit_extension,
)
from ...core.config import get_cycle_paths, MERGE_DIR, EXCEPTION_SUBDIRS
from ...utils.file_ops import detect_file_type

logger = logging.getLogger(__name__)


class ExtensionNormalizer:
    """Нормализатор расширений файлов."""

    # Маппинг типов на расширения
    TYPE_TO_EXTENSION = {
        "pdf": ".pdf",
        "docx": ".docx",
        "doc": ".doc",
        "xlsx": ".xlsx",
        "xls": ".xls",
        "pptx": ".pptx",
        "ppt": ".ppt",
        "zip_archive": ".zip",
        "rar_archive": ".rar",
        "7z_archive": ".7z",
        "jpg": ".jpg",
        "jpeg": ".jpeg",
        "png": ".png",
        "gif": ".gif",
        "tiff": ".tiff",
        "html": ".html",
        "xml": ".xml",
        "txt": ".txt",
    }

    def __init__(self):
        """Инициализирует ExtensionNormalizer."""
        self.audit_logger = get_audit_logger()

    def normalize_extensions(
        self,
        unit_path: Path,
        cycle: int,
        protocol_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Нормализует расширения всех файлов в UNIT, перемещает UNIT и обновляет state.

        Args:
            unit_path: Путь к директории UNIT
            cycle: Номер цикла (1, 2, 3)
            protocol_date: Дата протокола для организации по датам (опционально)
            dry_run: Если True, только показывает что будет сделано

        Returns:
            Словарь с результатами нормализации:
            - unit_id: идентификатор UNIT
            - files_normalized: количество нормализованных файлов
            - normalized_files: список нормализованных файлов
            - errors: список ошибок
            - moved_to: путь к новой директории UNIT (после перемещения)
        """
        unit_id = unit_path.name
        correlation_id = self.audit_logger.get_correlation_id()

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

        # Находим все файлы
        files = [
            f
            for f in unit_path.rglob("*")
            if f.is_file() and f.name not in ["manifest.json", "audit.log.jsonl"]
        ]

        normalized_files = []
        errors = []
        detected_types = []

        for file_path in files:
            try:
                detection = detect_file_type(file_path)
                detected_type = detection.get("detected_type")
                detected_types.append(detected_type)

                # Используем результат Decision Engine
                classification = detection.get("classification")
                correct_extension = detection.get("correct_extension")

                # Если Decision Engine определил normalize, исправляем расширение
                if classification == "normalize" and correct_extension:
                    current_ext = file_path.suffix.lower()

                    if current_ext != correct_extension:
                        # Переименовываем файл
                        new_name = file_path.stem + correct_extension
                        new_path = file_path.parent / new_name
                        if not dry_run:
                            file_path.rename(new_path)

                        normalized_files.append(
                            {
                                "original_name": file_path.name,
                                "normalized_name": new_name,
                                "original_extension": current_ext,
                                "correct_extension": correct_extension,
                                "detected_type": detected_type,
                                "original_path": str(file_path),
                                "new_path": str(new_path),
                            }
                        )

                        # Обновляем manifest
                        if manifest:
                            operation = {
                                "type": "normalize",
                                "status": "success",
                                "subtype": "extension",
                                "original_extension": current_ext,
                                "correct_extension": correct_extension,
                                "detected_type": detected_type,
                                "cycle": current_cycle,
                            }
                            manifest = update_manifest_operation(manifest, operation)
            except Exception as e:
                errors.append({"file": str(file_path), "error": str(e)})
                logger.error(f"Failed to normalize extension for {file_path}: {e}")

        # Проверяем, были ли критические ошибки нормализации
        # Если есть ошибки и файлы требуют нормализации, но нормализация не удалась
        has_normalization_errors = False
        if errors:
            # Проверяем, были ли ошибки при попытке нормализации файлов, которые требуют нормализации
            # Если есть ошибки и нет успешно нормализованных файлов, это критическая ошибка
            files_requiring_normalization = []
            for file_path in files:
                try:
                    detection = detect_file_type(file_path)
                    classification = detection.get("classification")
                    if classification == "normalize":
                        files_requiring_normalization.append(file_path)
                except (OSError, IOError, PermissionError, KeyError) as e:
                    logger.debug(f"Could not detect file type for {file_path}: {e}")
            
            # Если есть файлы, требующие нормализации, но нормализация не удалась (есть ошибки и нет нормализованных файлов)
            if files_requiring_normalization and not normalized_files and errors:
                has_normalization_errors = True

        # Если были критические ошибки нормализации, перемещаем в Exceptions
        if has_normalization_errors and not dry_run:
            logger.warning(f"Normalization failed for unit {unit_id} - moving to Exceptions")

            # Определяем целевую директорию в Exceptions
            # ИСПРАВЛЕНО: Получаем base_dir от Classifier для правильных путей
            from ...core.config import get_data_paths, EXCEPTIONS_DIR
            from ...engine.classifier import Classifier
            effective_base_dir = Classifier._override_base_dir
            if protocol_date:
                data_paths = get_data_paths(protocol_date, base_dir=effective_base_dir)
                exceptions_base = data_paths["exceptions"]
            else:
                exceptions_base = EXCEPTIONS_DIR
            
            # НОВАЯ СТРУКТУРА v2: Exceptions/Direct для цикла 1, Exceptions/Processing_N для остальных
            if current_cycle == 1:
                target_base_dir = exceptions_base / "Direct" / EXCEPTION_SUBDIRS["ErNormalize"]
            else:
                target_base_dir = exceptions_base / f"Processing_{current_cycle}" / EXCEPTION_SUBDIRS["ErNormalize"]
            
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
                    "type": "normalize",
                    "status": "failed",
                    "errors": errors,
                },
            )
            
            return {
                "unit_id": unit_id,
                "files_normalized": 0,
                "normalized_files": [],
                "errors": errors,
                "moved_to": str(target_dir),
            }

        if not normalized_files:
            logger.info(f"No files needed normalization in unit {unit_id}")
            if manifest:
                save_manifest(unit_path, manifest)

        # Сохраняем обновленный manifest
        if manifest:
            save_manifest(unit_path, manifest)

        # Определяем расширение для сортировки (используем detected_type после нормализации)
        # Для Mixed units используем "Mixed" вместо расширения файла
        if manifest and manifest.get("is_mixed", False):
            extension = "Mixed"
        else:
            extension = None
            if detected_types:
                # Используем первый detected_type, убираем суффикс "_archive" если есть
                first_type = detected_types[0]
                if first_type:
                    extension = first_type.replace("_archive", "")
            if not extension:
                extension = determine_unit_extension(unit_path)

        # Перемещаем НАПРЯМУЮ в Merge_N/Normalized/ (без Processing_N+1/Direct/)
        # Правильный путь: Data/YYYY-MM-DD/Merge, а не Data/Merge/YYYY-MM-DD
        # ИСПРАВЛЕНО: Получаем base_dir от Classifier для правильных путей
        from ...engine.classifier import Classifier
        effective_base_dir = Classifier._override_base_dir

        if protocol_date and effective_base_dir is None:
            # Если указана дата но base_dir не установлен, используем структуру Data/date/Merge
            from ...core.config import DATA_BASE_DIR
            merge_base = DATA_BASE_DIR / protocol_date / "Merge"
        else:
            # Используем base_dir если установлен, иначе MERGE_DIR
            if effective_base_dir is not None:
                merge_base = effective_base_dir / "Merge"
            else:
                merge_base = MERGE_DIR

        cycle_paths = get_cycle_paths(current_cycle, None, merge_base, None)
        target_base_dir = cycle_paths["merge"] / "Normalized"

        # Определяем новое состояние ПЕРЕД перемещением
        # Проверяем текущее состояние из manifest
        from ...core.state_machine import UnitStateMachine
        state_machine = UnitStateMachine(unit_id, manifest_path)
        current_state = state_machine.get_current_state()
        
        # Определяем следующий цикл
        next_cycle = min(current_cycle + 1, 3)
        
        # Определяем целевое состояние
        if current_state == UnitState.CLASSIFIED_1:
            # Из CLASSIFIED_1 переходим в PENDING_NORMALIZE, затем в CLASSIFIED_2
            # Сначала переводим в PENDING_NORMALIZE (если не dry_run)
            if not dry_run:
                update_unit_state(
                    unit_path=unit_path,
                    new_state=UnitState.PENDING_NORMALIZE,
                    cycle=current_cycle,
                    operation={
                        "type": "normalize",
                        "status": "pending",
                    },
                )
            # Целевое состояние после нормализации
            new_state = UnitState.CLASSIFIED_2
        elif current_state == UnitState.PENDING_NORMALIZE:
            # Уже в PENDING_NORMALIZE, переводим в CLASSIFIED_2
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
            
            # Переходим в целевое состояние
            update_unit_state(
                unit_path=target_dir,
                new_state=new_state,
                cycle=next_cycle,
                operation={
                    "type": "normalize",
                    "status": "success",
                    "subtype": "extension",
                    "files_normalized": len(normalized_files),
                },
            )

        # Логируем операцию
        self.audit_logger.log_event(
            unit_id=unit_id,
            event_type="operation",
            operation="normalize",
            details={
                "subtype": "extension",
                "cycle": current_cycle,
                "files_normalized": len(normalized_files),
                "extension": extension,
                "target_directory": str(target_dir),
                "errors": errors,
            },
            state_before=manifest.get("state_machine", {}).get("current_state") if manifest else None,
            state_after=new_state.value,
            unit_path=target_dir,
        )

        return {
            "unit_id": unit_id,
            "files_normalized": len(normalized_files),
            "normalized_files": normalized_files,
            "errors": errors,
            "moved_to": str(target_dir),
            "extension": extension,
        }


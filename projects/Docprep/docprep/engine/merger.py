"""
Merger - объединение UNIT из Merge/Direct, Merge/Processing_1, Processing_2, Processing_3 в Ready2Docling.

Сборка финальных UNIT с дедупликацией и сортировкой по расширениям.

НОВАЯ СТРУКТУРА v2:
- Merge/Direct/ - для прямых файлов готовых к Docling (без обработки)
- Merge/Processing_N/ - для обработанных units в цикле N
"""
import json
import shutil
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

from ..core.manifest import (
    load_manifest,
    save_manifest,
    update_manifest_operation,
    create_unit_meta_from_manifest,  # ★ Для создания unit.meta.json в Ready2Docling
)
from ..core.audit import get_audit_logger
from ..core.unit_processor import update_unit_state
from ..core.state_machine import UnitState
from ..core.contract import generate_contract_from_manifest, save_contract
from ..utils.file_ops import detect_file_type

logger = logging.getLogger(__name__)


def _sanitize_extension(ext: str) -> str:
    """Sanitize extension for use as directory name.

    Преобразует расширение файла в безопасное имя директории.

    Args:
        ext: Расширение файла (может быть с точкой или без)

    Returns:
        Санитизированное расширение (lowercase, без точки, без опасных символов)

    Raises:
        ValueError: Если расширение пустое или становится пустым после санитизации.
                   Это указывает на баг классификатора - файлы без расширений
                   должны фильтроваться в Exceptions на предыдущих этапах.
    """
    if not ext:
        raise ValueError(
            "File without extension reached merger - this is a classifier bug. "
            "Such files should be filtered to Exceptions before merge."
        )

    # Lowercase и удаление точки в начале
    ext = ext.lower().lstrip(".")

    if not ext:
        raise ValueError(
            "Empty extension after removing dot - this is a classifier bug. "
            "Such files should be filtered to Exceptions before merge."
        )

    # Ограничение длины
    if len(ext) > 10:
        ext = ext[:10]

    # Удаление опасных символов для filesystem
    # Windows/Linux forbidden chars: < > : " / \ | ? * и null byte
    dangerous_chars = '<>:"/\\|?*\x00'
    for char in dangerous_chars:
        ext = ext.replace(char, "")

    if not ext:
        raise ValueError(
            f"Extension became empty after sanitization - this is a classifier bug. "
            f"Files with only special characters should be filtered to Exceptions."
        )

    return ext


def safe_move_unit(source: Path, target_base_dir: Path, unit_id: Optional[str] = None) -> Path:
    """
    Безопасное перемещение UNIT с проверкой manifest.

    Перемещает UNIT в целевую директорию с гарантией целостности manifest.json.
    После перемещения проверяет существование и валидность manifest.
    При ошибке выполняет откат перемещения.

    Args:
        source: Исходная директория UNIT
        target_base_dir: Базовая директория для перемещения
        unit_id: Идентификатор UNIT (опционально, извлекается из source если не указан)

    Returns:
        Путь к целевой директории UNIT

    Raises:
        ValueError: Если manifest.json отсутствует до перемещения
        RuntimeError: Если manifest.json отсутствует после перемещения
        IOError: При ошибках файловой системы
    """
    import tempfile
    import os

    if unit_id is None:
        unit_id = source.name

    # 1. Проверяем manifest ДО перемещения
    manifest_before = source / "manifest.json"
    if not manifest_before.exists():
        raise ValueError(f"manifest.json missing before move: {unit_id} in {source}")

    # Проверяем валидность JSON
    try:
        with open(manifest_before, "r", encoding="utf-8") as f:
            manifest_data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        raise ValueError(f"manifest.json is corrupted before move: {unit_id} - {e}")

    # 2. Создаём временную директорию для промежуточного копирования
    temp_dir = None
    try:
        temp_dir = Path(tempfile.mkdtemp(prefix=f"safe_move_{unit_id}_"))
        temp_target = temp_dir / unit_id

        # 3. Копируем всё во временную директорию (не move!)
        if source.is_dir():
            shutil.copytree(source, temp_target)
        else:
            shutil.copy2(source, temp_target)

        # 4. Проверяем manifest ПОСЛЕ копирования
        manifest_after = temp_target / "manifest.json"
        if not manifest_after.exists():
            if temp_dir:
                shutil.rmtree(temp_dir)
            raise RuntimeError(f"manifest.json missing after copy: {unit_id}")

        # Проверяем валидность JSON после копирования
        try:
            with open(manifest_after, "r", encoding="utf-8") as f:
                _ = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            if temp_dir:
                shutil.rmtree(temp_dir)
            raise RuntimeError(f"manifest.json corrupted after copy: {unit_id} - {e}")

        # 5. Проверяем размер файла (дополнительная валидация)
        size_before = manifest_before.stat().st_size
        size_after = manifest_after.stat().st_size
        if size_before != size_after:
            if temp_dir:
                shutil.rmtree(temp_dir)
            raise RuntimeError(
                f"manifest.json size mismatch after copy: {unit_id} "
                f"(before={size_before}, after={size_after})"
            )

        # 6. Создаём финальную целевую директорию
        target_base_dir.mkdir(parents=True, exist_ok=True)
        final_target = target_base_dir / unit_id

        # 7. Перемещаем из временной в финальную директорию
        if final_target.exists():
            # Если целевая директория существует, удаляем её (с логированием)
            logger.warning(f"Target directory already exists, will be replaced: {final_target}")
            shutil.rmtree(final_target)

        shutil.move(str(temp_target), str(final_target))

        # 8. Финальная проверка manifest в целевой директории
        manifest_final = final_target / "manifest.json"
        if not manifest_final.exists():
            # Это критическая ошибка - пытаемся восстановить из временной
            logger.error(f"CRITICAL: manifest.json missing in final target: {final_target}")
            raise RuntimeError(f"manifest.json missing in final target: {unit_id}")

        # 9. Только после всех проверок удаляем исходную директорию
        try:
            if source.exists():
                shutil.rmtree(source)
                logger.debug(f"Successfully removed source after safe move: {source}")
        except Exception as e:
            # Не критично если исходная не удалилась - главное, данные в целевой
            logger.warning(f"Failed to remove source directory {source}: {e}")

        logger.info(f"Safe move completed: {unit_id} -> {final_target}")
        return final_target

    except Exception as e:
        # При любой ошибке пробуем откатить временную директорию
        if temp_dir and temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
                logger.debug(f"Cleaned up temp directory after error: {temp_dir}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup temp directory: {cleanup_error}")
        raise


class Merger:
    """Объединитель UNIT для финальной сборки."""

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
        """Инициализирует Merger."""
        self.audit_logger = get_audit_logger()

    def _get_registration_number(self, unit_path: Path) -> str:
        """
        Извлекает registrationNumber из unit.meta.json или manifest.json.

        Args:
            unit_path: Путь к директории UNIT

        Returns:
            Registration number или пустая строка если не найден
        """
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

    def _record_merge_event(
        self,
        unit_id: str,
        registration_number: str,
        files_count: int,
        file_type: str,
        duration_ms: int,
        status: str = "success",
        error: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Записывает UnitEvent для слияния в PipelineTracker.

        Args:
            unit_id: Идентификатор UNIT
            registration_number: Регистрационный номер (primary trace ID)
            files_count: Количество файлов в UNIT
            file_type: Тип файлов (dominant_type)
            duration_ms: Длительность операции в миллисекундах
            status: Статус операции ("success" или "failed")
            error: Описание ошибки (если есть)
            metrics: Дополнительные метрики
        """
        if not Merger._tracker_run_id or not Merger._db_client:
            return

        try:
            from docreciv.pipeline.events import EventType, EventStatus, Stage

            event_metrics = {
                "files_count": files_count,
                "file_type": file_type,
            }
            if metrics:
                event_metrics.update(metrics)

            Merger._db_client.record_unit_event(
                unit_id=unit_id,
                run_id=Merger._tracker_run_id,
                registration_number=registration_number,
                event_type=EventType.PROCESSED,  # PROCESSED вместо MERGED (нет такого типа в EventType)
                stage=Stage.DOCPREP,
                status=EventStatus.COMPLETED if status == "success" else EventStatus.FAILED,
                metrics=event_metrics,
                error=error,
                duration_ms=duration_ms
            )
        except ImportError as e:
            logger.debug(f"PipelineTracker events not available: {e}")
        except Exception as e:
            logger.warning(f"Failed to record merge event for {unit_id}: {e}")

    def _move_skipped_unit(self, unit_id: str, source_path: Path, exceptions_base: Path, reason: str, cycle: int) -> None:
        """
        Перемещает UNIT в директорию исключений с указанием причины.

        НОВАЯ СТРУКТУРА v2:
        - Exceptions/Direct/ - для исключений до обработки (цикл 1)
        - Exceptions/Processing_N/ - для исключений после обработки (цикл N)
        """
        # Для цикла 1 используем Direct, для остальных Processing_N
        if cycle == 1:
            target_base_dir = exceptions_base / "Direct" / reason
        else:
            target_base_dir = exceptions_base / f"Processing_{cycle}" / reason
        try:
            # Используем safe_move_unit для защиты manifest при перемещении
            target_path = safe_move_unit(source_path, target_base_dir, unit_id)
            logger.info(f"Unit {unit_id} moved to Exceptions/{reason} due to: {reason}")
            # Обновляем манифест, если он существует
            manifest_path = target_path / "manifest.json"
            if manifest_path.exists():
                update_unit_state(
                    unit_path=target_path,
                    new_state=UnitState.MERGER_SKIPPED,
                    cycle=cycle,
                    operation={
                        "type": "merger_skip",
                        "reason": reason,
                    },
                    final_cluster=f"Exceptions/{reason}",
                    final_reason=f"Skipped by Merger: {reason}",
                )
        except Exception as e:
            logger.error(f"Failed to move unit {unit_id} to Exceptions/{reason}: {e}")

    def _move_to_er_merge(self, unit_id: str, source_path: Path, er_merge_base: Path, reason: str, cycle: int) -> None:
        """
        Перемещает UNIT в директорию ErMerge с указанием причины.
        """
        target_base_dir = er_merge_base / f"Cycle_{cycle}"
        try:
            # Используем safe_move_unit для защиты manifest при перемещении
            target_path = safe_move_unit(source_path, target_base_dir, unit_id)
            logger.info(f"Unit {unit_id} moved to ErMerge/{reason} due to: {reason}")
            # Обновляем манифест, если он существует
            manifest_path = target_path / "manifest.json"
            if manifest_path.exists():
                update_unit_state(
                    unit_path=target_path,
                    new_state=UnitState.MERGER_SKIPPED,
                    cycle=cycle,
                    operation={
                        "type": "merger_error",
                        "reason": reason,
                    },
                    final_cluster=f"ErMerge/{reason}",
                    final_reason=f"Error during final merge: {reason}",
                )
        except Exception as e:
            logger.error(f"Failed to move unit {unit_id} to ErMerge/{reason}: {e}")

    def _validate_unit_files(self, files: List[Path]) -> List[Path]:
        """
        Валидирует файлы UNIT - проверяет существование и непустоту.

        Args:
            files: Список путей к файлам

        Returns:
            Список валидных (существующих и непустых) файлов
        """
        valid_files = []
        for file_path in files:
            if file_path.exists() and file_path.stat().st_size > 0:
                valid_files.append(file_path)
        return valid_files

    def _determine_dominant_file_type(self, files: List[Path]) -> str:
        """
        Определяет доминирующий тип файлов в UNIT.

        Args:
            files: Список путей к файлам

        Returns:
            Строка с доминирующим типом файла
        """
        file_types = {}
        for file_path in files:
            detection = detect_file_type(file_path)
            file_type = detection.get("detected_type", "unknown")

            # Если detected_type unknown, проверяем расширение файла
            if file_type == "unknown":
                file_ext = file_path.suffix.lower().lstrip(".")
                file_name_lower = file_path.name.lower()

                # Проверяем PDF по расширению (включая неправильные расширения)
                if file_ext == "pdf" or file_name_lower.endswith("pdf") or file_name_lower.endswith(".pdf"):
                    file_type = "pdf"
                # Проверяем другие типы по расширению
                elif file_ext in ["docx", "doc", "xlsx", "xls", "pptx", "ppt", "rtf", "xml", "jpg", "jpeg", "png"]:
                    file_type = file_ext

            file_types[file_type] = file_types.get(file_type, 0) + 1

        # Определяем доминирующий тип
        if file_types:
            dominant_type = max(file_types.items(), key=lambda x: x[1])[0]
        else:
            dominant_type = "unknown"

        # Дополнительная проверка: если dominant_type unknown, но есть PDF файлы по расширению
        if dominant_type == "unknown":
            pdf_count = 0
            for file_path in files:
                file_ext = file_path.suffix.lower().lstrip(".")
                file_name_lower = file_path.name.lower()
                if file_ext == "pdf" or file_name_lower.endswith("pdf") or file_name_lower.endswith(".pdf"):
                    pdf_count += 1
            if pdf_count > 0:
                dominant_type = "pdf"

        return dominant_type

    def _check_unit_processing_state(
        self,
        unit_id: str,
        unit_info: Dict[str, Any],
        dominant_type: str,
        files: List[Path],
        exceptions_base: Path,
        current_cycle: int,
    ) -> Optional[str]:
        """
        Проверяет состояние обработки UNIT и возвращает причину пропуска или None.

        Args:
            unit_id: Идентификатор UNIT
            unit_info: Информация о UNIT
            dominant_type: Доминирующий тип файлов
            files: Список файлов
            exceptions_base: Базовая директория для исключений
            current_cycle: Текущий цикл обработки

        Returns:
            Причина пропуска (строка) или None если UNIT валиден
        """
        manifest = unit_info.get("manifest")
        if not manifest:
            return None

        processing_info = manifest.get("processing", {})

        # Проверяем, что UNIT прошел классификацию
        state = manifest.get("state_machine", {}).get("current_state", "")
        if state not in ["MERGED_DIRECT", "MERGED_PROCESSED", "CLASSIFIED_1", "CLASSIFIED_2", "CLASSIFIED_3"]:
            return "skip_state"  # Специальный маркер для пропуска без перемещения

        # Дополнительная проверка по расширениям файлов
        archive_extensions = {".zip", ".rar", ".7z", ".tar", ".gz"}
        unsupported_extensions = {".exe", ".dll", ".bin", ".msi", ".bat", ".cmd", ".sh"}

        # Проверяем, был ли архив успешно извлечен
        # Проверяем в processing.operations, applied_operations и files[].transformations
        all_operations = (
            processing_info.get("operations", []) or
            manifest.get("applied_operations", [])
        )
        extraction_successful = any(
            op.get("type") == "extract" and op.get("status") == "success"
            for op in all_operations
        ) or any(
            t.get("type") == "extract" and t.get("status") == "success"
            for f in manifest.get("files", [])
            for t in f.get("transformations", [])
        )

        # Определяем какие файлы проверять
        files_to_check = []
        for f in files:
            is_archive = f.suffix.lower() in archive_extensions
            # Если архив был извлечен, пропускаем исходные архивы
            if is_archive and extraction_successful:
                continue
            # Пропускаем извлеченные директории
            if f.is_dir() and "_extracted" in f.name:
                continue
            files_to_check.append(f)

        # Если после извлечения есть только архивы — проверяем извлеченный контент
        if not files_to_check and extraction_successful:
            extracted_dirs = [f for f in files if f.is_dir() and "_extracted" in f.name]
            for extracted_dir in extracted_dirs:
                if extracted_dir.exists():
                    files_to_check.extend([f for f in extracted_dir.iterdir() if f.is_file()])

        # Проверяем на неподдерживаемые расширения
        if any(f.suffix.lower() in unsupported_extensions for f in files_to_check):
            return "UnsupportedExtension"

        # Проверяем операции обработки
        # Операции могут быть в processing.operations ИЛИ в applied_operations (корневой уровень)
        operations = processing_info.get("operations", [])
        if not operations:
            operations = manifest.get("applied_operations", [])

        # Для файлов, требующих конвертации, проверяем наличие операции convert
        if dominant_type in ["doc", "xls", "ppt", "rtf"]:
            has_convert = any(op.get("type") == "convert" for op in operations)
            if not has_convert:
                return "ErConvert"

        # Для архивов проверяем наличие УСПЕШНОЙ операции extract с files_extracted > 0
        # ИСПРАВЛЕНО: Добавлен "zip_or_office" для корректной обработки архивов с неоднозначной детекцией
        if dominant_type in ["zip_archive", "rar_archive", "7z_archive", "zip_or_office", "zip"]:
            # ИСПРАВЛЕНО: Проверяем не только наличие операции, но и её успешность
            # Архив с files_extracted=0 должен идти в ErExtract
            extraction_successful = False

            # Проверяем в processing.operations и applied_operations
            for op in operations:
                if op.get("type") == "extract":
                    # Если есть files_extracted - проверяем что > 0
                    if "files_extracted" in op:
                        if op.get("files_extracted", 0) > 0:
                            extraction_successful = True
                            break
                    # Обратная совместимость: если нет files_extracted, проверяем status=success
                    elif op.get("status") == "success":
                        extraction_successful = True
                        break

            # ИСПРАВЛЕНО: Также проверяем в files[].transformations
            if not extraction_successful:
                for f in manifest.get("files", []):
                    for t in f.get("transformations", []):
                        if t.get("type") == "extract":
                            if "files_extracted" in t:
                                if t.get("files_extracted", 0) > 0:
                                    extraction_successful = True
                                    break
                            elif t.get("status") == "success":
                                extraction_successful = True
                                break
                    if extraction_successful:
                        break

            if not extraction_successful:
                # Архивы без успешного извлечения (или с 0 файлов) -> Exceptions/ErExtract
                return "ErExtract"
            return "skip_state"  # Архив успешно извлечен - пропускаем оригинал

        # Фильтрация форматов, неподдерживаемых Docling
        docling_unsupported_types = [
            "zip_archive", "rar_archive", "7z_archive", "zip_or_office", "zip",
            "exe", "dll", "bin",
        ]
        if dominant_type in docling_unsupported_types and not extraction_successful:
            return "UnsupportedType"

        return None

    def _copy_and_finalize_unit(
        self,
        unit_id: str,
        unit_info: Dict[str, Any],
        files: List[Path],
        target_unit_dir: Path,
        dominant_type: str,
    ) -> List[str]:
        """
        Копирует файлы UNIT в целевую директорию и финализирует манифест.

        Args:
            unit_id: Идентификатор UNIT
            unit_info: Информация о UNIT
            files: Список файлов для копирования
            target_unit_dir: Целевая директория UNIT
            dominant_type: Доминирующий тип файлов

        Returns:
            Список путей скопированных файлов
        """
        # Копируем файлы напрямую в UNIT директорию (без поддиректории files)
        copied_files = []
        for file_path in files:
            target_file = target_unit_dir / file_path.name
            # Избегаем перезаписи существующих файлов
            if not target_file.exists():
                shutil.copy2(file_path, target_file)
            copied_files.append(str(target_file))

        # Копируем manifest если есть
        if unit_info["manifest"]:
            manifest = unit_info["manifest"]
            # Обновляем состояние на READY_FOR_DOCLING
            manifest["processing"]["final_cluster"] = unit_info["source"]
            manifest["state_machine"]["current_state"] = "READY_FOR_DOCLING"
            manifest["state_machine"]["final_state"] = "READY_FOR_DOCLING"
            if "READY_FOR_DOCLING" not in manifest["state_machine"]["state_trace"]:
                manifest["state_machine"]["state_trace"].append("READY_FOR_DOCLING")

            save_manifest(target_unit_dir, manifest)

            # ★ ЕДИНАЯ СИСТЕМА ТРЕЙСИНГА: Создаём unit.meta.json для Ready2Docling
            # Пропагируем registrationNumber из manifest в unit.meta.json
            try:
                create_unit_meta_from_manifest(unit_info["source_path"], target_unit_dir)
                logger.debug(f"Created unit.meta.json for {unit_id}")
            except Exception as meta_error:
                logger.warning(f"Failed to create unit.meta.json for {unit_id}: {meta_error}")

            # Генерируем docprep.contract.json для Docling
            self._generate_contract_for_unit(unit_id, manifest, target_unit_dir, copied_files)

        return copied_files

    def _generate_contract_for_unit(
        self,
        unit_id: str,
        manifest: Dict[str, Any],
        target_unit_dir: Path,
        copied_files: List[str],
    ) -> None:
        """
        Генерирует docprep.contract.json для UNIT.

        Args:
            unit_id: Идентификатор UNIT
            manifest: Манифест UNIT
            target_unit_dir: Целевая директория UNIT
            copied_files: Список скопированных файлов
        """
        try:
            # Определяем главный файл для контракта
            main_file_for_contract = None
            for file_path_str in copied_files:
                file_path_obj = Path(file_path_str)
                # Используем первый скопированный файл как главный
                if file_path_obj.exists():
                    main_file_for_contract = file_path_obj
                    break

            if main_file_for_contract:
                contract = generate_contract_from_manifest(
                    unit_path=target_unit_dir,
                    manifest=manifest,
                    main_file_path=main_file_for_contract,
                )
                save_contract(target_unit_dir, contract)
                logger.info(f"Generated docprep.contract.json for unit {unit_id}")
            else:
                logger.warning(f"No main file found for contract generation in unit {unit_id}")
        except Exception as e:
            # Ошибка генерации контракта критична - без контракта Docling не сможет обработать UNIT
            logger.error(f"CRITICAL: Failed to generate contract for unit {unit_id}: {e}")

    def collect_units(
        self,
        source_dirs: List[Path],
        target_dir: Path,
        cycle: Optional[int] = None,
        er_merge_base: Optional[Path] = None,
    ) -> Dict[str, Any]:
        """
        Собирает UNIT из нескольких источников в целевую директорию.

        НОВАЯ СТРУКТУРА v2:
        Args:
            source_dirs: Список директорий источников (Merge/Direct, Merge/Processing_1, Processing_2, Processing_3)
            target_dir: Целевая директория (Ready2Docling)
            cycle: Номер цикла (опционально, для фильтрации)
            er_merge_base: Базовая директория ErMerge (опционально)

        Returns:
            Словарь с результатами объединения
        """
        correlation_id = self.audit_logger.get_correlation_id()

        # UnitEvents: начинаем отслеживание времени
        import time
        start_time = time.time()

        # Собираем все UNIT из источников
        all_units = {}  # unit_id -> {source, files, cycle, source_path}

        for source_dir in source_dirs:
            if not source_dir.exists():
                continue

            # Обрабатываем все директории рекурсивно
            # Новая структура: Merge/Direct/, Merge/Processing_N/
            for unit_dir in source_dir.rglob("UNIT_*"):
                if unit_dir.is_dir():
                    self._process_unit_dir(unit_dir, source_dir.name, all_units)

        # Определяем Exceptions base
        exceptions_base = target_dir.parent / "Exceptions"
        current_cycle = cycle or 1

        # Обрабатываем каждый UNIT
        processed_units = []
        errors = []

        for unit_id, unit_info in all_units.items():
            try:
                # Проверяем наличие файлов
                files = unit_info["files"]
                if not files:
                    self._move_skipped_unit(unit_id, unit_info["source_path"], exceptions_base, "Empty", current_cycle)
                    continue

                # Валидируем файлы (существование и непустота)
                files = self._validate_unit_files(files)
                if not files:
                    self._move_skipped_unit(unit_id, unit_info["source_path"], exceptions_base, "Empty", current_cycle)
                    continue

                # Определяем доминирующий тип файлов
                dominant_type = self._determine_dominant_file_type(files)

                # Проверяем состояние обработки UNIT
                skip_reason = self._check_unit_processing_state(
                    unit_id, unit_info, dominant_type, files, exceptions_base, current_cycle
                )
                if skip_reason:
                    if skip_reason == "skip_state":
                        # Пропускаем без перемещения
                        continue
                    # Перемещаем в соответствующую директорию исключений
                    logger.warning(
                        f"Unit {unit_id} skipped due to: {skip_reason}. "
                        f"Moving to Exceptions/{skip_reason}."
                    )
                    self._move_skipped_unit(unit_id, unit_info["source_path"], exceptions_base, skip_reason, current_cycle)
                    continue

                # Определяем целевую директорию с сортировкой по расширению
                target_subdir = self._get_target_subdir(
                    dominant_type, files, target_dir, unit_info.get("manifest")
                )

                # Создаем директорию UNIT
                target_unit_dir = target_subdir / unit_id
                target_unit_dir.mkdir(parents=True, exist_ok=True)

                # Копируем файлы и финализируем UNIT
                copied_files = self._copy_and_finalize_unit(
                    unit_id, unit_info, files, target_unit_dir, dominant_type
                )

                # UnitEvents: записываем событие успешного слияния
                duration_ms = int((time.time() - start_time) * 1000)
                registration_number = self._get_registration_number(unit_info["source_path"])
                self._record_merge_event(
                    unit_id=unit_id,
                    registration_number=registration_number,
                    files_count=len(copied_files),
                    file_type=dominant_type,
                    duration_ms=duration_ms,
                    status="success",
                    metrics={"source": unit_info["source"], "target_dir": str(target_unit_dir)}
                )

                processed_units.append(
                    {
                        "unit_id": unit_id,
                        "source": unit_info["source"],
                        "files_count": len(copied_files),
                        "target_dir": str(target_unit_dir),
                        "file_type": dominant_type,
                    }
                )

                # Логируем операцию
                self.audit_logger.log_event(
                    unit_id=unit_id,
                    event_type="operation",
                    operation="merge",
                    details={
                        "source": unit_info["source"],
                        "files_count": len(copied_files),
                        "target_dir": str(target_unit_dir),
                    },
                    state_before="MERGED",
                    state_after="READY_FOR_DOCLING",
                    unit_path=target_unit_dir,
                )

                # Удаляем исходную директорию UNIT после успешного копирования
                source_unit_path = unit_info["source_path"]
                if source_unit_path.exists():
                    try:
                        shutil.rmtree(source_unit_path)
                        logger.debug(f"Removed source unit directory: {source_unit_path}")
                    except Exception as e:
                        logger.warning(f"Failed to remove source unit directory {source_unit_path}: {e}")

            except Exception as e:
                # UnitEvents: записываем событие об ошибке слияния
                duration_ms = int((time.time() - start_time) * 1000)
                registration_number = self._get_registration_number(unit_info["source_path"])
                self._record_merge_event(
                    unit_id=unit_id,
                    registration_number=registration_number,
                    files_count=len(files) if files else 0,
                    file_type=dominant_type if 'dominant_type' in locals() else "unknown",
                    duration_ms=duration_ms,
                    status="failed",
                    error=str(e)
                )

                errors.append({"unit_id": unit_id, "error": str(e)})
                # Перемещаем UNIT в ErMerge при критических ошибках
                if er_merge_base:
                    try:
                        self._move_to_er_merge(unit_id, unit_info["source_path"], er_merge_base, str(e), current_cycle)
                    except Exception as move_error:
                        logger.error(f"Failed to move unit {unit_id} to ErMerge: {move_error}")

        return {
            "units_processed": len(processed_units),
            "processed_units": processed_units,
            "errors": errors,
        }

    def _process_unit_dir(self, unit_dir: Path, source_name: str, all_units: Dict[str, Any]) -> None:
        """Обрабатывает директорию UNIT и добавляет её в коллекцию."""
        unit_id = unit_dir.name
        # Приоритет более поздним циклам
        if unit_id in all_units:
            # Если UNIT уже есть, проверяем, не является ли новый источник более приоритетным
            # В списке source_dirs порядок: Direct, Processing_1, Processing_2, Processing_3
            # Поэтому каждый следующий источник более приоритетен
            all_units[unit_id]["files"] = []  # Очищаем старые файлы
            all_units[unit_id]["manifest"] = None # Сбросим манифест для загрузки нового
            all_units[unit_id]["source"] = source_name
            all_units[unit_id]["source_path"] = unit_dir
        else:
            all_units[unit_id] = {
                "source": source_name,
                "files": [],
                "manifest": None,
                "source_path": unit_dir,
            }

        # Рекурсивный поиск файлов
        # ИСПРАВЛЕНО: Исключаем JSON метаданные которые влияют на _determine_dominant_file_type
        excluded_metadata = {
            "manifest.json", "audit.log.jsonl", "metadata.json",
            "docprep.contract.json", "raw_url_map.json", "unit.meta.json"
        }
        files = [
            f
            for f in unit_dir.rglob("*")
            if f.is_file() and f.name not in excluded_metadata
        ]
        
        # Фильтрация файлов - исключаем служебные и временные файлы
        filtered_files = []
        for file_path in files:
            # Пропускаем файлы в скрытых директориях
            if any(part.startswith('.') for part in file_path.parts):
                continue
            # Пропускаем временные файлы
            if file_path.name.startswith('~') or file_path.name.startswith('.'):
                continue
            filtered_files.append(file_path)
        
        all_units[unit_id]["files"].extend(filtered_files)

        # Загружаем manifest
        manifest_path = unit_dir / "manifest.json"
        if manifest_path.exists() and all_units[unit_id]["manifest"] is None:
            try:
                all_units[unit_id]["manifest"] = load_manifest(unit_dir)
            except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
                logger.debug(f"Could not load manifest for {unit_id}: {e}")

    def _get_target_subdir(
        self, file_type: str, files: List[Path], base_dir: Path, manifest: Optional[Dict] = None
    ) -> Path:
        """
        Определяет целевую поддиректорию для UNIT на основе типа файлов.

        Для PDF дополнительно сортирует на scan/text на основе needs_ocr из manifest.

        Args:
            file_type: Тип файла
            files: Список файлов
            base_dir: Базовая директория Ready2Docling
            manifest: Manifest UNIT для получения needs_ocr и route

        Returns:
            Путь к целевой поддиректории
        """
        # 1. Пытаемся определить по route из manifest (самый надежный способ)
        if manifest:
            # ИСПРАВЛЕНО: Проверяем is_mixed из classification ПЕРВЫМ
            # is_mixed флаг должен иметь приоритет над route
            classification = manifest.get("processing", {}).get("classification", {})
            if classification.get("is_mixed", False):
                return base_dir / "Mixed"

            route = manifest.get("processing", {}).get("route")
            if route == "mixed":
                return base_dir / "Mixed"
            if route:
                # Маппинг route на директории
                route_to_dir = {
                    "pdf_text": "pdf",
                    "pdf_scan": "pdf",
                    "pdf_scan_table": "pdf",
                    "docx": "docx",
                    "xlsx": "xlsx",
                    "pptx": "pptx",
                    "html": "html",
                    "xml": "xml",
                    "image_ocr": "image",
                    "rtf": "rtf",
                }
                
                # Специальная обработка для изображений (сортируем по расширению)
                # ИСПРАВЛЕНО: Расширен список поддерживаемых изображений
                if route == "image_ocr" and files:
                    ext = files[0].suffix.lower().lstrip(".")
                    # Поддерживаем все распространённые форматы изображений
                    if ext in ["jpg", "jpeg", "png", "tiff", "tif", "bmp", "gif", "webp"]:
                        return base_dir / ext
                    # Fallback только для действительно неизвестных форматов
                    return base_dir / "image"

                # ИСПРАВЛЕНО: Обработка route="convert" для уже конвертированных файлов
                # Файлы с route="convert" должны быть отсортированы по фактическому расширению
                if route == "convert" and files:
                    ext = files[0].suffix.lower().lstrip(".")
                    # Определяем директорию по реальному расширению файла
                    if ext in ["docx", "xlsx", "pptx", "pdf", "jpg", "jpeg", "png", "tiff", "tif", "bmp", "gif", "webp", "html", "xml", "json"]:
                        return base_dir / ext
                    # Если старый формат (.doc, .xls, .ppt) — это ошибка маршрутизации
                    logger.warning(f"File with convert route but unconverted format: {ext} in {files[0].name}")

                subdir_name = route_to_dir.get(route)
                if subdir_name:
                    # Для PDF добавляем проверку на scan/text если нужно (но route уже содержит это)
                    # Однако для обратной совместимости с существующей структурой папок:
                    if subdir_name == "pdf":
                        # Проверяем, есть ли scan в route
                        if "scan" in route:
                            return base_dir / "pdf" / "scan"
                        return base_dir / "pdf" / "text"
                    
                    return base_dir / subdir_name

        # 2. Fallback: Маппинг типов на директории
        # ВАЖНО: .doc файлы НИКОГДА не должны попадать в docx без предварительной конвертации
        # ИСПРАВЛЕНО: Добавлены tif, bmp, gif для изображений
        type_to_dir = {
            "docx": "docx",
            # "doc": "docx",  # УДАЛЕНО: .doc файлы должны быть конвертированы, а не напрямую в docx
            "pdf": "pdf",
            "jpg": "jpg",
            "jpeg": "jpeg",
            "png": "png",
            "tiff": "tiff",
            "tif": "tiff",   # ИСПРАВЛЕНО: tif → tiff
            "bmp": "bmp",    # ИСПРАВЛЕНО: добавлено
            "gif": "gif",    # ИСПРАВЛЕНО: добавлено
            "webp": "webp",  # ИСПРАВЛЕНО: добавлено
            "pptx": "pptx",
            "xlsx": "xlsx",
            "xml": "xml",
            "rtf": "rtf",
        }
        
        if file_type == "mixed":
            return base_dir / "Mixed"

        subdir_name = type_to_dir.get(file_type)

        # Если тип не найден в маппинге, проверяем расширение напрямую
        if not subdir_name and files:
            # Проверяем расширения файлов
            extensions = set()
            for file_path in files:
                ext = file_path.suffix.lower().lstrip(".")
                if ext:
                    extensions.add(ext)
            
            # Маппинг расширений на директории
            # ИСПРАВЛЕНО: Добавлены tif, bmp, gif, webp
            ext_to_dir = {
                "docx": "docx",
                "pdf": "pdf",
                "jpg": "jpg",
                "jpeg": "jpeg",
                "png": "png",
                "tiff": "tiff",
                "tif": "tiff",   # ИСПРАВЛЕНО: добавлено
                "bmp": "bmp",    # ИСПРАВЛЕНО: добавлено
                "gif": "gif",    # ИСПРАВЛЕНО: добавлено
                "webp": "webp",  # ИСПРАВЛЕНО: добавлено
                "pptx": "pptx",
                "xlsx": "xlsx",
                "xml": "xml",
                "rtf": "rtf",
            }
            
            # Берем первое найденное расширение
            if extensions:
                first_ext = list(extensions)[0]
                # Проверяем, является ли это PDF
                is_pdf_by_ext = False
                for ext in extensions:
                    if ext == "pdf" or ext.endswith("pdf"):
                        is_pdf_by_ext = True
                        first_ext = "pdf"
                        break
                subdir_name = ext_to_dir.get(first_ext) or _sanitize_extension(first_ext)

        # 3. Для PDF (fallback логика)
        is_pdf_unit = False
        if file_type == "pdf":
            is_pdf_unit = True
        else:
            for file_path in files:
                file_ext = file_path.suffix.lower()
                if file_ext == ".pdf" or file_path.name.lower().endswith(".pdf"):
                    is_pdf_unit = True
                    break
        
        if is_pdf_unit:
            pdf_files_need_ocr = []
            pdf_files_has_text = []

            if manifest:
                file_infos = manifest.get("files", [])
                for file_info in file_infos:
                    # Проверяем как по detected_type, так и по расширению файла
                    detected_type = file_info.get("detected_type", "").lower()
                    original_name = file_info.get("original_name", "")
                    current_name = file_info.get("current_name", original_name)
                    
                    # Проверяем, является ли файл PDF (по типу или расширению)
                    is_pdf = (
                        detected_type == "pdf" or
                        original_name.lower().endswith(".pdf") or
                        current_name.lower().endswith(".pdf") or
                        original_name.lower().endswith("pdf") or  # Для случаев типа "гpdf"
                        current_name.lower().endswith("pdf")
                    )
                    
                    if is_pdf:
                        needs_ocr = file_info.get("needs_ocr")
                        if needs_ocr is not None:
                            pdf_files_need_ocr.append(needs_ocr)
                            pdf_files_has_text.append(not needs_ocr)
                        else:
                            # Fallback to detect_file_type if needs_ocr is not in manifest
                            # Ищем локальный файл для детекции
                            current_name = file_info.get("current_name", "")
                            file_path_in_unit = None
                            for f in files:
                                if f.name == current_name:
                                    file_path_in_unit = f
                                    break
                            
                            if file_path_in_unit and file_path_in_unit.exists():
                                try:
                                    detection = detect_file_type(file_path_in_unit)
                                    needs_ocr_detected = detection.get("needs_ocr", True)
                                    pdf_files_need_ocr.append(needs_ocr_detected)
                                    pdf_files_has_text.append(not needs_ocr_detected)
                                except (OSError, IOError, PermissionError, KeyError) as e:
                                    logger.debug(f"Could not detect file type for {file_path_in_unit}: {e}")
                                    pdf_files_need_ocr.append(True)
                                    pdf_files_has_text.append(False)
            
            # Если не нашли в manifest, проверяем файлы напрямую
            if not pdf_files_need_ocr:
                for file_path in files:
                    file_ext = file_path.suffix.lower()
                    file_name_lower = file_path.name.lower()
                    is_pdf_file = (
                        file_ext == ".pdf" or
                        file_name_lower.endswith(".pdf")
                    )
                    
                    if is_pdf_file:
                        try:
                            detection = detect_file_type(file_path)
                            needs_ocr_detected = detection.get("needs_ocr", True)
                            pdf_files_need_ocr.append(needs_ocr_detected)
                            pdf_files_has_text.append(not needs_ocr_detected)
                        except (OSError, IOError, PermissionError, KeyError) as e:
                            logger.debug(f"Could not detect file type for {file_path}: {e}")
                            pdf_files_need_ocr.append(True)
                            pdf_files_has_text.append(False)
            
            if pdf_files_need_ocr:
                all_need_ocr = all(pdf_files_need_ocr)
                all_has_text = all(pdf_files_has_text)

                if all_need_ocr:
                    subdir_name = "pdf/scan"
                elif all_has_text:
                    subdir_name = "pdf/text"
                else:
                    subdir_name = "pdf/mixed"
            else:
                subdir_name = "pdf/scan"

        # Fallback: если subdir_name всё ещё None, используем тип файла
        if not subdir_name:
            subdir_name = _sanitize_extension(file_type)

        return base_dir / subdir_name

    def collect_to_ready2docling(self, source_dirs: List[Path], target_dir: Path) -> Dict[str, Any]:
        """
        Собирает UNIT в Ready2Docling с сортировкой по типам файлов.

        Ожидаемая структура Ready2Docling:
        - pdf/          # PDF файлы
        - docx/         # DOCX файлы
        - html/         # HTML файлы
        - xlsx/         # XLSX файлы
        - Mixed/        # Mixed UNIT (несколько типов файлов)
        - {ext}/        # Прочие форматы по имени расширения (csv/, txt/, json/ и т.д.)

        Args:
            source_dirs: Список директорий источников (Merge/Direct, Merge/Processing_N)
            target_dir: Целевая директория (Ready2Docling)

        Returns:
            Словарь с результатами:
            - units_processed: количество обработанных UNIT
            - by_type: распределение по типам
        """
        results = {"units_processed": 0, "by_type": {}}

        for source_dir in source_dirs:
            if not source_dir.exists():
                continue

            for unit_dir in source_dir.glob("**/UNIT_*"):
                if not unit_dir.is_dir():
                    continue

                # Определяем тип UNIT (пропускаем пустые)
                try:
                    unit_type = self._determine_unit_type(unit_dir)
                except ValueError as e:
                    # Пустые UNIT пропускаем
                    logger.warning(f"Skipping {unit_dir.name}: {e}")
                    continue

                # Целевая поддиректория
                type_dir = target_dir / unit_type
                type_dir.mkdir(parents=True, exist_ok=True)

                # Копируем UNIT
                dest = type_dir / unit_dir.name
                if not dest.exists():
                    shutil.copytree(unit_dir, dest)
                    results["units_processed"] += 1
                    results["by_type"][unit_type] = results["by_type"].get(unit_type, 0) + 1

                    # ★ ЕДИНАЯ СИСТЕМА ТРЕЙСИНГА: Создаём unit.meta.json в Ready2Docling
                    # Пропагируем registrationNumber из manifest в unit.meta.json
                    try:
                        create_unit_meta_from_manifest(unit_dir, dest)
                        logger.debug(f"Created unit.meta.json in {unit_dir.name}")
                    except Exception as meta_error:
                        logger.warning(f"Failed to create unit.meta.json for {unit_dir.name}: {meta_error}")

                    logger.info(f"Collected {unit_dir.name} to {unit_type}")

        return results

    def _determine_unit_type(self, unit_dir: Path) -> str:
        """
        Определяет тип UNIT по файлам внутри.

        Args:
            unit_dir: Путь к директории UNIT

        Returns:
            Тип UNIT (pdf, docx, html, xlsx, Mixed, или имя расширения)
        """
        files = [f for f in unit_dir.iterdir() if f.is_file() and not f.name.startswith('.')]

        # ИСПРАВЛЕНО: Исключаем служебные файлы и метаданные
        # Это позволяет избежать попадания UNIT в неправильную категорию из-за presence JSON-метаданных
        excluded_files = {
            'manifest.json', 'audit.log.jsonl',
            'raw_url_map.json', 'unit.meta.json',
            'docprep.contract.json',  # Метаданные контракта
        }
        content_files = [f for f in files if f.name not in excluded_files]

        if not content_files:
            raise ValueError(
                f"Empty UNIT {unit_dir.name} reached merger - this is a classifier bug. "
                "UNITs without content files should be filtered to Exceptions before merge."
            )

        # Собираем расширения с приоритетизацией (content > metadata)
        # ИСПРАВЛЕНО: Приоритетные расширения должны выбираться первыми
        priority_exts = ['docx', 'pdf', 'xlsx', 'pptx', 'html', 'jpg', 'jpeg', 'png', 'tiff', 'tif', 'bmp', 'gif']
        extensions = []
        other_extensions = []

        for f in content_files:
            ext = f.suffix.lower().lstrip('.')
            if ext:
                if ext in priority_exts and ext not in extensions:
                    extensions.append(ext)
                elif ext not in extensions and ext not in priority_exts:
                    other_extensions.append(ext)

        # Приоритетные расширения идут первыми
        extensions.extend(other_extensions)

        # Если разные типы - Mixed
        if len(extensions) > 1:
            return "Mixed"

        if not extensions:
            raise ValueError(
                f"UNIT {unit_dir.name} has no file extensions - this is a classifier bug. "
                "Files without extensions should be filtered to Exceptions before merge."
            )

        # Используем первое (и приоритетное) расширение
        ext = extensions[0]
        type_map = {
            'pdf': 'pdf',
            'docx': 'docx',
            'doc': 'docx',
            'xlsx': 'xlsx',
            'xls': 'xlsx',
            'html': 'html',
            'htm': 'html',
            'xml': 'html',
            'pptx': 'pptx',
            'ppt': 'pptx',
            'rtf': 'rtf',
            'jpg': 'jpg',
            'jpeg': 'jpeg',
            'png': 'png',
            'tiff': 'tiff',
            'tif': 'tiff',
            'bmp': 'bmp',
            'gif': 'gif',
            'webp': 'webp',
        }
        return type_map.get(ext) or _sanitize_extension(ext)

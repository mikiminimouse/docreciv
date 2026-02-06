"""
Classifier - классификация файлов и определение категорий обработки.

Оптимизирован для параллельной обработки на многоядерных системах.
"""
import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from collections import Counter
import logging

from ..core.manifest import load_manifest
from ..core.state_machine import UnitState, UnitStateMachine
from ..core.audit import get_audit_logger
from ..core.unit_processor import (
    create_unit_manifest_if_needed,
    move_unit_to_target,
    update_unit_state,
    get_extension_subdirectory,
    determine_unit_extension,
)
from ..core.config import (
    get_processing_paths,
    get_data_paths,
    INPUT_DIR,
)
from ..core.parallel import parallel_map_threads, get_parallel_config
from ..utils.file_ops import detect_file_type
from ..utils.paths import get_unit_files
from ..core.manifest import _determine_route_from_files, save_manifest, load_manifest, get_is_mixed

logger = logging.getLogger(__name__)


class Classifier:
    """
    Классификатор для определения категории обработки UNIT.

    Классифицирует файлы и определяет, куда должен быть направлен UNIT
    для дальнейшей обработки.
    """

    # Расширения для подписей
    SIGNATURE_EXTENSIONS = {".sig", ".p7s", ".pem", ".cer", ".crt"}

    # Неподдерживаемые расширения
    UNSUPPORTED_EXTENSIONS = {".exe", ".dll", ".db", ".tmp", ".log", ".ini", ".sys", ".bat", ".sh"}

    # Типы, требующие конвертации
    CONVERTIBLE_TYPES = {
        "doc": "docx",
        "xls": "xlsx",
        "ppt": "pptx",
    }

    # Глобальное переопределение base_dir (для pipeline с кастомными путями)
    _override_base_dir: Optional[Path] = None

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

    @classmethod
    def set_base_dir(cls, base_dir: Path) -> None:
        """
        Устанавливает глобальную базовую директорию для всех экземпляров Classifier.

        Args:
            base_dir: Базовая директория для обработки
        """
        cls._override_base_dir = base_dir

    @classmethod
    def clear_base_dir(cls) -> None:
        """Очищает переопределение базовой директории."""
        cls._override_base_dir = None

    def __init__(self):
        """Инициализирует Classifier."""
        self.audit_logger = get_audit_logger()
        self._local_base_dir: Optional[Path] = None

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
                    reg_num = meta.get("registrationNumber", "")
                    if reg_num:
                        return reg_num
            except Exception:
                pass

        # 2. Fallback: manifest.json
        manifest_path = unit_path / "manifest.json"
        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
                    reg_num = manifest.get("registration_number", "")
                    if reg_num:
                        return reg_num
            except Exception:
                pass

        return ""  # Пустая строка если не найден

    def _record_classification_event(
        self,
        unit_id: str,
        registration_number: str,
        unit_category: str,
        is_mixed: bool,
        file_count: int,
        duration_ms: int,
        status: str = "success"
    ) -> None:
        """
        Записывает UnitEvent для классификации через PipelineTracker.

        Args:
            unit_id: Идентификатор UNIT
            registration_number: Регистрационный номер
            unit_category: Категория UNIT
            is_mixed: Является ли UNIT mixed
            file_count: Количество файлов
            duration_ms: Длительность операции в миллисекундах
            status: Статус операции (success/failed)
        """
        if not Classifier._tracker_run_id or not Classifier._db_client:
            return

        try:
            # Lazy import для避免 circular dependency
            from docreciv.pipeline.events import EventType, EventStatus, Stage

            Classifier._db_client.record_unit_event(
                unit_id=unit_id,
                run_id=Classifier._tracker_run_id,
                registration_number=registration_number,
                event_type=EventType.PROCESSED,  # PROCESSED вместо CLASSIFIED (нет такого типа в EventType)
                stage=Stage.DOCPREP,
                status=EventStatus.COMPLETED if status == "success" else EventStatus.FAILED,
                metrics={
                    "operation": "classify",  # Добавляем sub-type операции
                    "category": unit_category,
                    "is_mixed": is_mixed,
                    "file_count": file_count,
                },
                duration_ms=duration_ms
            )
        except ImportError as e:
            logger.debug(f"PipelineTracker events not available: {e}")
        except Exception as e:
            logger.warning(f"Failed to record classification event for {unit_id}: {e}")

    def set_local_base_dir(self, base_dir: Path) -> None:
        """
        Устанавливает базовую директорию для этого экземпляра.

        Args:
            base_dir: Базовая директория для обработки
        """
        self._local_base_dir = base_dir

    def _get_effective_base_dir(self) -> Optional[Path]:
        """Возвращает эффективную базовую директорию (локальная > классовая > None)."""
        return self._local_base_dir or self._override_base_dir

    def _handle_empty_unit(
        self,
        unit_path: Path,
        unit_id: str,
        cycle: int,
        protocol_date: Optional[str],
        protocol_id: Optional[str],
        dry_run: bool,
        copy_mode: bool,
    ) -> Dict[str, Any]:
        """
        Обрабатывает пустой UNIT (без файлов).

        Пустые UNIT перемещаются в Exceptions/Empty.

        Args:
            unit_path: Путь к директории UNIT
            unit_id: Идентификатор UNIT
            cycle: Номер цикла
            protocol_date: Дата протокола
            protocol_id: ID протокола
            dry_run: Режим dry-run
            copy_mode: Режим копирования

        Returns:
            Результат классификации пустого UNIT
        """
        target_base_dir = self._get_target_directory_base("empty", cycle, protocol_date)
        target_dir_base = target_base_dir / "Empty"

        # Загружаем manifest если существует
        manifest = None
        manifest_path = unit_path / "manifest.json"
        if manifest_path.exists():
            try:
                manifest = load_manifest(unit_path)
                if not protocol_date:
                    protocol_date = manifest.get("protocol_date")
                if not protocol_id:
                    protocol_id = manifest.get("protocol_id")
            except Exception as e:
                logger.warning(f"Failed to load manifest for {unit_id}: {e}")

        # Создаем manifest если его нет
        if not manifest:
            manifest = create_unit_manifest_if_needed(
                unit_path=unit_path,
                unit_id=unit_id,
                protocol_id=protocol_id,
                protocol_date=protocol_date,
                files=[],
                cycle=cycle,
            )

        # Перемещаем пустой UNIT в Empty
        if not dry_run:
            target_dir = move_unit_to_target(
                unit_dir=unit_path,
                target_base_dir=target_dir_base,
                extension=None,
                dry_run=dry_run,
                copy_mode=copy_mode,
            )

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
                    "type": "classify",
                    "category": "empty",
                    "is_mixed": False,
                    "file_count": 0,
                    "reason": "empty_unit",
                },
                final_cluster="Exceptions/Direct" if cycle == 1 else f"Exceptions/Processing_{cycle}",
                final_reason="Empty unit with no files",
            )

            self.audit_logger.log_event(
                unit_id=unit_id,
                event_type="operation",
                operation="classify",
                details={
                    "cycle": cycle,
                    "category": "empty",
                    "is_mixed": False,
                    "file_count": 0,
                    "reason": "empty_unit",
                    "target_directory": str(target_dir),
                    "final_cluster": "Exceptions/Direct" if cycle == 1 else f"Exceptions/Processing_{cycle}",
                    "final_reason": "Empty unit with no files",
                },
                state_before="RAW",
                state_after=new_state.value,
                unit_path=target_dir,
            )
        else:
            target_dir = target_dir_base / unit_path.name

        return {
            "category": "empty",
            "unit_category": "empty",
            "is_mixed": False,
            "file_classifications": [],
            "target_directory": str(target_dir_base),
            "moved_to": str(target_dir),
            "error": "No files found in UNIT",
        }

    def _classify_files(
        self,
        files: List[Path],
        parallel: bool = True,
    ) -> Tuple[List[Dict], List[Dict], List[str], List[Dict]]:
        """
        Классифицирует файлы UNIT с поддержкой параллельной обработки.

        Оптимизация: detect_file_type() выполняется параллельно для всех файлов,
        что даёт 4-8x ускорение на многоядерных системах.

        Args:
            files: Список файлов для классификации
            parallel: Использовать параллельную обработку (по умолчанию True)

        Returns:
            Кортеж (file_classifications, classifications_by_file, categories, manifest_files)
        """
        file_classifications = []
        categories = []
        classifications_by_file = []
        manifest_files = []

        if not files:
            return file_classifications, classifications_by_file, categories, manifest_files

        # Проверяем глобальную конфигурацию параллелизма
        config = get_parallel_config()
        use_parallel = parallel and config.enabled and len(files) > 2

        if use_parallel:
            # Параллельное определение типов файлов (I/O-bound операция)
            detections = parallel_map_threads(
                detect_file_type,
                files,
                operation_type="classifier",
                desc="File type detection",
            )
        else:
            # Последовательная обработка для малого количества файлов
            detections = [detect_file_type(f) for f in files]

        # Классификация на основе полученных detections (CPU-light операция)
        for file_path, detection in zip(files, detections):
            classification = self._classify_file_with_detection(file_path, detection)
            file_classifications.append({
                "file_path": str(file_path),
                "classification": classification,
            })
            classifications_by_file.append(classification)
            categories.append(classification["category"])

            manifest_files.append({
                "original_name": file_path.name,
                "current_name": file_path.name,
                "mime_type": classification.get("mime_type", detection.get("mime_type", "")),
                "detected_type": classification.get("detected_type", "unknown"),
                "needs_ocr": detection.get("needs_ocr", False),
                "transformations": [],
            })

        return file_classifications, classifications_by_file, categories, manifest_files

    def _classify_file_with_detection(
        self,
        file_path: Path,
        detection: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Классифицирует файл на основе уже полученного detection.

        Это внутренний метод, оптимизированный для использования с параллельной
        обработкой, где detect_file_type() уже вызван.

        Args:
            file_path: Путь к файлу
            detection: Результат detect_file_type()

        Returns:
            Словарь с классификацией
        """
        extension = file_path.suffix.lower()

        classification = {
            "category": "unknown",
            "detected_type": detection.get("detected_type", "unknown"),
            "mime_type": detection.get("mime_type", ""),
            "original_extension": extension,
            "needs_conversion": False,
            "needs_extraction": False,
            "needs_normalization": False,
            "extension_matches_content": detection.get("extension_matches_content", True),
            "correct_extension": detection.get("correct_extension"),
        }

        # Проверка на malformed расширения (файлы без точки в имени)
        # Например: 26VP0630__vipis_уторgdocx (без точки перед docx)
        if not extension or ('.' not in file_path.name and file_path.name != file_path.stem):
            detected_type = detection.get("detected_type")
            # Если есть определенный тип файла (MIME/Signature), направляем на нормализацию
            if detected_type and detected_type in ["docx", "doc", "pdf", "xlsx", "xls", "pptx", "ppt", "html", "xml", "txt"]:
                classification["category"] = "normalize"
                classification["needs_normalization"] = True
                classification["correct_extension"] = f".{detected_type}"
                return classification
            # Иначе считаем как direct если MIME уверенный
            mime_conf = detection.get("mime_confidence", 0)
            if mime_conf >= 0.8:
                classification["category"] = "direct"
                return classification

        # Проверка на подписи
        if extension in self.SIGNATURE_EXTENSIONS:
            classification["category"] = "special"
            return classification

        # Проверка на неподдерживаемые форматы
        if extension in self.UNSUPPORTED_EXTENSIONS:
            classification["category"] = "special"
            return classification

        # Проверка на архивы
        archive_extensions = {".zip", ".rar", ".7z"}
        archive_types = ["zip_archive", "rar_archive", "7z_archive"]

        if (extension in archive_extensions or
            detection.get("is_archive") or
            detection.get("detected_type") in archive_types):
            classification["category"] = "extract"
            classification["needs_extraction"] = True
            return classification

        detected_type = detection.get("detected_type")
        mime_type = detection.get("mime_type", "").lower()

        # ИСПРАВЛЕНИЕ: Проверка HTML-контента в файлах .doc/.xls/.ppt
        # Многие файлы с расширением .doc на самом деле являются HTML (экспорт из веб-систем)
        # Такие файлы должны быть перенаправлены на нормализацию расширения
        html_indicators = (
            detected_type in ["html", "xml", "text"] or
            "html" in mime_type or
            "xml" in mime_type or
            mime_type == "text/plain"
        )

        if extension in [".doc", ".xls", ".ppt"] and html_indicators:
            # Файл с Office-расширением, но содержимое HTML/XML/Text
            # Перенаправляем на нормализацию расширения
            classification["category"] = "normalize"
            classification["needs_normalization"] = True
            if "html" in mime_type or detected_type == "html":
                classification["correct_extension"] = ".html"
            elif "xml" in mime_type or detected_type == "xml":
                classification["correct_extension"] = ".xml"
            else:
                classification["correct_extension"] = ".txt"
            classification["reason"] = f"Office extension {extension} with {detected_type} content"
            logger.debug(
                f"HTML/.doc fix: {file_path.name} - {extension} contains {detected_type} (MIME: {mime_type})"
            )
            return classification

        # Проверка старых Office форматов (стандартная логика)
        if extension in [".doc", ".xls", ".ppt", ".rtf"]:
            if detected_type not in ["html", "xml", "text"]:
                if detected_type in self.CONVERTIBLE_TYPES or detection.get("requires_conversion", False):
                    classification["category"] = "convert"
                    classification["needs_conversion"] = True
                    return classification

        if detected_type in self.CONVERTIBLE_TYPES:
            classification["category"] = "convert"
            classification["needs_conversion"] = True
            return classification

        decision_classification = detection.get("classification")

        if decision_classification == "normalize":
            if extension in [".doc", ".xls", ".ppt", ".rtf"]:
                if detected_type in ["html", "xml"]:
                    classification["category"] = "normalize"
                    classification["needs_normalization"] = True
                    classification["correct_extension"] = detection.get("correct_extension")
                    return classification
                classification["category"] = "convert"
                classification["needs_conversion"] = True
                return classification

            classification["category"] = "normalize"
            classification["needs_normalization"] = True
            classification["correct_extension"] = detection.get("correct_extension")
            return classification

        if decision_classification == "ambiguous":
            # Проверяем, не нужно ли нормализовать расширение
            correct_ext = detection.get("correct_extension")
            if correct_ext:
                # Есть правильное расширение - направляем в Normalize
                classification["category"] = "normalize"
                classification["needs_normalization"] = True
                classification["correct_extension"] = correct_ext
                classification["ambiguous_reason"] = detection.get("reason", "")
            else:
                # Действительно неоднозначный случай
                classification["category"] = "special"
                classification["scenario"] = "ambiguous"
                classification["ambiguous_reason"] = detection.get("reason", "")
            return classification

        if decision_classification == "unknown":
            classification["category"] = "unknown"
            return classification

        if decision_classification == "direct":
            classification["category"] = "direct"
            return classification

        if detected_type and detected_type != "unknown":
            if detected_type in ["pdf", "docx", "xlsx", "pptx"]:
                classification["category"] = "direct"
            else:
                classification["category"] = "unknown"
        else:
            classification["category"] = "unknown"

        return classification

    def _determine_unit_category(
        self,
        categories: List[str],
        classifications_by_file: List[Dict],
    ) -> tuple[str, bool, Dict[str, int]]:
        """
        Определяет категорию UNIT и mixed статус.

        Args:
            categories: Список категорий файлов
            classifications_by_file: Классификации по файлам

        Returns:
            Кортеж (unit_category, is_mixed, category_counts)
        """
        category_counts = Counter(categories)
        unique_categories = set(categories)

        # Проверяем mixed по категориям обработки
        is_mixed_by_category = len(unique_categories) > 1

        # Проверяем mixed по типам файлов
        detected_types = [fc.get("detected_type", "unknown") for fc in classifications_by_file]
        unique_types = set(detected_types)
        is_mixed_by_type = len(unique_types) > 1

        is_mixed = is_mixed_by_category or is_mixed_by_type

        if is_mixed:
            unit_category = "mixed"
        elif categories:
            unit_category = categories[0]
        else:
            unit_category = "unknown"

        return unit_category, is_mixed, dict(category_counts)

    def _get_extension_for_sorting(
        self,
        classifications_by_file: List[Dict],
        files: List[Path],
        unit_category: str,
        unit_path: Path,
    ) -> Optional[str]:
        """
        Определяет расширение для сортировки UNIT.

        Args:
            classifications_by_file: Классификации файлов
            files: Список файлов
            unit_category: Категория UNIT
            unit_path: Путь к UNIT

        Returns:
            Расширение для сортировки или None
        """
        extension = None
        if classifications_by_file and files:
            first_classification = classifications_by_file[0]
            first_file = files[0]
            original_ext = first_file.suffix.lower()
            extension = get_extension_subdirectory(
                category=unit_category,
                classification=first_classification,
                original_extension=original_ext,
            )
        if not extension and files:
            extension = files[0].suffix.lower().lstrip(".")
        if not extension:
            extension = determine_unit_extension(unit_path)
        return extension

    def _handle_direct_unit_late_cycles(
        self,
        unit_path: Path,
        unit_id: str,
        cycle: int,
        protocol_date: Optional[str],
        extension: Optional[str],
        manifest: Optional[Dict],
        classifications_by_file: List[Dict],
        files: List[Path],
        current_route: str,
        dry_run: bool,
        copy_mode: bool,
    ) -> Dict[str, Any]:
        """
        Обрабатывает direct UNIT в циклах 2-3.

        Direct файлы всех циклов идут в Merge/Direct/.

        Args:
            unit_path: Путь к UNIT
            unit_id: ID UNIT
            cycle: Номер цикла
            protocol_date: Дата протокола
            extension: Расширение для сортировки
            manifest: Манифест (если есть)
            classifications_by_file: Классификации файлов
            files: Список файлов
            current_route: Текущий route
            dry_run: Режим dry-run
            copy_mode: Режим копирования

        Returns:
            Результат классификации
        """
        # ИСПРАВЛЕНО: Передаём base_dir чтобы использовать правильные пути
        effective_base_dir = self._get_effective_base_dir()
        data_paths = get_data_paths(protocol_date, base_dir=effective_base_dir)
        target_base_dir = data_paths["merge"] / "Direct"

        target_dir = move_unit_to_target(
            unit_dir=unit_path,
            target_base_dir=target_base_dir,
            extension=extension,
            dry_run=dry_run,
            copy_mode=copy_mode,
        )

        if not dry_run:
            self._update_manifest_route(target_dir, current_route)
            new_state = UnitState.MERGED_PROCESSED
            update_unit_state(
                unit_path=target_dir,
                new_state=new_state,
                cycle=cycle,
                operation={
                    "type": "classify",
                    "status": "success",
                    "category": "direct",
                    "file_count": len(files),
                },
            )
        else:
            new_state = UnitState.MERGED_PROCESSED

        self.audit_logger.log_event(
            unit_id=unit_id,
            event_type="operation",
            operation="classify",
            details={
                "cycle": cycle,
                "category": "direct",
                "is_mixed": False,
                "file_count": len(files),
                "target_directory": str(target_dir),
            },
            state_before=manifest.get("state_machine", {}).get("current_state") if manifest else "CLASSIFIED_2",
            state_after=new_state.value,
            unit_path=target_dir,
        )

        return {
            "category": "direct",
            "unit_category": "direct",
            "is_mixed": False,
            "file_classifications": classifications_by_file,
            "target_directory": str(target_dir),
            "moved_to": str(target_dir),
        }

    def _create_or_update_manifest_for_unit(
        self,
        unit_path: Path,
        unit_id: str,
        protocol_id: Optional[str],
        protocol_date: Optional[str],
        manifest_files: List[Dict],
        cycle: int,
        manifest: Optional[Dict],
        current_route: str,
        dry_run: bool,
    ) -> Optional[Dict]:
        """
        Создает или обновляет манифест для UNIT.

        Args:
            unit_path: Путь к UNIT
            unit_id: ID UNIT
            protocol_id: ID протокола
            protocol_date: Дата протокола
            manifest_files: Файлы для манифеста
            cycle: Номер цикла
            manifest: Существующий манифест (или None)
            current_route: Текущий route
            dry_run: Режим dry-run

        Returns:
            Обновленный или созданный манифест
        """
        if not manifest:
            return create_unit_manifest_if_needed(
                unit_path=unit_path,
                unit_id=unit_id,
                protocol_id=protocol_id,
                protocol_date=protocol_date,
                files=manifest_files,
                cycle=cycle,
            )

        # Обогащаем существующий манифест
        manifest["files_metadata"] = {
            f.get("original_name", ""): {
                "detected_type": f.get("detected_type", "unknown"),
                "needs_ocr": f.get("needs_ocr", False),
                "mime_type": f.get("mime_detected", f.get("mime_type", "unknown")),
                "pages_or_parts": f.get("pages_or_parts", 1),
            }
            for f in manifest_files
        }

        if "processing" not in manifest:
            manifest["processing"] = {}
        manifest["processing"]["route"] = current_route

        if not dry_run:
            save_manifest(unit_path, manifest)

        return manifest

    def _log_classification_result(
        self,
        unit_id: str,
        cycle: int,
        unit_category: str,
        is_mixed: bool,
        files: List[Path],
        category_counts: Dict[str, int],
        extension: Optional[str],
        target_dir: Path,
        manifest: Optional[Dict],
        new_state: UnitState,
    ) -> None:
        """
        Логирует результат классификации.

        Args:
            unit_id: ID UNIT
            cycle: Номер цикла
            unit_category: Категория UNIT
            is_mixed: Mixed статус
            files: Список файлов
            category_counts: Подсчет категорий
            extension: Расширение
            target_dir: Целевая директория
            manifest: Манифест
            new_state: Новое состояние
        """
        self.audit_logger.log_event(
            unit_id=unit_id,
            event_type="operation",
            operation="classify",
            details={
                "cycle": cycle,
                "category": unit_category,
                "is_mixed": is_mixed,
                "file_count": len(files),
                "category_distribution": category_counts,
                "extension": extension,
                "target_directory": str(target_dir),
            },
            state_before=manifest.get("state_machine", {}).get("current_state") if manifest else "RAW",
            state_after=new_state.value,
            unit_path=target_dir,
        )

    def _update_manifest_route(self, target_dir: Path, route: str) -> None:
        """Обновляет route в manifest целевой директории."""
        try:
            manifest_path = target_dir / "manifest.json"
            if manifest_path.exists():
                manifest = load_manifest(target_dir)
                current_route = manifest.get("processing", {}).get("route")
                
                # Обновляем только если route изменился или отсутствует
                if current_route != route:
                    if "processing" not in manifest:
                        manifest["processing"] = {}
                    manifest["processing"]["route"] = route
                    save_manifest(target_dir, manifest)
                    logger.debug(f"Updated route to '{route}' for unit in {target_dir}")
        except Exception as e:
            logger.warning(f"Failed to update manifest route in {target_dir}: {e}")

    def classify_unit(
        self,
        unit_path: Path,
        cycle: int,
        protocol_date: Optional[str] = None,
        protocol_id: Optional[str] = None,
        dry_run: bool = False,
        copy_mode: bool = False,
    ) -> Dict[str, Any]:
        """
        Классифицирует UNIT, создает manifest, перемещает UNIT в целевую директорию и обновляет state.

        NOTE: Ready2Docling - это отдельный этап, запускаемый через merge2docling.
              Direct units из ВСЕХ циклов идут в единую директорию Merge/Direct/.

        Args:
            unit_path: Путь к директории UNIT
            cycle: Номер цикла (1, 2, 3)
            protocol_date: Дата протокола (опционально)
            protocol_id: ID протокола (опционально)
            dry_run: Если True, только показывает что будет сделано
            copy_mode: Если True, копирует вместо перемещения (сохраняет исходные файлы)

        Returns:
            Словарь с результатами классификации:
            - category: категория (direct, convert, extract, normalize, special, mixed)
            - unit_category: категория UNIT
            - is_mixed: является ли UNIT mixed
            - file_classifications: классификация каждого файла
            - target_directory: целевая директория для UNIT
            - moved_to: путь к новой директории UNIT (после перемещения)
        """
        import time
        start_time = time.time()

        unit_id = unit_path.name

        # Автоматически включаем copy_mode для units из Input директории
        # (для периода тестирования, чтобы не удалять исходные файлы)
        if not copy_mode:
            try:
                # Проверяем, находится ли unit_path в Input директории
                unit_path_real = unit_path.resolve()
                
                # Получаем все возможные пути к Input директории
                is_in_input = False
                
                # Проверяем различные варианты путей Input
                from datetime import datetime
                current_date = datetime.now().strftime("%Y-%m-%d")
                
                # Пытаемся извлечь дату из пути unit_path
                unit_parts = unit_path.parts
                date_part = None
                for part in unit_parts:
                    if len(part) == 10 and part[4] == '-' and part[7] == '-':
                        try:
                            datetime.strptime(part, "%Y-%m-%d")
                            date_part = part
                            break
                        except ValueError:
                            continue
                
                # Список путей для проверки (только относительные части)
                check_patterns = []

                # Получаем эффективный base_dir
                effective_base_dir = self._get_effective_base_dir()

                if effective_base_dir is not None:
                    # Если base_dir установлен, используем его напрямую
                    check_patterns.append(str(effective_base_dir / "Input"))
                else:
                    # Стандартный путь Input (относительная часть)
                    input_dir_path = Path(INPUT_DIR)
                    check_patterns.append(str(input_dir_path))

                    # Пути с датами (относительные части) - только если base_dir не установлен
                    check_dates = [current_date]
                    if date_part:
                        check_dates.append(date_part)

                    for check_date in check_dates:
                        try:
                            # ИСПРАВЛЕНО: Передаём base_dir чтобы использовать правильные пути
                            data_paths = get_data_paths(check_date, base_dir=effective_base_dir)
                            dated_input_dir = data_paths["input"]
                            check_patterns.append(str(dated_input_dir))
                        except (KeyError, ValueError, TypeError):
                            pass  # Игнорируем ошибки при получении путей для невалидных дат
                
                # Проверяем каждый путь (проверяем наличие паттернов в пути unit)
                unit_path_str = str(unit_path_real)
                for pattern in check_patterns:
                    if pattern in unit_path_str:
                        is_in_input = True
                        break
                
                # Дополнительная проверка: ищем "Input" в пути с датой
                if not is_in_input:
                    # Ищем паттерн вида "/YYYY-MM-DD/Input/"
                    import re
                    date_input_pattern = r"/\d{4}-\d{2}-\d{2}/Input/"
                    if re.search(date_input_pattern, unit_path_str):
                        is_in_input = True
                
                if is_in_input:
                    copy_mode = True
                    logger.debug(f"Auto-enabling copy_mode for unit from Input: {unit_id}")
            except Exception as e:
                logger.warning(f"Failed to check if unit is in Input directory: {e}")
                # Игнорируем ошибки, оставляем copy_mode как есть

        # Получаем файлы UNIT
        files = get_unit_files(unit_path)
        if not files:
            return self._handle_empty_unit(
                unit_path=unit_path,
                unit_id=unit_id,
                cycle=cycle,
                protocol_date=protocol_date,
                protocol_id=protocol_id,
                dry_run=dry_run,
                copy_mode=copy_mode,
            )

        # Загружаем manifest если существует
        manifest = None
        manifest_path = unit_path / "manifest.json"
        if manifest_path.exists():
            try:
                manifest = load_manifest(unit_path)
                # Используем protocol_date и protocol_id из manifest если они не предоставлены
                if not protocol_date:
                    protocol_date = manifest.get("protocol_date")
                if not protocol_id:
                    protocol_id = manifest.get("protocol_id")
            except Exception as e:
                logger.warning(f"Failed to load manifest for {unit_id}: {e}")

        # Классифицируем файлы
        file_classifications, classifications_by_file, categories, manifest_files = self._classify_files(files)

        # Определяем категорию UNIT
        unit_category, is_mixed, category_counts = self._determine_unit_category(
            categories, classifications_by_file
        )

        # ВАЖНО: Сохраняем is_mixed из существующего manifest при re-classification (Cycle 2+)
        # Это предотвращает потерю Mixed статуса при повторной классификации
        if manifest and cycle > 1:
            existing_is_mixed = get_is_mixed(manifest)
            if existing_is_mixed:
                is_mixed = True
                unit_category = "mixed"

        # Предварительно вычисляем route для обновления старых манифестов
        current_route = _determine_route_from_files(manifest_files)

        # Определяем расширение для сортировки
        extension = self._get_extension_for_sorting(
            classifications_by_file, files, unit_category, unit_path
        )

        # Определяем целевую директорию на основе категории
        target_base_dir = self._get_target_directory_base(unit_category, cycle, protocol_date)
        
        # Обрабатываем случай direct в циклах 2-3
        if unit_category == "direct" and cycle > 1:
            return self._handle_direct_unit_late_cycles(
                unit_path=unit_path,
                unit_id=unit_id,
                cycle=cycle,
                protocol_date=protocol_date,
                extension=extension,
                manifest=manifest,
                classifications_by_file=classifications_by_file,
                files=files,
                current_route=current_route,
                dry_run=dry_run,
                copy_mode=copy_mode,
            )

        # Создаем или обновляем manifest
        manifest = self._create_or_update_manifest_for_unit(
            unit_path=unit_path,
            unit_id=unit_id,
            protocol_id=protocol_id,
            protocol_date=protocol_date,
            manifest_files=manifest_files,
            cycle=cycle,
            manifest=manifest,
            current_route=current_route,
            dry_run=dry_run,
        )

        # Перемещаем UNIT в целевую директорию (с учетом расширения)
        if unit_category == "direct" and cycle == 1:
            # Direct файлы идут НАПРЯМУЮ в Merge/Direct/ (без Processing)
            target_dir = move_unit_to_target(
                unit_dir=unit_path,
                target_base_dir=target_base_dir,
                extension=extension,
                dry_run=dry_run,
                copy_mode=copy_mode,
            )
            # Обновляем state сразу на MERGED_DIRECT
            if not dry_run:
                self._update_manifest_route(target_dir, current_route)

                update_unit_state(
                    unit_path=target_dir,
                    new_state=UnitState.MERGED_DIRECT,
                    cycle=cycle,
                    operation={
                        "type": "classify",
                        "category": unit_category,
                        "direct_to_merge_0": True,
                        "file_count": len(files),
                    },
                )
                new_state = UnitState.MERGED_DIRECT
            else:
                new_state = UnitState.MERGED_DIRECT
        elif unit_category in ["special", "unknown"]:
            # Для special и unknown используем subcategory как поддиректорию
            if unit_category == "unknown":
                # Unknown файлы идут в Ambiguous
                subcategory = "Ambiguous"
            else:
                subcategory = "Special"
            
            # Проверяем, есть ли ambiguous файлы (для special)
            if unit_category != "unknown":
                # Проверяем, есть ли ambiguous файлы (по scenario или по classification из Decision Engine)
                has_ambiguous = any(
                    (fc.get("classification", {}).get("scenario") and 
                     "ambiguous" in str(fc.get("classification", {}).get("scenario", "")).lower()) or
                    (fc.get("classification", {}).get("category") == "special" and 
                     fc.get("classification", {}).get("scenario"))
                    for fc in file_classifications
                )
                
                # Если есть ambiguous файлы, идем в Ambiguous
                if has_ambiguous:
                    subcategory = "Ambiguous"
                elif unit_category == "special":
                    subcategory = "Special"  # Все special (не ambiguous) идут в Special
            target_dir = move_unit_to_target(
                unit_dir=unit_path,
                target_base_dir=target_base_dir / subcategory,
                extension=None,  # Exceptions не сортируются по расширениям
                dry_run=dry_run,
                copy_mode=copy_mode,
            )
            # Для exceptions (special, ambiguous, unknown, empty) используем EXCEPTION_N состояния
            exception_state_map = {
                1: UnitState.EXCEPTION_1,
                2: UnitState.EXCEPTION_2,
                3: UnitState.EXCEPTION_3,
            }
            new_state = exception_state_map.get(cycle, UnitState.EXCEPTION_1)
            
            # Проверяем текущее состояние перед обновлением
            manifest_path = target_dir / "manifest.json"
            if manifest_path.exists():
                try:
                    manifest = load_manifest(target_dir)
                    state_machine = UnitStateMachine(unit_id, manifest_path)
                    current_state = state_machine.get_current_state()
                    
                    # Если UNIT уже в нужном состоянии для exceptions, не обновляем
                    if current_state == new_state:
                        # UNIT уже в правильном состоянии для exceptions - не обновляем
                        should_update_state = False
                    else:
                        should_update_state = True
                except (json.JSONDecodeError, FileNotFoundError, KeyError, ValueError) as e:
                    # Если не удалось загрузить manifest или state machine, обновляем состояние
                    logger.debug(f"Could not load manifest for {unit_id}: {e}")
                    should_update_state = True
            else:
                # Нет manifest - обновляем состояние
                should_update_state = True
            
            # Обновляем state machine (если не dry_run и состояние изменилось)
            if not dry_run and should_update_state:
                self._update_manifest_route(target_dir, current_route)

                update_unit_state(
                    unit_path=target_dir,
                    new_state=new_state,
                    cycle=cycle,
                    operation={
                        "type": "classify",
                        "category": unit_category,
                        "is_mixed": is_mixed,
                        "file_count": len(files),
                    },
                )
        elif unit_category == "mixed":
            # Для mixed юнитов выбираем приоритетную категорию обработки
            # Приоритет: extract > convert > normalize > direct
            priority_order = ["extract", "convert", "normalize", "direct"]
            chosen_category = "direct"
            
            # Проверяем наличие категорий в файлах
            file_cats = {fc["category"] for fc in classifications_by_file}
            for cat in priority_order:
                if cat in file_cats:
                    chosen_category = cat
                    break
            
            # Определяем целевую базу для выбранной категории
            target_base_dir = self._get_target_directory_base(chosen_category, cycle, protocol_date)

            # Если это direct в циклах 2-3, обрабатываем отдельно (уже реализовано выше для unit_category == "direct")
            # Но для простоты в mixed мы просто направляем в соответствующую директорию

            # Mixed - это поддиректория, а не расширение файла
            # Создаем поддиректорию Mixed и используем её как target
            mixed_dir = target_base_dir / "Mixed"
            mixed_dir.mkdir(parents=True, exist_ok=True)

            target_dir = move_unit_to_target(
                unit_dir=unit_path,
                target_base_dir=mixed_dir,
                extension=None,  # Без дополнительной сортировки по расширению внутри Mixed
                dry_run=dry_run,
                copy_mode=copy_mode,
            )

            # Определяем новое состояние на основе выбранной категории
            if cycle == 1:
                if chosen_category == "direct":
                    new_state = UnitState.MERGED_DIRECT
                else:
                    new_state = UnitState.CLASSIFIED_1
            elif cycle == 2:
                # В цикле 2 mixed может идти либо в CLASSIFIED_2 (если требует еще обработки),
                # либо в MERGED_PROCESSED (если это финальная стадия)
                # По умолчанию - CLASSIFIED_2, если это convert/extract/normalize
                if chosen_category in ["convert", "extract", "normalize"]:
                    new_state = UnitState.CLASSIFIED_2
                else:
                    new_state = UnitState.MERGED_PROCESSED
            else:
                new_state = UnitState.MERGED_PROCESSED
                
            if not dry_run:
                self._update_manifest_route(target_dir, current_route)
                
                update_unit_state(
                    unit_path=target_dir,
                    new_state=new_state,
                    cycle=cycle,
                    operation={
                        "type": "classify",
                        "status": "success",
                        "category": "mixed",
                        "chosen_route_category": chosen_category,
                        "is_mixed": True,
                        "file_count": len(files),
                    },
                )
        else:
            # Для категорий (convert, extract, normalize) сортируем по расширению
            # ИСКЛЮЧЕНИЕ: normalize НЕ сортируем по расширению, т.к. normalize шаг
            # ожидает UNIT прямо в Processing_N/Normalize/, а не в Processing_N/Normalize/{ext}/
            extension_for_move = None if unit_category == "normalize" else extension
            target_dir = move_unit_to_target(
                unit_dir=unit_path,
                target_base_dir=target_base_dir,
                extension=extension_for_move,
                dry_run=dry_run,
                copy_mode=copy_mode,
            )
            # Определяем новое состояние на основе цикла и текущего состояния
            # Проверяем текущее состояние из manifest
            manifest_path = target_dir / "manifest.json"
            # UnitStateMachine уже импортирован на уровне модуля
            state_machine = UnitStateMachine(unit_id, manifest_path)
            current_state = state_machine.get_current_state()
            
            # Если UNIT уже в CLASSIFIED_2 и приходит из Merge (обработан), переводим в MERGED_PROCESSED
            # Если UNIT в CLASSIFIED_2 и требует дальнейшей обработки, переводим в PENDING_*
            if current_state == UnitState.CLASSIFIED_2:
                # UNIT уже обработан, проверяем категорию
                if unit_category == "direct":
                    # Готов к merge - переводим в MERGED_PROCESSED
                    new_state = UnitState.MERGED_PROCESSED
                elif unit_category in ["convert", "extract", "normalize"]:
                    # Требует дальнейшей обработки - переводим в PENDING_*
                    pending_map = {
                        "convert": UnitState.PENDING_CONVERT,
                        "extract": UnitState.PENDING_EXTRACT,
                        "normalize": UnitState.PENDING_NORMALIZE,
                    }
                    new_state = pending_map.get(unit_category, UnitState.MERGED_PROCESSED)
                else:
                    # Для mixed, unknown, special - переводим в MERGED_PROCESSED или EXCEPTION
                    new_state = UnitState.MERGED_PROCESSED
            elif current_state == UnitState.CLASSIFIED_3:
                # UNIT уже в CLASSIFIED_3 - переводим в MERGED_PROCESSED
                new_state = UnitState.MERGED_PROCESSED
            elif unit_category == "direct" and cycle > 1:
                # Direct категория в циклах 2-3 (из обработанных UNIT) - переводим в MERGED_PROCESSED
                new_state = UnitState.MERGED_PROCESSED
            elif cycle == 3 and current_state in [UnitState.CLASSIFIED_2, UnitState.MERGED_PROCESSED]:
                # Для цикла 3, если UNIT уже в CLASSIFIED_2 или MERGED_PROCESSED, переводим в MERGED_PROCESSED
                new_state = UnitState.MERGED_PROCESSED
            else:
                # Для других состояний используем стандартную логику
                new_state_map = {
                    1: UnitState.CLASSIFIED_1,
                    2: UnitState.CLASSIFIED_2,
                    3: UnitState.MERGED_PROCESSED,  # Для цикла 3 переходим сразу в MERGED_PROCESSED
                }
                new_state = new_state_map.get(cycle, UnitState.CLASSIFIED_1)
            
            # Обновляем state machine (если не dry_run)
            if not dry_run:
                self._update_manifest_route(target_dir, current_route)

                update_unit_state(
                    unit_path=target_dir,
                    new_state=new_state,
                    cycle=cycle,
                    operation={
                        "type": "classify",
                        "category": unit_category,
                        "is_mixed": is_mixed,
                        "file_count": len(files),
                    },
                )

        # Логируем классификацию
        self.audit_logger.log_event(
            unit_id=unit_id,
            event_type="operation",
            operation="classify",
            details={
                "cycle": cycle,
                "category": unit_category,
                "is_mixed": is_mixed,
                "file_count": len(files),
                "category_distribution": dict(category_counts),
                "extension": extension,
                "target_directory": str(target_dir),
            },
            state_before=manifest.get("state_machine", {}).get("current_state") if manifest else "RAW",
            state_after=new_state.value,
            unit_path=target_dir,
        )

        # Записываем UnitEvent для классификации
        duration_ms = int((time.time() - start_time) * 1000)
        registration_number = self._get_registration_number(unit_path)
        self._record_classification_event(
            unit_id=unit_id,
            registration_number=registration_number,
            unit_category=unit_category,
            is_mixed=is_mixed,
            file_count=len(files),
            duration_ms=duration_ms,
            status="success"
        )

        return {
            "category": unit_category,
            "unit_category": unit_category,
            "is_mixed": is_mixed,
            "file_classifications": file_classifications,
            "target_directory": str(target_base_dir),
            "moved_to": str(target_dir),
            "category_distribution": dict(category_counts),
            "extension": extension,
        }

    def _classify_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Классифицирует отдельный файл.

        Использует результат Decision Engine для определения категории.
        Для параллельной обработки используйте _classify_file_with_detection().

        Args:
            file_path: Путь к файлу

        Returns:
            Словарь с классификацией:
            - category: категория (direct, convert, extract, normalize, special)
            - detected_type: определенный тип файла
            - needs_conversion: требуется ли конвертация
            - needs_extraction: требуется ли разархивация
            - needs_normalization: требуется ли нормализация
            - correct_extension: правильное расширение (если нужна нормализация)
        """
        detection = detect_file_type(file_path)
        return self._classify_file_with_detection(file_path, detection)

    def _get_target_directory_base(
        self, category: str, cycle: int, protocol_date: Optional[str] = None
    ) -> Path:
        """
        Определяет базовую целевую директорию для UNIT на основе категории.

        Расширение будет добавлено позже через move_unit_to_target.
        NOTE: Ready2Docling - это отдельный этап (merge2docling).

        Args:
            category: Категория UNIT
            cycle: Номер цикла
            protocol_date: Дата протокола для организации по датам (опционально)

        Returns:
            Базовая целевая директория (без учета расширения)
        """
        # Получаем эффективную base_dir если она установлена
        base_dir = self._get_effective_base_dir()

        # Получаем пути с учётом base_dir
        if protocol_date:
            data_paths = get_data_paths(protocol_date, base_dir=base_dir)
        else:
            # Без даты используем base_dir напрямую
            data_paths = get_data_paths(date=None, base_dir=base_dir)

        # Определяем базовую директорию в зависимости от категории
        # СТРУКТУРА:
        # - Exceptions/Direct/ - для исключений до обработки (цикл 1)
        # - Exceptions/Processed_N/ - для исключений после обработки (цикл N)
        # - Merge/Direct/ - для ВСЕХ direct файлов готовых к Docling (все циклы)
        # - Merge/Processed_N/ - для обработанных units (Converted, Extracted, Normalized, Mixed)

        if category in ["special", "unknown", "empty"]:
            # Exceptions находится внутри директории с датой
            exceptions_base = data_paths["exceptions"]
            if cycle == 1:
                # Исключения до обработки идут в Exceptions/Direct/
                return exceptions_base / "Direct"
            else:
                # Исключения после обработки идут в Exceptions/Processed_N/
                return exceptions_base / f"Processing_{cycle}"
        elif category == "direct":
            merge_base = data_paths["merge"]
            # Direct файлы ВСЕГДА идут в Merge/Direct/ независимо от цикла
            # Это единственная директория Direct в ветке Merge (как в Exceptions)
            return merge_base / "Direct"
        else:
            # Processing категории (convert, extract, normalize)
            processing_base = data_paths["processing"]
            processing_paths = get_processing_paths(cycle, processing_base)

            category_mapping = {
                "convert": processing_paths["Convert"],
                "extract": processing_paths["Extract"],
                "normalize": processing_paths["Normalize"],
            }

            return category_mapping.get(category, processing_paths["Convert"])


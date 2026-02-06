"""
NameNormalizer - нормализация имен файлов.

Исправляет смещённые точки, убирает двойные расширения.
НЕ меняет тип файла, только имя.
"""
import re
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

from ...core.manifest import load_manifest, save_manifest, update_manifest_operation
from ...core.audit import get_audit_logger
from ...core.state_machine import UnitState
from ...core.unit_processor import (
    move_unit_to_target,
    update_unit_state,
    determine_unit_extension,
)
from ...core.config import PROCESSING_DIR

logger = logging.getLogger(__name__)


class NameNormalizer:
    """Нормализатор имен файлов."""

    def __init__(self):
        """Инициализирует NameNormalizer."""
        self.audit_logger = get_audit_logger()

    def normalize_names(
        self,
        unit_path: Path,
        cycle: int,
        protocol_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Нормализует имена всех файлов в UNIT, перемещает UNIT и обновляет state.

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

        for file_path in files:
            try:
                original_name = file_path.name
                normalized_name = self._normalize_filename(original_name)

                if normalized_name != original_name:
                    # Переименовываем файл
                    new_path = file_path.parent / normalized_name
                    if not dry_run:
                        file_path.rename(new_path)

                    normalized_files.append(
                        {
                            "original_name": original_name,
                            "normalized_name": normalized_name,
                            "original_path": str(file_path),
                            "new_path": str(new_path),
                        }
                    )

                    # Обновляем manifest
                    if manifest:
                        operation = {
                            "type": "normalize",
                            "subtype": "name",
                            "original_name": original_name,
                            "normalized_name": normalized_name,
                            "cycle": current_cycle,
                        }
                        manifest = update_manifest_operation(manifest, operation)
            except Exception as e:
                errors.append({"file": str(file_path), "error": str(e)})
                logger.error(f"Failed to normalize name for {file_path}: {e}")

        # Сохраняем обновленный manifest
        if manifest:
            save_manifest(unit_path, manifest)

        # После нормализации имени файлы остаются в той же директории Normalize
        # (требуется также нормализация расширения через ExtensionNormalizer)
        # Определяем расширение для сортировки
        extension = determine_unit_extension(unit_path)

        # Файлы остаются в той же директории (не перемещаем)
        # ExtensionNormalizer обработает их после этого
        target_dir = unit_path

        # Логируем операцию
        self.audit_logger.log_event(
            unit_id=unit_id,
            event_type="operation",
            operation="normalize",
            details={
                "subtype": "name",
                "cycle": current_cycle,
                "files_normalized": len(normalized_files),
                "extension": extension,
                "target_directory": str(target_dir),
                "errors": errors,
            },
            state_before=manifest.get("state_machine", {}).get("current_state") if manifest else None,
            state_after=manifest.get("state_machine", {}).get("current_state") if manifest else None,  # State не меняется
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

    def _normalize_filename(self, filename: str) -> str:
        """
        Нормализует имя файла.

        Исправления:
        - Удаление пробелов перед точкой расширения (file. doc → file.doc)
        - Удаление множественных пробелов (file  name → file name)
        - Удаление двойных расширений (file.doc.docx → file.docx)
        - Удаление точек в начале/конце имени
        - НОВОЕ: Исправление повреждённых имен с кириллицей перед расширением
        - НОВОЕ: Нормализация Unicode и удаление невалидных символов

        Args:
            filename: Исходное имя файла

        Returns:
            Нормализованное имя файла
        """
        import unicodedata

        original_filename = filename

        # 0. Нормализация Unicode (NFKC объединяет совместимые символы)
        try:
            filename = unicodedata.normalize('NFKC', filename)
        except Exception:
            pass  # Продолжаем с оригинальным именем

        # 1. ИСПРАВЛЕНИЕ ПОВРЕЖДЁННЫХ ИМЕН: Кириллица/мусор перед расширением
        # Паттерн: "2025 гpdf" → "2025.pdf", "документ докpdf" → "документ.pdf"
        # Ищем расширение, склеенное с мусором
        common_extensions = {
            "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
            "txt", "zip", "rar", "7z", "html", "htm", "xml", "rtf",
            "odt", "ods", "odp", "csv", "json",
        }

        # Паттерн для поиска расширения с мусором перед ним
        # Ищем: [не-ASCII или пробелы][расширение] в конце имени
        for ext in common_extensions:
            # Паттерн: любые символы (кириллица, пробелы) + расширение в конце
            # Примеры: "гpdf", " pdf", "кpdf", "  pdf"
            pattern = rf'([а-яА-ЯёЁ\s]+)({ext})$'
            match = re.search(pattern, filename, re.IGNORECASE)
            if match:
                # Нашли мусор перед расширением - исправляем
                prefix = filename[:match.start()]
                clean_ext = match.group(2).lower()
                # Убираем trailing пробелы из prefix
                prefix = prefix.rstrip()
                filename = f"{prefix}.{clean_ext}"
                logger.debug(f"Fixed corrupted extension: '{original_filename}' → '{filename}'")
                break

        # 2. Убираем пробелы перед точкой расширения
        # Паттерн: один или более пробелов перед точкой
        # Примеры: "file. doc" → "file.doc", "file  .doc" → "file.doc"
        filename = re.sub(r'\s+\.', '.', filename)

        # 3. Убираем пробелы после точки расширения (file. doc → file.doc)
        filename = re.sub(r'\.\s+', '.', filename)

        # 4. Убираем множественные пробелы (заменяем на один пробел)
        filename = re.sub(r'\s+', ' ', filename)

        # 5. Убираем двойные расширения
        # Например: file.doc.docx → file.docx
        parts = filename.rsplit(".", 2)
        if len(parts) == 3:
            name, ext1, ext2 = parts
            # Проверяем, является ли ext1 валидным расширением
            if ext1.lower() in common_extensions:
                # Убираем первое расширение
                filename = f"{name}.{ext2}"

        # 6. Убираем точки в начале/конце имени
        filename = filename.strip('.')

        # 7. Убираем trailing пробелы
        filename = filename.strip()

        # 8. Заменяем недопустимые символы для файловой системы
        # Windows/Linux/MacOS имеют разные ограничения, используем общий минимум
        invalid_chars = r'[<>:"/\\|?*\x00-\x1f]'
        filename = re.sub(invalid_chars, '_', filename)

        # 9. Убираем множественные подчёркивания
        filename = re.sub(r'_+', '_', filename)

        # 10. Проверяем, что имя не стало пустым
        if not filename or filename == '.':
            # Генерируем базовое имя из хэша оригинального
            import hashlib
            name_hash = hashlib.md5(original_filename.encode('utf-8', errors='ignore')).hexdigest()[:8]
            filename = f"normalized_{name_hash}"
            logger.warning(f"Filename became empty after normalization, using: {filename}")

        return filename

    def normalize_unit(
        self,
        unit_path: Path,
        cycle: int,
        protocol_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Alias для normalize_names для совместимости с другими engine-компонентами.
        """
        return self.normalize_names(unit_path, cycle, protocol_date, dry_run)


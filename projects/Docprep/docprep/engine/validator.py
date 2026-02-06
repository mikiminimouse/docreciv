"""
Validator - проверка целостности UNIT.

Валидация manifest, state machine переходов, соответствия файлов, checksums.
"""
import logging
from pathlib import Path
from typing import Dict, Any, List

from ..core.manifest import load_manifest
from ..core.state_machine import UnitStateMachine, validate_state_transition
from ..core.exceptions import ManifestError
from ..utils.file_ops import calculate_sha256
from ..utils.paths import get_unit_files

logger = logging.getLogger(__name__)


class Validator:
    """Валидатор целостности UNIT."""

    def validate_unit(self, unit_path: Path) -> Dict[str, Any]:
        """
        Валидирует целостность UNIT.

        Args:
            unit_path: Путь к директории UNIT

        Returns:
            Словарь с результатами валидации
        """
        unit_id = unit_path.name
        validation_results = {
            "unit_id": unit_id,
            "valid": True,
            "errors": [],
            "warnings": [],
        }

        # Проверка наличия manifest.json
        manifest_path = unit_path / "manifest.json"
        if not manifest_path.exists():
            validation_results["valid"] = False
            validation_results["errors"].append("manifest.json not found")
            return validation_results

        try:
            manifest = load_manifest(unit_path)
        except Exception as e:
            validation_results["valid"] = False
            validation_results["errors"].append(f"Failed to load manifest: {str(e)}")
            return validation_results

        # Валидация структуры manifest
        required_fields = ["schema_version", "unit_id", "state_machine", "processing"]
        for field in required_fields:
            if field not in manifest:
                validation_results["valid"] = False
                validation_results["errors"].append(f"Manifest missing required field: {field}")

        # Валидация state machine переходов
        if "state_machine" in manifest:
            state_trace = manifest["state_machine"].get("state_trace", [])
            if len(state_trace) > 1:
                for i in range(len(state_trace) - 1):
                    try:
                        current_state = _state_from_string(state_trace[i])
                        next_state = _state_from_string(state_trace[i + 1])
                        if current_state and next_state:
                            if not validate_state_transition(current_state, next_state):
                                validation_results["warnings"].append(
                                    f"Invalid transition: {state_trace[i]} -> {state_trace[i + 1]}"
                                )
                    except Exception:
                        pass

        # Проверка соответствия файлов manifest
        if "files" in manifest:
            manifest_files = {f.get("current_name") for f in manifest["files"]}
            actual_files = {
                f.name
                for f in unit_path.rglob("*")
                if f.is_file() and f.name not in ["manifest.json", "audit.log.jsonl"]
            }

            # Проверяем, что файлы из manifest существуют
            for file_info in manifest["files"]:
                file_name = file_info.get("current_name")
                if file_name and file_name not in actual_files:
                    validation_results["warnings"].append(
                        f"File from manifest not found: {file_name}"
                    )

        # Проверка структуры UNIT
        structure_errors = self._validate_unit_structure(unit_path)
        validation_results["errors"].extend(structure_errors)
        if structure_errors:
            validation_results["valid"] = False

        # Проверка checksums для всех файлов из manifest
        checksum_errors = self._validate_checksums(unit_path, manifest)
        validation_results["errors"].extend(checksum_errors)
        if checksum_errors:
            validation_results["valid"] = False

        return validation_results

    def _validate_unit_structure(self, unit_path: Path) -> List[str]:
        """
        Валидирует структуру UNIT.

        Проверяет:
        - Наличие директории files (опционально)
        - Правильность структуры директорий
        - Отсутствие лишних файлов в корне UNIT

        Args:
            unit_path: Путь к директории UNIT

        Returns:
            Список ошибок структуры
        """
        errors = []
        unit_id = unit_path.name

        # Проверяем, что это директория
        if not unit_path.is_dir():
            errors.append(f"Unit path is not a directory: {unit_path}")
            return errors

        # Проверяем наличие manifest.json
        manifest_path = unit_path / "manifest.json"
        if not manifest_path.exists():
            errors.append("manifest.json not found in unit root")

        # Проверяем наличие файлов (должен быть хотя бы один файл)
        unit_files = get_unit_files(unit_path)
        if not unit_files:
            errors.append("No files found in unit")

        # Проверяем, что нет лишних директорий в корне (кроме files/)
        allowed_dirs = {"files"}
        for item in unit_path.iterdir():
            if item.is_dir() and item.name not in allowed_dirs:
                errors.append(f"Unexpected directory in unit root: {item.name}")

        return errors

    def _validate_checksums(self, unit_path: Path, manifest: Dict[str, Any]) -> List[str]:
        """
        Валидирует checksums файлов из manifest.

        Проверяет SHA256 для каждого файла, если указан в manifest.

        Args:
            unit_path: Путь к директории UNIT
            manifest: Загруженный manifest

        Returns:
            Список ошибок checksum
        """
        errors = []
        unit_files = get_unit_files(unit_path)

        if "files" not in manifest:
            return errors

        # Создаем маппинг имя файла -> путь
        file_path_map = {f.name: f for f in unit_files}

        for file_info in manifest["files"]:
            file_name = file_info.get("current_name") or file_info.get("original_name")
            if not file_name:
                continue

            # Находим файл
            file_path = file_path_map.get(file_name)
            if not file_path:
                # Файл не найден - это уже проверяется в основной валидации
                continue

            # Проверяем checksum если указан
            manifest_sha256 = file_info.get("sha256")
            if manifest_sha256:
                try:
                    actual_sha256 = calculate_sha256(file_path)
                    if actual_sha256 != manifest_sha256:
                        errors.append(
                            f"Checksum mismatch for {file_name}: "
                            f"manifest={manifest_sha256[:16]}..., "
                            f"actual={actual_sha256[:16]}..."
                        )
                except Exception as e:
                    errors.append(f"Failed to calculate checksum for {file_name}: {str(e)}")
                    logger.error(f"Checksum calculation error for {file_path}: {e}")

        return errors


# Вспомогательная функция для парсинга состояния из строки
def _state_from_string(state_str: str):
    """Вспомогательная функция для парсинга состояния из строки."""
    from ..core.state_machine import UnitState

    try:
        return UnitState(state_str)
    except ValueError:
        return None


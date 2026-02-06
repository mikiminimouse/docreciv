"""
TraceManager - утилита для сквозного трейсинга через все компоненты.

Обеспечивает единый интерфейс для получения и обновления trace информации
на всех этапах обработки: Docreciv → Docprep → doclingproc → LLM_qaenrich
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class TraceManager:
    """
    Менеджер trace информации через все компоненты системы.

    trace system использует registrationNumber как PRIMARY TRACE ID
    для сквозного трейсинга от исходного протокола до финальной обработки.
    """

    # Приоритет источников trace ID (от высшего к низшему)
    TRACE_ID_SOURCES = [
        "manifest.trace.primary_id",      # manifest.json trace секция
        "manifest.registration_number",   # manifest.json registration_number
        "unit_meta.registrationNumber",   # unit.meta.json от Docreciv
        "unit_meta.trace_id",             # unit.meta.json trace_id (синоним)
        "unit_id",                        # Fallback на unit_id
    ]

    @staticmethod
    def get_primary_trace_id(unit_dir: Path) -> str:
        """
        Получает primary trace ID (registrationNumber) из UNIT.

        Priority:
        1. manifest["trace"]["primary_id"]
        2. manifest["registration_number"]
        3. unit.meta.json["registrationNumber"]
        4. unit.meta.json["trace_id"]
        5. unit_id (fallback)

        Args:
            unit_dir: Path к директории UNIT

        Returns:
            Primary trace ID (registrationNumber или unit_id как fallback)
        """
        # 1. Проверяем manifest.json
        manifest_file = unit_dir / "manifest.json"
        if manifest_file.exists():
            try:
                with open(manifest_file, 'r', encoding='utf-8') as f:
                    manifest = json.load(f)
                    # Проверяем trace.primary_id
                    trace = manifest.get("trace", {})
                    primary_id = trace.get("primary_id")
                    if primary_id and primary_id != unit_dir.name:
                        return primary_id
                    # Проверяем registration_number
                    reg_num = manifest.get("registration_number")
                    if reg_num:
                        return reg_num
            except (json.JSONDecodeError, IOError) as e:
                logger.debug(f"Failed to read manifest.json: {e}")

        # 2. Проверяем unit.meta.json (от Docreciv)
        meta_file = unit_dir / "unit.meta.json"
        if meta_file.exists():
            try:
                with open(meta_file, 'r', encoding='utf-8') as f:
                    meta = json.load(f)
                    # Пробуем registrationNumber (от Docreciv)
                    reg_num = meta.get("registrationNumber")
                    if reg_num:
                        return reg_num
                    # Пробуем trace_id (синоним)
                    trace_id = meta.get("trace_id")
                    if trace_id:
                        return trace_id
            except (json.JSONDecodeError, IOError) as e:
                logger.debug(f"Failed to read unit.meta.json: {e}")

        # 3. Fallback на unit_id
        return unit_dir.name

    @staticmethod
    def get_trace_info(unit_dir: Path) -> Dict[str, Any]:
        """
        Получает полную trace информацию из UNIT.

        Собирает информацию из manifest.json и unit.meta.json.

        Args:
            unit_dir: Path к директории UNIT

        Returns:
            Словарь с trace информацией:
            - primary_id: основной trace ID
            - source: источник primary_id
            - registration_number: registrationNumber если есть
            - unit_id: unit_id
            - components: обработанные компоненты
            - history: история событий
        """
        trace_info = {
            "unit_id": unit_dir.name,
            "primary_id": unit_dir.name,  # default fallback
            "source": "unit_id",
            "registration_number": None,
            "components": {},
            "history": [],
        }

        # Читаем manifest.json
        manifest_file = unit_dir / "manifest.json"
        if manifest_file.exists():
            try:
                with open(manifest_file, 'r', encoding='utf-8') as f:
                    manifest = json.load(f)

                    # Проверяем trace секцию
                    trace = manifest.get("trace", {})
                    if trace.get("primary_id"):
                        trace_info["primary_id"] = trace["primary_id"]
                        trace_info["source"] = "manifest.trace.primary_id"

                    # Проверяем registration_number
                    reg_num = manifest.get("registration_number")
                    if reg_num:
                        trace_info["registration_number"] = reg_num
                        if not trace.get("primary_id"):
                            trace_info["primary_id"] = reg_num
                            trace_info["source"] = "manifest.registration_number"

                    # Собираем компоненты из trace
                    for component, data in trace.items():
                        if component != "primary_id":
                            trace_info["components"][component] = data

                    # История из manifest
                    trace_info["history"].extend(manifest.get("history", []))

            except (json.JSONDecodeError, IOError) as e:
                logger.debug(f"Failed to read manifest.json: {e}")

        # Читаем unit.meta.json (от Docreciv)
        meta_file = unit_dir / "unit.meta.json"
        if meta_file.exists():
            try:
                with open(meta_file, 'r', encoding='utf-8') as f:
                    meta = json.load(f)

                    reg_num = meta.get("registrationNumber") or meta.get("trace_id")
                    if reg_num:
                        trace_info["registration_number"] = reg_num
                        # Если manifest не дал primary_id, используем из meta
                        if trace_info["source"] == "unit_id":
                            trace_info["primary_id"] = reg_num
                            trace_info["source"] = "unit_meta.registrationNumber"

                    # Добавляем docreciv компонент
                    if "docreciv" not in trace_info["components"]:
                        trace_info["components"]["docreciv"] = {
                            "unit_id": meta.get("unit_id"),
                            "source_date": meta.get("source_date"),
                            "downloaded_at": meta.get("downloaded_at"),
                        }

            except (json.JSONDecodeError, IOError) as e:
                logger.debug(f"Failed to read unit.meta.json: {e}")

        return trace_info

    @staticmethod
    def update_trace(
        unit_dir: Path,
        component: str,
        event: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Обновляет trace информацию в manifest.json.

        Добавляет или обновляет component trace и добавляет событие в history.

        Args:
            unit_dir: Path к директории UNIT
            component: Имя компонента (docprep, doclingproc, llm_qaenrich)
            event: Событие (processed, classified, converted, etc.)
            metadata: Дополнительные метаданные

        Returns:
            True если обновление успешно
        """
        manifest_file = unit_dir / "manifest.json"
        if not manifest_file.exists():
            logger.warning(f"manifest.json not found for {unit_dir.name}")
            return False

        try:
            with open(manifest_file, 'r', encoding='utf-8') as f:
                manifest = json.load(f)

            # Обновляем или создаём trace секцию
            if "trace" not in manifest:
                manifest["trace"] = {}

            # Обновляем component trace
            manifest["trace"][component] = {
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "event": event,
                **(metadata or {})
            }

            # Добавляем в history
            if "history" not in manifest:
                manifest["history"] = []

            manifest["history"].append({
                "component": component,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "event": event,
            })

            # Обновляем updated_at
            manifest["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

            # Записываем с fsync для гарантии
            import os
            with open(manifest_file, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())

            logger.debug(f"Updated trace for {unit_dir.name}: {component}.{event}")
            return True

        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to update trace for {unit_dir.name}: {e}")
            return False

    @staticmethod
    def format_trace_summary(trace_info: Dict[str, Any]) -> str:
        """
        Форматирует trace информацию для логирования.

        Args:
            trace_info: Словарь с trace информацией от get_trace_info()

        Returns:
            Отформатированная строка для логирования
        """
        lines = [
            f"Trace Info: {trace_info['primary_id']}",
            f"  Source: {trace_info['source']}",
            f"  Unit ID: {trace_info['unit_id']}",
        ]

        if trace_info['registration_number']:
            lines.append(f"  Registration Number: {trace_info['registration_number']}")

        if trace_info['components']:
            lines.append("  Components:")
            for comp, data in trace_info['components'].items():
                lines.append(f"    - {comp}: {data.get('event', 'unknown')}")

        if trace_info['history']:
            lines.append(f"  History: {len(trace_info['history'])} events")

        return "\n".join(lines)

    @staticmethod
    def verify_trace_chain(unit_dir: Path) -> Dict[str, Any]:
        """
        Проверяет целостность trace цепи.

        Args:
            unit_dir: Path к директории UNIT

        Returns:
            Словарь с результатами проверки:
            - valid:True если цепь валидна
            - missing_components: список отсутствующих компонентов
            - gaps: обнаруженные разрывы в цепи
        """
        trace_info = TraceManager.get_trace_info(unit_dir)

        result = {
            "valid": True,
            "primary_id": trace_info["primary_id"],
            "missing_components": [],
            "gaps": [],
        }

        # Ожидаемые компоненты в цепи
        expected_components = ["docreciv", "docprep"]
        # TODO: Добавить doclingproc, llm_qaenrich когда будут реализованы

        for comp in expected_components:
            if comp not in trace_info["components"]:
                result["missing_components"].append(comp)
                result["valid"] = False

        # Проверяем историю на разрывы
        history = trace_info.get("history", [])
        if not history:
            result["gaps"].append("No history found")
            result["valid"] = False

        return result

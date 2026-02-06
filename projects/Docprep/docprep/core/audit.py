"""
Audit Log - журналирование всех операций preprocessing.

Append-only JSONL формат для отслеживания истории изменений UNIT.
"""
import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import uuid


class AuditLogger:
    """
    Логгер для записи событий в audit.log.jsonl.

    Каждое событие записывается в формате JSONL (JSON Lines) в файл
    audit.log.jsonl внутри директории UNIT.
    """

    def __init__(self):
        """Инициализирует AuditLogger."""
        self._current_correlation_id: Optional[str] = None

    def get_correlation_id(self) -> str:
        """
        Генерирует новый correlation_id для связи событий.

        Returns:
            UUID строка
        """
        self._current_correlation_id = str(uuid.uuid4())
        return self._current_correlation_id

    def log_event(
        self,
        unit_id: str,
        event_type: str,
        operation: str,
        details: Dict[str, Any],
        state_before: Optional[str] = None,
        state_after: Optional[str] = None,
        unit_path: Optional[Path] = None,
    ) -> None:
        """
        Логирует событие в audit.log.jsonl.

        Args:
            unit_id: Идентификатор UNIT
            event_type: Тип события (transition, operation, error, invalid_transition)
            operation: Операция (classify, convert, extract, normalize, merge)
            details: Детали операции (контекст)
            state_before: Состояние до операции
            state_after: Состояние после операции
            unit_path: Путь к директории UNIT (если не указан, используется корневая директория)
        """
        # Генерируем correlation_id если его нет
        if not self._current_correlation_id:
            self._current_correlation_id = self.get_correlation_id()

        event = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "unit_id": unit_id,
            "event_type": event_type,
            "operation": operation,
            "correlation_id": self._current_correlation_id,
            "details": details,
            "state_before": state_before,
            "state_after": state_after,
        }

        # Определяем путь к audit.log.jsonl
        if unit_path:
            log_path = unit_path / "audit.log.jsonl"
        else:
            # Если путь не указан, это ошибка - логи должны сохраняться в директории UNIT
            # Используем временную директорию для предотвращения создания файлов в корне
            import warnings
            warnings.warn(
                f"log_event called without unit_path for {unit_id}. "
                f"Audit log should be saved in UNIT directory. "
                f"Using temporary location to prevent root directory pollution."
            )
            # Создаем в временной директории вместо корня проекта
            import tempfile
            temp_dir = Path(tempfile.gettempdir()) / "docprep_audit"
            temp_dir.mkdir(parents=True, exist_ok=True)
            log_path = temp_dir / f"audit_{unit_id}.log.jsonl"

        self._write_to_log(log_path, event)

    def _write_to_log(self, log_path: Path, event: Dict[str, Any]) -> None:
        """
        Записывает событие в файл audit.log.jsonl.

        Args:
            log_path: Путь к файлу audit.log.jsonl
            event: Событие для записи
        """
        # Создаем директорию если нужно
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # Append-only запись
        with open(log_path, "a", encoding="utf-8") as f:
            json.dump(event, f, ensure_ascii=False)
            f.write("\n")

    def reset_correlation_id(self) -> None:
        """Сбрасывает текущий correlation_id."""
        self._current_correlation_id = None


# Глобальный экземпляр логгера
_audit_logger = AuditLogger()


def get_audit_logger() -> AuditLogger:
    """
    Возвращает глобальный экземпляр AuditLogger.

    Returns:
        Экземпляр AuditLogger
    """
    return _audit_logger


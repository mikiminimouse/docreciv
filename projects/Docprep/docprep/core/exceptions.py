"""
Кастомные исключения для preprocessing системы.

Иерархия исключений позволяет точно определять тип ошибки
и применять соответствующую политику обработки.
"""


class PreprocessingError(Exception):
    """Базовое исключение для всех ошибок preprocessing."""

    def __init__(self, message: str, unit_id: str = None, details: dict = None):
        super().__init__(message)
        self.message = message
        self.unit_id = unit_id
        self.details = details or {}

    def __str__(self):
        if self.unit_id:
            return f"[{self.unit_id}] {self.message}"
        return self.message


class StateTransitionError(PreprocessingError):
    """Ошибка при попытке выполнить недопустимый переход состояния."""

    def __init__(
        self, message: str, unit_id: str = None, current_state: str = None, target_state: str = None
    ):
        super().__init__(message, unit_id)
        self.current_state = current_state
        self.target_state = target_state
        if current_state and target_state:
            self.details = {
                "current_state": current_state,
                "target_state": target_state,
            }


class ManifestError(PreprocessingError):
    """Ошибка работы с manifest.json."""

    def __init__(self, message: str, unit_id: str = None, manifest_path: str = None):
        super().__init__(message, unit_id)
        self.manifest_path = manifest_path
        if manifest_path:
            self.details = {"manifest_path": manifest_path}


class OperationError(PreprocessingError):
    """Ошибка выполнения операции (convert/extract/normalize)."""

    def __init__(
        self,
        message: str,
        unit_id: str = None,
        operation: str = None,
        operation_details: dict = None,
    ):
        super().__init__(message, unit_id)
        self.operation = operation
        self.operation_details = operation_details or {}
        if operation:
            self.details = {"operation": operation, **self.operation_details}


class QuarantineError(PreprocessingError):
    """Ошибка, требующая изоляции файла/UNIT (опасные данные, zip bomb и т.д.)."""

    def __init__(
        self, message: str, unit_id: str = None, severity: str = "high", reason: str = None
    ):
        super().__init__(message, unit_id)
        self.severity = severity  # low, medium, high, critical
        self.reason = reason
        self.details = {
            "severity": severity,
            "quarantined": True,
            "reason": reason or message,
        }


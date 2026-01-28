"""
Единая модель состояний для модуля receiver.

Определяет все возможные состояния протоколов, файлов и units.
"""
from enum import Enum


class ProtocolState(Enum):
    """Состояния протоколов в системе."""
    PENDING = "pending"  # Протокол синхронизирован, документы не скачаны
    DOWNLOADED = "downloaded"  # Документы скачаны, готовы к обработке
    PROCESSING = "processing"  # В процессе обработки
    PROCESSED = "processed"  # Обработка завершена
    ERROR = "error"  # Ошибка обработки


class FileState(Enum):
    """Состояния файлов в системе."""
    INPUT = "input"  # Файл в директории input
    PENDING = "pending"  # Файл распределен по категории в pending
    NORMALIZED = "normalized"  # Файл нормализован, создан manifest
    PROCESSED = "processed"  # Файл обработан Docling
    ERROR = "error"  # Ошибка обработки


class UnitState(Enum):
    """Состояния units в системе."""
    CREATED = "created"  # Unit создан
    DISTRIBUTED = "distributed"  # Unit распределен по категориям
    NORMALIZED = "normalized"  # Unit нормализован
    PROCESSED = "processed"  # Unit обработан
    ERROR = "error"  # Ошибка обработки


def validate_state_transition(
    current_state: Enum,
    new_state: Enum,
    allowed_transitions: dict
) -> bool:
    """
    Валидирует переход из одного состояния в другое.
    
    Args:
        current_state: Текущее состояние
        new_state: Новое состояние
        allowed_transitions: Словарь разрешенных переходов
    
    Returns:
        True если переход разрешен, False иначе
    """
    if current_state not in allowed_transitions:
        return False
    
    allowed = allowed_transitions[current_state]
    return new_state in allowed


# Разрешенные переходы для ProtocolState
PROTOCOL_STATE_TRANSITIONS = {
    ProtocolState.PENDING: [ProtocolState.DOWNLOADED, ProtocolState.ERROR],
    ProtocolState.DOWNLOADED: [ProtocolState.PROCESSING, ProtocolState.ERROR],
    ProtocolState.PROCESSING: [ProtocolState.PROCESSED, ProtocolState.ERROR],
    ProtocolState.PROCESSED: [],  # Финальное состояние
    ProtocolState.ERROR: []  # Финальное состояние
}

# Разрешенные переходы для FileState
FILE_STATE_TRANSITIONS = {
    FileState.INPUT: [FileState.PENDING, FileState.ERROR],
    FileState.PENDING: [FileState.NORMALIZED, FileState.ERROR],
    FileState.NORMALIZED: [FileState.PROCESSED, FileState.ERROR],
    FileState.PROCESSED: [],  # Финальное состояние
    FileState.ERROR: []  # Финальное состояние
}

# Разрешенные переходы для UnitState
UNIT_STATE_TRANSITIONS = {
    UnitState.CREATED: [UnitState.DISTRIBUTED, UnitState.ERROR],
    UnitState.DISTRIBUTED: [UnitState.NORMALIZED, UnitState.ERROR],
    UnitState.NORMALIZED: [UnitState.PROCESSED, UnitState.ERROR],
    UnitState.PROCESSED: [],  # Финальное состояние
    UnitState.ERROR: []  # Финальное состояние
}


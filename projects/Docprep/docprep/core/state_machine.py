"""
State Machine для управления переходами состояний UNIT.

Реализует детерминированную state machine на уровне UNIT согласно PRD раздел 13.
Файловая система является материализованным представлением состояний.
"""
from enum import Enum
from typing import List, Optional, Dict, Any
from pathlib import Path
import json
import logging

from .exceptions import StateTransitionError

logger = logging.getLogger(__name__)


class UnitState(Enum):
    """Состояния UNIT согласно плану."""

    RAW = "RAW"  # Начальное состояние из Input
    CLASSIFIED_1 = "CLASSIFIED_1"  # После классификации цикла 1
    CLASSIFIED_2 = "CLASSIFIED_2"  # После классификации цикла 2
    CLASSIFIED_3 = "CLASSIFIED_3"  # После классификации цикла 3
    PENDING_CONVERT = "PENDING_CONVERT"  # Требует конвертации
    PENDING_EXTRACT = "PENDING_EXTRACT"  # Требует разархивации
    PENDING_NORMALIZE = "PENDING_NORMALIZE"  # Требует нормализации
    MERGED_DIRECT = "MERGED_DIRECT"  # Готов к merge (direct из цикла 1)
    MERGED_PROCESSED = "MERGED_PROCESSED"  # Готов к merge (обработанный)
    EXCEPTION_1 = "EXCEPTION_1"  # Ошибка цикла 1
    EXCEPTION_2 = "EXCEPTION_2"  # Ошибка цикла 2
    EXCEPTION_3 = "EXCEPTION_3"  # Ошибка цикла 3
    READY_FOR_DOCLING = "READY_FOR_DOCLING"  # Финальное состояние
    MERGER_SKIPPED = "MERGER_SKIPPED"  # Отфильтровано Merger (mixed, unsupported и т.д.)


# Разрешенные переходы между состояниями
ALLOWED_TRANSITIONS: Dict[UnitState, List[UnitState]] = {
    UnitState.RAW: [
        UnitState.CLASSIFIED_1,
        UnitState.EXCEPTION_1,
        UnitState.MERGED_DIRECT,
        UnitState.MERGER_SKIPPED,  # Для единиц, которые пропускаются при финальном merge (пустые/невалидные)
    ],  # EXCEPTION_1 для пустых UNIT, MERGED_DIRECT для direct файлов
    UnitState.CLASSIFIED_1: [
        UnitState.CLASSIFIED_1, # Self-transition allowed for re-classification
        UnitState.MERGED_DIRECT,
        UnitState.PENDING_CONVERT,
        UnitState.PENDING_EXTRACT,
        UnitState.PENDING_NORMALIZE,
        UnitState.EXCEPTION_1,
        UnitState.MERGER_SKIPPED,
    ],
    UnitState.PENDING_CONVERT: [UnitState.CLASSIFIED_2],
    UnitState.PENDING_EXTRACT: [UnitState.CLASSIFIED_2],
    UnitState.PENDING_NORMALIZE: [UnitState.CLASSIFIED_2],
    UnitState.CLASSIFIED_2: [
        UnitState.CLASSIFIED_2, # Self-transition allowed for re-classification
        UnitState.MERGED_PROCESSED,
        UnitState.PENDING_CONVERT,
        UnitState.PENDING_EXTRACT,
        UnitState.PENDING_NORMALIZE,
        UnitState.CLASSIFIED_3,
        UnitState.EXCEPTION_2,
        UnitState.MERGER_SKIPPED,
    ],
    UnitState.CLASSIFIED_3: [
        UnitState.MERGED_PROCESSED,
        UnitState.EXCEPTION_3,
        UnitState.MERGER_SKIPPED,
    ],
    UnitState.MERGED_DIRECT: [UnitState.READY_FOR_DOCLING, UnitState.MERGER_SKIPPED],
    UnitState.MERGED_PROCESSED: [UnitState.READY_FOR_DOCLING, UnitState.MERGER_SKIPPED, UnitState.MERGED_PROCESSED],  # Рефлексивный переход для повторной обработки
    # Финальные состояния (разрешаем переходы для переобработки/спасения)
    UnitState.READY_FOR_DOCLING: [],
    UnitState.EXCEPTION_1: [UnitState.CLASSIFIED_1],
    UnitState.EXCEPTION_2: [UnitState.CLASSIFIED_2],
    UnitState.EXCEPTION_3: [UnitState.CLASSIFIED_3, UnitState.MERGED_PROCESSED],
    UnitState.MERGER_SKIPPED: [UnitState.CLASSIFIED_1, UnitState.CLASSIFIED_2, UnitState.CLASSIFIED_3],
}


class UnitStateMachine:
    """
    State Machine для управления переходами состояний UNIT.

    Согласно PRD:
    - UNIT никогда не дробится
    - UNIT может находиться только в одном состоянии
    - Переходы выполняются только через Classifier
    - Максимум 3 классификационных цикла
    """

    def __init__(self, unit_id: str, manifest_path: Optional[Path] = None):
        """
        Инициализирует state machine для UNIT.

        Args:
            unit_id: Идентификатор UNIT
            manifest_path: Путь к manifest.json (опционально)
        """
        self.unit_id = unit_id
        self.manifest_path = manifest_path
        self._state_trace: List[str] = []
        self._current_state: Optional[UnitState] = None

        # Загружаем состояние из manifest, если он существует
        if manifest_path and manifest_path.exists():
            self._load_from_manifest()
        else:
            # Начальное состояние
            self._current_state = UnitState.RAW
            self._state_trace = [UnitState.RAW.value]

    def _load_from_manifest(self) -> None:
        """Загружает состояние из manifest.json."""
        try:
            with open(self.manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)

            # Загружаем state_trace из manifest v2
            if "state_machine" in manifest:
                state_machine = manifest["state_machine"]
                state_trace = state_machine.get("state_trace", [])

                if state_trace:
                    self._state_trace = state_trace
                    # Текущее состояние - последнее в trace
                    last_state_str = state_trace[-1]
                    try:
                        self._current_state = UnitState(last_state_str)
                    except ValueError:
                        # Если состояние неизвестно, начинаем с RAW
                        self._current_state = UnitState.RAW
                        self._state_trace = [UnitState.RAW.value]
                else:
                    self._current_state = UnitState.RAW
                    self._state_trace = [UnitState.RAW.value]
            else:
                # Старый формат manifest - начинаем с RAW
                self._current_state = UnitState.RAW
                self._state_trace = [UnitState.RAW.value]
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse manifest for unit {self.unit_id}: {e}")
            raise StateTransitionError(f"Failed to parse manifest: {e}", unit_id=self.unit_id)
        except Exception as e:
            # При других ошибках логируем и пробрасываем
            logger.error(f"Error loading manifest for unit {self.unit_id}: {e}")
            raise

    def get_current_state(self) -> UnitState:
        """Возвращает текущее состояние UNIT."""
        if self._current_state is None:
            return UnitState.RAW
        return self._current_state

    def get_state_trace(self) -> List[str]:
        """Возвращает историю переходов состояний."""
        return self._state_trace.copy()

    def can_transition_to(self, new_state: UnitState) -> bool:
        """
        Проверяет, возможен ли переход в новое состояние.

        Args:
            new_state: Целевое состояние

        Returns:
            True если переход разрешен, False иначе
        """
        if self._current_state is None:
            return new_state == UnitState.RAW

        allowed = ALLOWED_TRANSITIONS.get(self._current_state, [])
        return new_state in allowed

    def transition(self, new_state: UnitState) -> None:
        """
        Выполняет переход в новое состояние с валидацией.

        Args:
            new_state: Целевое состояние

        Raises:
            StateTransitionError: Если переход не разрешен
        """
        if not self.can_transition_to(new_state):
            current_str = self._current_state.value if self._current_state else "None"
            raise StateTransitionError(
                f"Transition from {current_str} to {new_state.value} is not allowed",
                unit_id=self.unit_id,
                current_state=current_str,
                target_state=new_state.value,
            )

        # Выполняем переход
        old_state = self._current_state
        self._current_state = new_state
        self._state_trace.append(new_state.value)

    def get_cycle_from_state(self) -> Optional[int]:
        """
        Определяет номер цикла на основе текущего состояния.

        Returns:
            Номер цикла (1, 2, 3) или None
        """
        if self._current_state in [
            UnitState.CLASSIFIED_1,
            UnitState.PENDING_CONVERT,
            UnitState.PENDING_EXTRACT,
            UnitState.PENDING_NORMALIZE,
            UnitState.EXCEPTION_1,
        ]:
            return 1
        elif self._current_state in [
            UnitState.CLASSIFIED_2,
            UnitState.EXCEPTION_2,
        ]:
            return 2
        elif self._current_state in [
            UnitState.CLASSIFIED_3,
            UnitState.EXCEPTION_3,
        ]:
            return 3
        elif self._current_state in [
            UnitState.MERGED_DIRECT,
            UnitState.MERGED_PROCESSED,
            UnitState.READY_FOR_DOCLING,
        ]:
            # Определяем из trace
            for state_str in reversed(self._state_trace):
                if "CLASSIFIED_1" in state_str or "PENDING" in state_str:
                    return 1
                elif "CLASSIFIED_2" in state_str:
                    return 2
                elif "CLASSIFIED_3" in state_str:
                    return 3
        return None

    def is_final_state(self) -> bool:
        """Проверяет, является ли текущее состояние финальным."""
        return self._current_state in [
            UnitState.READY_FOR_DOCLING,
            UnitState.EXCEPTION_1,
            UnitState.EXCEPTION_2,
            UnitState.EXCEPTION_3,
        ]

    def transition_and_save(self, new_state: UnitState, manifest_path: Path) -> None:
        """
        Атомарно выполняет переход состояния и сохраняет в manifest.
        
        Инкапсулирует логику transition() + _save_to_manifest() для
        предотвращения рассинхронизации состояния и файла.
        
        Args:
            new_state: Целевое состояние
            manifest_path: Путь к manifest.json
        
        Raises:
            StateTransitionError: Если переход не разрешен
        """
        self.transition(new_state)
        self._save_to_manifest(manifest_path)

    def _save_to_manifest(self, manifest_path: Path) -> None:
        """
        Сохраняет состояние в manifest.json.

        Args:
            manifest_path: Путь к manifest.json
        """
        try:
            # Загружаем существующий manifest или создаем новый
            if manifest_path.exists():
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
            else:
                manifest = {"schema_version": "2.0", "unit_id": self.unit_id}

            # Обновляем state_machine секцию
            manifest["state_machine"] = {
                "initial_state": self._state_trace[0] if self._state_trace else UnitState.RAW.value,
                "current_state": self._current_state.value if self._current_state else UnitState.RAW.value,
                "final_state": self._state_trace[-1] if self._state_trace else None,
                "state_trace": self._state_trace.copy(),
            }

            # Сохраняем
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise RuntimeError(f"Failed to save state to manifest: {e}")


def validate_state_transition(current_state: UnitState, new_state: UnitState) -> bool:
    """
    Валидирует переход между состояниями.

    Args:
        current_state: Текущее состояние
        new_state: Целевое состояние

    Returns:
        True если переход разрешен
    """
    allowed = ALLOWED_TRANSITIONS.get(current_state, [])
    return new_state in allowed


"""
Тесты для core/state_machine.py - State Machine управления состояниями UNIT.

Покрывает:
- Инициализация State Machine
- Валидация переходов между состояниями
- Полные циклы обработки
- Обработка ошибок и recovery
- MERGER_SKIPPED состояние
- Сохранение и загрузка состояний
- Граничные случаи
"""
import pytest
import json
from pathlib import Path
from datetime import datetime

from docprep.core.state_machine import (
    UnitState,
    UnitStateMachine,
    ALLOWED_TRANSITIONS,
    validate_state_transition,
)
from docprep.core.exceptions import StateTransitionError


# =============================================================================
# Блок 1: Инициализация
# =============================================================================


class TestStateMachineInitialization:
    """Тесты инициализации State Machine."""

    def test_state_machine_init_raw(self):
        """Новый State Machine начинается с RAW."""
        sm = UnitStateMachine("UNIT_001")
        assert sm.get_current_state() == UnitState.RAW
        assert sm.get_state_trace() == ["RAW"]
        assert sm.unit_id == "UNIT_001"

    def test_state_machine_load_from_manifest(self, temp_dir):
        """State Machine загружает состояние из manifest.json."""
        unit_id = "UNIT_002"
        manifest_path = temp_dir / "manifest.json"

        # Создаём manifest с историей состояний
        manifest = {
            "schema_version": "2.0",
            "unit_id": unit_id,
            "state_machine": {
                "initial_state": "RAW",
                "current_state": "CLASSIFIED_1",
                "final_state": "CLASSIFIED_1",
                "state_trace": ["RAW", "CLASSIFIED_1"],
            },
        }
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        sm = UnitStateMachine(unit_id, manifest_path)
        assert sm.get_current_state() == UnitState.CLASSIFIED_1
        assert sm.get_state_trace() == ["RAW", "CLASSIFIED_1"]

    def test_get_cycle_from_state_all_cycles(self):
        """get_cycle_from_state возвращает правильный цикл для всех состояний."""
        # Cycle 1 states
        for state in [UnitState.CLASSIFIED_1, UnitState.PENDING_CONVERT,
                      UnitState.PENDING_EXTRACT, UnitState.PENDING_NORMALIZE,
                      UnitState.EXCEPTION_1]:
            sm = UnitStateMachine("UNIT_test")
            sm._current_state = state
            sm._state_trace = ["RAW", state.value]
            assert sm.get_cycle_from_state() == 1, f"Expected cycle 1 for {state}"

        # Cycle 2 states
        for state in [UnitState.CLASSIFIED_2, UnitState.EXCEPTION_2]:
            sm = UnitStateMachine("UNIT_test")
            sm._current_state = state
            sm._state_trace = ["RAW", "CLASSIFIED_1", "PENDING_CONVERT", state.value]
            assert sm.get_cycle_from_state() == 2, f"Expected cycle 2 for {state}"

        # Cycle 3 states
        for state in [UnitState.CLASSIFIED_3, UnitState.EXCEPTION_3]:
            sm = UnitStateMachine("UNIT_test")
            sm._current_state = state
            sm._state_trace = ["RAW", "CLASSIFIED_1", "CLASSIFIED_2", state.value]
            assert sm.get_cycle_from_state() == 3, f"Expected cycle 3 for {state}"


# =============================================================================
# Блок 2: Валидация переходов
# =============================================================================


class TestStateTransitionValidation:
    """Тесты валидации переходов между состояниями."""

    def test_allowed_transitions_from_raw(self):
        """RAW может перейти в CLASSIFIED_1, EXCEPTION_1 или MERGED_DIRECT."""
        sm = UnitStateMachine("UNIT_003")

        assert sm.can_transition_to(UnitState.CLASSIFIED_1) is True
        assert sm.can_transition_to(UnitState.EXCEPTION_1) is True
        assert sm.can_transition_to(UnitState.MERGED_DIRECT) is True

        # Недопустимые переходы
        assert sm.can_transition_to(UnitState.CLASSIFIED_2) is False
        assert sm.can_transition_to(UnitState.READY_FOR_DOCLING) is False

    def test_allowed_transitions_from_classified_1(self):
        """CLASSIFIED_1 имеет множество допустимых переходов."""
        sm = UnitStateMachine("UNIT_004")
        sm.transition(UnitState.CLASSIFIED_1)

        allowed = [
            UnitState.CLASSIFIED_1,  # Self-transition
            UnitState.MERGED_DIRECT,
            UnitState.PENDING_CONVERT,
            UnitState.PENDING_EXTRACT,
            UnitState.PENDING_NORMALIZE,
            UnitState.EXCEPTION_1,
            UnitState.MERGER_SKIPPED,
        ]

        for state in allowed:
            assert sm.can_transition_to(state) is True, f"Should allow {state}"

    def test_self_transition_classified_1(self):
        """Self-transition разрешён для CLASSIFIED_1 (re-classification)."""
        sm = UnitStateMachine("UNIT_005")
        sm.transition(UnitState.CLASSIFIED_1)

        # Self-transition должен работать
        sm.transition(UnitState.CLASSIFIED_1)
        assert sm.get_current_state() == UnitState.CLASSIFIED_1
        assert sm.get_state_trace().count("CLASSIFIED_1") == 2

    def test_self_transition_merged_processed(self):
        """Self-transition разрешён для MERGED_PROCESSED."""
        sm = UnitStateMachine("UNIT_006")
        sm._current_state = UnitState.MERGED_PROCESSED
        sm._state_trace = ["RAW", "CLASSIFIED_1", "PENDING_CONVERT", "CLASSIFIED_2", "MERGED_PROCESSED"]

        assert sm.can_transition_to(UnitState.MERGED_PROCESSED) is True
        sm.transition(UnitState.MERGED_PROCESSED)
        assert sm.get_state_trace()[-1] == "MERGED_PROCESSED"

    def test_invalid_transition_raises_error(self):
        """Недопустимый переход вызывает StateTransitionError."""
        sm = UnitStateMachine("UNIT_007")

        with pytest.raises(StateTransitionError) as exc_info:
            sm.transition(UnitState.READY_FOR_DOCLING)

        assert "RAW" in str(exc_info.value)
        assert "READY_FOR_DOCLING" in str(exc_info.value)


# =============================================================================
# Блок 3: Полные циклы
# =============================================================================


class TestFullCycles:
    """Тесты полных циклов обработки."""

    def test_full_cycle_1_direct(self):
        """Полный цикл 1 с прямым путём: RAW -> CLASSIFIED_1 -> MERGED_DIRECT -> READY."""
        sm = UnitStateMachine("UNIT_direct")

        sm.transition(UnitState.CLASSIFIED_1)
        sm.transition(UnitState.MERGED_DIRECT)
        sm.transition(UnitState.READY_FOR_DOCLING)

        assert sm.get_current_state() == UnitState.READY_FOR_DOCLING
        assert sm.get_state_trace() == ["RAW", "CLASSIFIED_1", "MERGED_DIRECT", "READY_FOR_DOCLING"]

    def test_full_cycle_1_with_convert(self):
        """Цикл 1 с конвертацией: RAW -> CLASSIFIED_1 -> PENDING_CONVERT -> CLASSIFIED_2."""
        sm = UnitStateMachine("UNIT_convert")

        sm.transition(UnitState.CLASSIFIED_1)
        sm.transition(UnitState.PENDING_CONVERT)
        sm.transition(UnitState.CLASSIFIED_2)

        assert sm.get_current_state() == UnitState.CLASSIFIED_2
        assert sm.get_cycle_from_state() == 2

    def test_full_cycle_1_with_extract(self):
        """Цикл 1 с распаковкой: RAW -> CLASSIFIED_1 -> PENDING_EXTRACT -> CLASSIFIED_2."""
        sm = UnitStateMachine("UNIT_extract")

        sm.transition(UnitState.CLASSIFIED_1)
        sm.transition(UnitState.PENDING_EXTRACT)
        sm.transition(UnitState.CLASSIFIED_2)

        assert sm.get_current_state() == UnitState.CLASSIFIED_2

    def test_full_cycle_1_with_normalize(self):
        """Цикл 1 с нормализацией: RAW -> CLASSIFIED_1 -> PENDING_NORMALIZE -> CLASSIFIED_2."""
        sm = UnitStateMachine("UNIT_normalize")

        sm.transition(UnitState.CLASSIFIED_1)
        sm.transition(UnitState.PENDING_NORMALIZE)
        sm.transition(UnitState.CLASSIFIED_2)

        assert sm.get_current_state() == UnitState.CLASSIFIED_2

    def test_all_pending_lead_to_classified_2(self):
        """Все PENDING состояния ведут только к CLASSIFIED_2."""
        pending_states = [
            UnitState.PENDING_CONVERT,
            UnitState.PENDING_EXTRACT,
            UnitState.PENDING_NORMALIZE,
        ]

        for pending in pending_states:
            allowed = ALLOWED_TRANSITIONS.get(pending, [])
            assert allowed == [UnitState.CLASSIFIED_2], f"{pending} should only lead to CLASSIFIED_2"

    def test_cycle_2_to_cycle_3(self):
        """Переход из цикла 2 в цикл 3: CLASSIFIED_2 -> CLASSIFIED_3."""
        sm = UnitStateMachine("UNIT_cycle3")
        sm._current_state = UnitState.CLASSIFIED_2
        sm._state_trace = ["RAW", "CLASSIFIED_1", "PENDING_CONVERT", "CLASSIFIED_2"]

        sm.transition(UnitState.CLASSIFIED_3)

        assert sm.get_current_state() == UnitState.CLASSIFIED_3
        assert sm.get_cycle_from_state() == 3


# =============================================================================
# Блок 4: Обработка ошибок
# =============================================================================


class TestErrorHandling:
    """Тесты обработки ошибок и recovery."""

    def test_raw_to_exception_1(self):
        """RAW может перейти в EXCEPTION_1 для пустых UNIT."""
        sm = UnitStateMachine("UNIT_empty")

        sm.transition(UnitState.EXCEPTION_1)

        assert sm.get_current_state() == UnitState.EXCEPTION_1

    def test_recovery_from_exception_1(self):
        """Возможен recovery из EXCEPTION_1 -> CLASSIFIED_1."""
        sm = UnitStateMachine("UNIT_recovery1")
        sm._current_state = UnitState.EXCEPTION_1
        sm._state_trace = ["RAW", "CLASSIFIED_1", "EXCEPTION_1"]

        assert sm.can_transition_to(UnitState.CLASSIFIED_1) is True
        sm.transition(UnitState.CLASSIFIED_1)
        assert sm.get_current_state() == UnitState.CLASSIFIED_1

    def test_recovery_from_exception_2(self):
        """Возможен recovery из EXCEPTION_2 -> CLASSIFIED_2."""
        sm = UnitStateMachine("UNIT_recovery2")
        sm._current_state = UnitState.EXCEPTION_2
        sm._state_trace = ["RAW", "CLASSIFIED_1", "PENDING_CONVERT", "CLASSIFIED_2", "EXCEPTION_2"]

        assert sm.can_transition_to(UnitState.CLASSIFIED_2) is True
        sm.transition(UnitState.CLASSIFIED_2)
        assert sm.get_current_state() == UnitState.CLASSIFIED_2

    def test_recovery_from_exception_3(self):
        """Возможен recovery из EXCEPTION_3 -> CLASSIFIED_3 или MERGED_PROCESSED."""
        sm = UnitStateMachine("UNIT_recovery3")
        sm._current_state = UnitState.EXCEPTION_3
        sm._state_trace = ["RAW", "CLASSIFIED_1", "CLASSIFIED_2", "CLASSIFIED_3", "EXCEPTION_3"]

        assert sm.can_transition_to(UnitState.CLASSIFIED_3) is True
        assert sm.can_transition_to(UnitState.MERGED_PROCESSED) is True

    def test_exception_states_not_final(self):
        """EXCEPTION состояния не являются терминальными (is_final_state = True для них)."""
        # По текущей логике EXCEPTION считаются "финальными" для is_final_state()
        # но имеют возможность recovery в ALLOWED_TRANSITIONS
        sm = UnitStateMachine("UNIT_exc_final")

        for exc_state in [UnitState.EXCEPTION_1, UnitState.EXCEPTION_2, UnitState.EXCEPTION_3]:
            sm._current_state = exc_state
            # is_final_state возвращает True для EXCEPTION
            assert sm.is_final_state() is True
            # Но recovery возможен через ALLOWED_TRANSITIONS
            assert len(ALLOWED_TRANSITIONS.get(exc_state, [])) > 0


# =============================================================================
# Блок 5: MERGER_SKIPPED
# =============================================================================


class TestMergerSkipped:
    """Тесты для состояния MERGER_SKIPPED."""

    def test_merger_skipped_from_classified_1(self):
        """CLASSIFIED_1 может перейти в MERGER_SKIPPED."""
        sm = UnitStateMachine("UNIT_skipped1")
        sm.transition(UnitState.CLASSIFIED_1)

        assert sm.can_transition_to(UnitState.MERGER_SKIPPED) is True
        sm.transition(UnitState.MERGER_SKIPPED)
        assert sm.get_current_state() == UnitState.MERGER_SKIPPED

    def test_recovery_from_merger_skipped(self):
        """Возможен recovery из MERGER_SKIPPED -> CLASSIFIED_X."""
        sm = UnitStateMachine("UNIT_skipped_recovery")
        sm._current_state = UnitState.MERGER_SKIPPED
        sm._state_trace = ["RAW", "CLASSIFIED_1", "MERGER_SKIPPED"]

        # Можно вернуться в любой CLASSIFIED
        assert sm.can_transition_to(UnitState.CLASSIFIED_1) is True
        assert sm.can_transition_to(UnitState.CLASSIFIED_2) is True
        assert sm.can_transition_to(UnitState.CLASSIFIED_3) is True

    def test_merged_direct_to_ready(self):
        """MERGED_DIRECT -> READY_FOR_DOCLING."""
        sm = UnitStateMachine("UNIT_direct_ready")
        sm._current_state = UnitState.MERGED_DIRECT
        sm._state_trace = ["RAW", "CLASSIFIED_1", "MERGED_DIRECT"]

        assert sm.can_transition_to(UnitState.READY_FOR_DOCLING) is True
        sm.transition(UnitState.READY_FOR_DOCLING)
        assert sm.get_current_state() == UnitState.READY_FOR_DOCLING

    def test_merged_processed_is_not_final(self):
        """MERGED_PROCESSED не является терминальным состоянием."""
        sm = UnitStateMachine("UNIT_merged_proc")
        sm._current_state = UnitState.MERGED_PROCESSED

        assert sm.is_final_state() is False
        # Может перейти в READY или MERGER_SKIPPED или self
        assert sm.can_transition_to(UnitState.READY_FOR_DOCLING) is True
        assert sm.can_transition_to(UnitState.MERGER_SKIPPED) is True

    def test_ready_for_docling_is_terminal(self):
        """READY_FOR_DOCLING - терминальное состояние без исходящих переходов."""
        sm = UnitStateMachine("UNIT_terminal")
        sm._current_state = UnitState.READY_FOR_DOCLING

        assert sm.is_final_state() is True
        assert ALLOWED_TRANSITIONS.get(UnitState.READY_FOR_DOCLING) == []


# =============================================================================
# Блок 6: Сохранение состояний
# =============================================================================


class TestStatePersistence:
    """Тесты сохранения и загрузки состояний."""

    def test_transition_and_save_atomic(self, temp_dir):
        """transition_and_save атомарно сохраняет переход."""
        manifest_path = temp_dir / "manifest.json"
        sm = UnitStateMachine("UNIT_atomic")

        sm.transition_and_save(UnitState.CLASSIFIED_1, manifest_path)

        # Проверяем файл
        assert manifest_path.exists()
        with open(manifest_path) as f:
            saved = json.load(f)

        assert saved["state_machine"]["current_state"] == "CLASSIFIED_1"
        assert saved["state_machine"]["state_trace"] == ["RAW", "CLASSIFIED_1"]

    def test_full_state_trace_saved(self, temp_dir):
        """Полная история состояний сохраняется."""
        manifest_path = temp_dir / "manifest.json"
        sm = UnitStateMachine("UNIT_trace")

        # Проходим несколько состояний
        sm.transition_and_save(UnitState.CLASSIFIED_1, manifest_path)
        sm.transition_and_save(UnitState.PENDING_CONVERT, manifest_path)
        sm.transition_and_save(UnitState.CLASSIFIED_2, manifest_path)
        sm.transition_and_save(UnitState.MERGED_PROCESSED, manifest_path)

        with open(manifest_path) as f:
            saved = json.load(f)

        expected_trace = ["RAW", "CLASSIFIED_1", "PENDING_CONVERT", "CLASSIFIED_2", "MERGED_PROCESSED"]
        assert saved["state_machine"]["state_trace"] == expected_trace

    def test_load_invalid_manifest(self, temp_dir):
        """Загрузка невалидного manifest вызывает исключение."""
        manifest_path = temp_dir / "bad_manifest.json"
        manifest_path.write_text("{ invalid json }")

        with pytest.raises(StateTransitionError) as exc_info:
            UnitStateMachine("UNIT_bad", manifest_path)

        assert "Failed to parse manifest" in str(exc_info.value)


# =============================================================================
# Блок 7: Граничные случаи
# =============================================================================


class TestEdgeCases:
    """Тесты граничных случаев."""

    def test_max_cycles_enforced(self):
        """После CLASSIFIED_3 нельзя перейти в CLASSIFIED_4 (нет такого состояния)."""
        sm = UnitStateMachine("UNIT_max_cycles")
        sm._current_state = UnitState.CLASSIFIED_3
        sm._state_trace = ["RAW", "CLASSIFIED_1", "CLASSIFIED_2", "CLASSIFIED_3"]

        # Из CLASSIFIED_3 можно только в MERGED_PROCESSED, EXCEPTION_3, MERGER_SKIPPED
        allowed = ALLOWED_TRANSITIONS.get(UnitState.CLASSIFIED_3, [])
        assert UnitState.MERGED_PROCESSED in allowed
        assert UnitState.EXCEPTION_3 in allowed
        assert UnitState.MERGER_SKIPPED in allowed
        # Нет возможности продолжить циклы
        assert len([s for s in allowed if "CLASSIFIED" in s.value]) == 0

    def test_complex_processing_chain(self):
        """Сложная цепочка обработки с несколькими операциями."""
        sm = UnitStateMachine("UNIT_complex")

        # RAW -> CLASSIFIED_1 -> PENDING_EXTRACT -> CLASSIFIED_2 ->
        # PENDING_CONVERT -> CLASSIFIED_2 (re-classify) -> MERGED_PROCESSED -> READY
        sm.transition(UnitState.CLASSIFIED_1)
        sm.transition(UnitState.PENDING_EXTRACT)
        sm.transition(UnitState.CLASSIFIED_2)
        # После CLASSIFIED_2 можно снова в PENDING_CONVERT
        sm.transition(UnitState.PENDING_CONVERT)
        sm.transition(UnitState.CLASSIFIED_2)  # Re-classify
        sm.transition(UnitState.MERGED_PROCESSED)
        sm.transition(UnitState.READY_FOR_DOCLING)

        assert sm.get_current_state() == UnitState.READY_FOR_DOCLING
        assert len(sm.get_state_trace()) == 8

    def test_validate_state_transition_matches_can_transition(self):
        """validate_state_transition и can_transition_to дают одинаковый результат."""
        all_states = list(UnitState)

        for current in all_states:
            for target in all_states:
                # validate_state_transition - standalone функция
                result_validate = validate_state_transition(current, target)

                # can_transition_to - метод класса
                sm = UnitStateMachine("UNIT_check")
                sm._current_state = current
                result_method = sm.can_transition_to(target)

                assert result_validate == result_method, \
                    f"Mismatch for {current} -> {target}: validate={result_validate}, method={result_method}"


# =============================================================================
# Дополнительные тесты для coverage
# =============================================================================


class TestAllowedTransitionsCompleteness:
    """Проверка полноты ALLOWED_TRANSITIONS."""

    def test_all_states_have_transitions_defined(self):
        """Все состояния имеют определённые переходы в ALLOWED_TRANSITIONS."""
        for state in UnitState:
            assert state in ALLOWED_TRANSITIONS, f"Missing transitions for {state}"

    def test_no_orphan_target_states(self):
        """Все целевые состояния достижимы из какого-то исходного."""
        reachable_states = set()
        for targets in ALLOWED_TRANSITIONS.values():
            reachable_states.update(targets)

        # RAW - начальное состояние, достижимо по умолчанию
        reachable_states.add(UnitState.RAW)

        for state in UnitState:
            assert state in reachable_states, f"{state} is not reachable from any state"

    def test_cycle_determination_for_merged_states(self):
        """get_cycle_from_state работает для MERGED состояний через trace."""
        sm = UnitStateMachine("UNIT_merged_cycle")
        sm._current_state = UnitState.MERGED_DIRECT

        # Если в trace есть CLASSIFIED_1, должен вернуть cycle 1
        sm._state_trace = ["RAW", "CLASSIFIED_1", "MERGED_DIRECT"]
        assert sm.get_cycle_from_state() == 1

        # Если в trace есть CLASSIFIED_2, должен вернуть cycle 2
        sm._state_trace = ["RAW", "CLASSIFIED_1", "PENDING_CONVERT", "CLASSIFIED_2", "MERGED_PROCESSED"]
        sm._current_state = UnitState.MERGED_PROCESSED
        assert sm.get_cycle_from_state() == 2

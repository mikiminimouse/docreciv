"""
Тесты для state_machine.py - валидация state machine согласно PRD раздел 13.
"""
import pytest
import json
import tempfile
from pathlib import Path
import sys
from pathlib import Path

# Добавляем путь к модулю
sys.path.insert(0, str(Path(__file__).parent.parent))

from router.state_machine import (
    UnitStateMachine, UnitState, validate_state_transition, ALLOWED_TRANSITIONS
)


class TestUnitStateMachine:
    """Тесты для UnitStateMachine."""
    
    def test_initial_state(self):
        """Тест начального состояния."""
        sm = UnitStateMachine("UNIT_TEST")
        assert sm.get_current_state() == UnitState.RAW_INPUT
        assert sm.get_state_trace() == ["RAW_INPUT"]
    
    def test_transition_raw_input_to_classified_1(self):
        """Тест перехода RAW_INPUT → CLASSIFIED_1."""
        sm = UnitStateMachine("UNIT_TEST")
        assert sm.can_transition_to(UnitState.CLASSIFIED_1)
        sm.transition(UnitState.CLASSIFIED_1)
        assert sm.get_current_state() == UnitState.CLASSIFIED_1
        assert "CLASSIFIED_1" in sm.get_state_trace()
    
    def test_transition_classified_1_to_pending_1(self):
        """Тест перехода CLASSIFIED_1 → PENDING_1."""
        sm = UnitStateMachine("UNIT_TEST")
        sm.transition(UnitState.CLASSIFIED_1)
        assert sm.can_transition_to(UnitState.PENDING_1)
        sm.transition(UnitState.PENDING_1)
        assert sm.get_current_state() == UnitState.PENDING_1
    
    def test_transition_classified_1_to_merged_1_direct(self):
        """Тест перехода CLASSIFIED_1 → MERGED_1_DIRECT."""
        sm = UnitStateMachine("UNIT_TEST")
        sm.transition(UnitState.CLASSIFIED_1)
        assert sm.can_transition_to(UnitState.MERGED_1_DIRECT)
        sm.transition(UnitState.MERGED_1_DIRECT)
        assert sm.get_current_state() == UnitState.MERGED_1_DIRECT
    
    def test_transition_pending_1_to_classified_2(self):
        """Тест перехода PENDING_1 → CLASSIFIED_2."""
        sm = UnitStateMachine("UNIT_TEST")
        sm.transition(UnitState.CLASSIFIED_1)
        sm.transition(UnitState.PENDING_1)
        assert sm.can_transition_to(UnitState.CLASSIFIED_2)
        sm.transition(UnitState.CLASSIFIED_2)
        assert sm.get_current_state() == UnitState.CLASSIFIED_2
    
    def test_invalid_transition(self):
        """Тест недопустимого перехода."""
        sm = UnitStateMachine("UNIT_TEST")
        assert not sm.can_transition_to(UnitState.MERGED_1_DIRECT)  # Пропущен CLASSIFIED_1
        with pytest.raises(ValueError):
            sm.transition(UnitState.MERGED_1_DIRECT)
    
    def test_cycle_detection(self):
        """Тест определения цикла из состояния."""
        sm = UnitStateMachine("UNIT_TEST")
        sm.transition(UnitState.CLASSIFIED_1)
        assert sm.get_cycle_from_state() == 1
        
        sm.transition(UnitState.PENDING_1)
        sm.transition(UnitState.CLASSIFIED_2)
        assert sm.get_cycle_from_state() == 2
    
    def test_max_cycle_reached(self):
        """Тест проверки максимального цикла."""
        sm = UnitStateMachine("UNIT_TEST")
        sm.transition(UnitState.CLASSIFIED_1)
        sm.transition(UnitState.PENDING_1)
        sm.transition(UnitState.CLASSIFIED_2)
        sm.transition(UnitState.PENDING_2)
        sm.transition(UnitState.CLASSIFIED_3)
        assert sm.is_max_cycle_reached()
    
    def test_final_state(self):
        """Тест финальных состояний."""
        sm = UnitStateMachine("UNIT_TEST")
        sm.transition(UnitState.CLASSIFIED_1)
        sm.transition(UnitState.MERGED_1_DIRECT)
        sm.transition(UnitState.READY2DOCLING)
        assert sm.is_final_state()
        assert sm.get_current_state() == UnitState.READY2DOCLING
    
    def test_save_and_load_manifest(self):
        """Тест сохранения и загрузки состояния из manifest."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "manifest.json"
            
            # Создаем state machine и переходим
            sm1 = UnitStateMachine("UNIT_TEST", manifest_path)
            sm1.transition(UnitState.CLASSIFIED_1)
            sm1.transition(UnitState.PENDING_1)
            sm1.save_to_manifest(manifest_path)
            
            # Загружаем в новую state machine
            sm2 = UnitStateMachine("UNIT_TEST", manifest_path)
            assert sm2.get_current_state() == UnitState.PENDING_1
            assert len(sm2.get_state_trace()) == 3
    
    def test_state_trace_preservation(self):
        """Тест сохранения истории переходов."""
        sm = UnitStateMachine("UNIT_TEST")
        trace = ["RAW_INPUT"]
        
        sm.transition(UnitState.CLASSIFIED_1)
        trace.append("CLASSIFIED_1")
        assert sm.get_state_trace() == trace
        
        sm.transition(UnitState.PENDING_1)
        trace.append("PENDING_1")
        assert sm.get_state_trace() == trace


class TestStateTransitions:
    """Тесты для валидации переходов состояний."""
    
    def test_allowed_transitions_structure(self):
        """Тест структуры ALLOWED_TRANSITIONS согласно PRD."""
        # RAW_INPUT может переходить только в CLASSIFIED_1
        assert UnitState.CLASSIFIED_1 in ALLOWED_TRANSITIONS[UnitState.RAW_INPUT]
        assert len(ALLOWED_TRANSITIONS[UnitState.RAW_INPUT]) == 1
        
        # CLASSIFIED_1 может переходить в MERGED_1_DIRECT, PENDING_1, EXCEPTIONS_1
        allowed = ALLOWED_TRANSITIONS[UnitState.CLASSIFIED_1]
        assert UnitState.MERGED_1_DIRECT in allowed
        assert UnitState.PENDING_1 in allowed
        assert UnitState.EXCEPTIONS_1 in allowed
    
    def test_validate_state_transition_function(self):
        """Тест функции validate_state_transition."""
        assert validate_state_transition(UnitState.RAW_INPUT, UnitState.CLASSIFIED_1)
        assert not validate_state_transition(UnitState.RAW_INPUT, UnitState.PENDING_1)


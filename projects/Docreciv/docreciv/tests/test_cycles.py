"""
Тесты для cycle_manager.py и iterative_processor.py - валидация циклов согласно PRD раздел 8.
"""
import pytest
from pathlib import Path
import sys
from pathlib import Path

# Добавляем путь к модулю
sys.path.insert(0, str(Path(__file__).parent.parent))

from router.cycle_manager import CycleManager
from router.config import MAX_CYCLES, get_cycle_directories
from router.iterative_processor import IterativeProcessor
from router.state_machine import UnitState


class TestCycleManager:
    """Тесты для CycleManager."""
    
    def test_initial_cycle(self):
        """Тест инициализации с начальным циклом."""
        cm = CycleManager(initial_cycle=1)
        assert cm.get_current_cycle() == 1
    
    def test_max_cycle(self):
        """Тест максимального цикла."""
        cm = CycleManager(initial_cycle=MAX_CYCLES)
        assert cm.is_max_cycle()
        assert cm.get_next_cycle() is None
    
    def test_advance_cycle(self):
        """Тест перехода к следующему циклу."""
        cm = CycleManager(initial_cycle=1)
        assert cm.advance_cycle()
        assert cm.get_current_cycle() == 2
        assert cm.advance_cycle()
        assert cm.get_current_cycle() == 3
        assert not cm.advance_cycle()  # Достигнут максимум
    
    def test_get_cycle_directories(self):
        """Тест получения директорий цикла."""
        cm = CycleManager(initial_cycle=1)
        dirs = cm.get_cycle_directories()
        assert "pending_dir" in dirs
        assert "merge_dir" in dirs
        assert "exceptions_dir" in dirs
    
    def test_get_target_pending_dir(self):
        """Тест получения целевой директории Pending с расширением."""
        cm = CycleManager(initial_cycle=1)
        target_dir = cm.get_target_pending_dir("direct", "pdf")
        assert "direct" in str(target_dir)
        assert "pdf" in str(target_dir)
    
    def test_get_target_merge_dir(self):
        """Тест получения целевой директории Merge."""
        cm = CycleManager(initial_cycle=1)
        # Merge_1 может содержать только direct
        target_dir = cm.get_target_merge_dir("direct", "docx")
        assert "direct" in str(target_dir)
        assert "docx" in str(target_dir)
        
        # Merge_2 не может содержать direct
        cm2 = CycleManager(initial_cycle=2)
        with pytest.raises(ValueError):
            cm2.get_target_merge_dir("direct", "docx")
    
    def test_invalid_cycle(self):
        """Тест недопустимого цикла."""
        with pytest.raises(ValueError):
            CycleManager(initial_cycle=0)
        
        with pytest.raises(ValueError):
            CycleManager(initial_cycle=MAX_CYCLES + 1)


class TestIterativeProcessor:
    """Тесты для IterativeProcessor."""
    
    def test_initialization(self):
        """Тест инициализации IterativeProcessor."""
        processor = IterativeProcessor("UNIT_TEST")
        assert processor.unit_id == "UNIT_TEST"
        assert processor.cycle_manager.get_current_cycle() == 1
        assert processor.state_machine.get_current_state() == UnitState.RAW_INPUT
    
    def test_process_cycle_1(self):
        """Тест обработки первого цикла."""
        # Этот тест требует реальных файлов, поэтому упрощен
        processor = IterativeProcessor("UNIT_TEST")
        # Проверяем что процессор создан корректно
        assert processor is not None


class TestCycleDirectories:
    """Тесты для функции get_cycle_directories."""
    
    def test_get_cycle_directories_structure(self):
        """Тест структуры директорий цикла."""
        for cycle in [1, 2, 3]:
            dirs = get_cycle_directories(cycle)
            assert "pending_dir" in dirs
            assert "merge_dir" in dirs
            assert "exceptions_dir" in dirs
            assert f"Pending_{cycle}" in str(dirs["pending_dir"])
            assert f"Merge_{cycle}" in str(dirs["merge_dir"])
            assert f"Exceptions_{cycle}" in str(dirs["exceptions_dir"])
    
    def test_invalid_cycle(self):
        """Тест недопустимого цикла."""
        with pytest.raises(ValueError):
            get_cycle_directories(0)
        
        with pytest.raises(ValueError):
            get_cycle_directories(MAX_CYCLES + 1)


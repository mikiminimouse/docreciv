"""
Unit тесты для Classifier.
"""
import pytest
from pathlib import Path

from docprep.engine.classifier import Classifier
from docprep.core.state_machine import UnitState


def test_classifier_initialization():
    """Тест инициализации Classifier."""
    classifier = Classifier()
    assert classifier is not None
    assert classifier.audit_logger is not None


def test_classify_unit_pdf(sample_unit_dir):
    """Тест классификации UNIT с PDF файлами."""
    classifier = Classifier()
    result = classifier.classify_unit(sample_unit_dir, cycle=1)
    
    assert result is not None
    assert "category" in result
    assert "target_directory" in result
    assert result["category"] in ["direct", "convert", "extract", "normalize", "special", "mixed"]


def test_classify_unit_archive(sample_archive_unit):
    """Тест классификации UNIT с архивом."""
    classifier = Classifier()
    result = classifier.classify_unit(sample_archive_unit, cycle=1)
    
    assert result is not None
    assert result["category"] == "extract" or result["category"] == "special"


def test_classify_unit_mixed(sample_mixed_unit):
    """Тест классификации UNIT со смешанными типами."""
    classifier = Classifier()
    result = classifier.classify_unit(sample_mixed_unit, cycle=1)
    
    assert result is not None
    assert result.get("is_mixed", False) or result["category"] in ["special", "mixed"]


def test_classify_unit_creates_manifest(sample_unit_dir):
    """Тест создания manifest при классификации."""
    # Удаляем существующий manifest
    manifest_path = sample_unit_dir / "manifest.json"
    if manifest_path.exists():
        manifest_path.unlink()
    
    classifier = Classifier()
    result = classifier.classify_unit(sample_unit_dir, cycle=1)
    
    # Проверяем, что UNIT перемещен и manifest создан в новой локации
    moved_to = Path(result["moved_to"])
    new_manifest_path = moved_to / "manifest.json"
    
    assert new_manifest_path.exists()
    assert moved_to.exists()


def test_classify_unit_updates_state(sample_archive_unit_alt):
    """Тест обновления state machine при классификации."""
    classifier = Classifier()
    result = classifier.classify_unit(sample_archive_unit_alt, cycle=1)
    
    # Проверяем, что state обновлен
    from docprep.core.manifest import load_manifest
    target_dir = Path(result["moved_to"])
    manifest = load_manifest(target_dir)
    
    assert manifest["state_machine"]["current_state"] == UnitState.CLASSIFIED_1.value


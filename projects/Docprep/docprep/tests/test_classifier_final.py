#!/usr/bin/env python3
"""
Финальные тесты для Classifier с учетом Input директории и copy_mode.
"""
import pytest
from pathlib import Path
import tempfile
import shutil
import json
from datetime import datetime

from docprep.engine.classifier import Classifier
from docprep.core.state_machine import UnitState
from docprep.core.manifest import create_manifest_v2, load_manifest


@pytest.fixture
def temp_dir():
    """Создает временную директорию для тестов."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def sample_unit_dir_in_input_with_manifest(temp_dir):
    """Создает пример UNIT директории в Input с одним PDF файлом и манифестом."""
    # Создаем структуру с датой
    data_dir = temp_dir / "Data"
    input_with_date = data_dir / "2025-03-18" / "Input"
    input_with_date.mkdir(parents=True)
    
    # Создаем тестовый UNIT в Input директории
    unit_id = "UNIT_000123"
    unit_path = input_with_date / unit_id
    unit_path.mkdir()
    
    # Создаем один тестовый PDF файл
    (unit_path / "test.pdf").write_bytes(b"%PDF-1.4\nstream\nBT\n/F1 12 Tf\n(Test PDF content) Tj\nET\nendstream\n%%EOF")
    
    # Создаем manifest с начальным состоянием RAW
    manifest = create_manifest_v2(
        unit_id=unit_id,
        protocol_id="000123",
        protocol_date=datetime.now().strftime("%Y-%m-%d"),
        files=[
            {
                "original_name": "test.pdf",
                "current_name": "test.pdf",
                "mime_type": "application/pdf",
                "detected_type": "pdf",
                "needs_ocr": False,
                "sha256": "test_hash_pdf",
                "size": 100,
                "transformations": [],
            },
        ],
        current_cycle=1,
        state_trace=[UnitState.RAW.value],
    )
    
    manifest_path = unit_path / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    
    return unit_path


def test_classify_unit_in_input_creates_manifest(sample_unit_dir_in_input_with_manifest):
    """Тест создания manifest при классификации UNIT из Input директории."""
    # Удаляем существующий manifest если есть
    manifest_path = sample_unit_dir_in_input_with_manifest / "manifest.json"
    if manifest_path.exists():
        manifest_path.unlink()
    
    classifier = Classifier()
    result = classifier.classify_unit(sample_unit_dir_in_input_with_manifest, cycle=1)
    
    # Проверяем, что manifest создан в исходной директории (при copy_mode)
    assert manifest_path.exists()
    
    # Проверяем, что UNIT скопирован (исходная директория существует)
    assert sample_unit_dir_in_input_with_manifest.exists()
    
    # Проверяем категорию
    assert result["category"] == "direct"


def test_classify_unit_in_input_updates_state(sample_unit_dir_in_input_with_manifest):
    """Тест обновления state machine при классификации UNIT из Input директории."""
    classifier = Classifier()
    result = classifier.classify_unit(sample_unit_dir_in_input_with_manifest, cycle=1)
    
    # Получаем путь к скопированной директории
    moved_to_path = Path(result["moved_to"])
    
    # Проверяем, что state обновлен в скопированной директории
    manifest = load_manifest(moved_to_path)
    
    assert manifest is not None
    assert "state_machine" in manifest
    # После классификации состояние должно быть MERGED_DIRECT
    assert manifest["state_machine"]["current_state"] == UnitState.MERGED_DIRECT.value


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

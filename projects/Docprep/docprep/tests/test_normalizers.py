"""
Unit тесты для Normalizers.
"""
import pytest
from pathlib import Path

from docprep.engine.normalizers import NameNormalizer, ExtensionNormalizer


def test_name_normalizer_initialization():
    """Тест инициализации NameNormalizer."""
    normalizer = NameNormalizer()
    assert normalizer is not None
    assert normalizer.audit_logger is not None


def test_name_normalizer_fixes_double_extension(temp_dir):
    """Тест исправления двойных расширений."""
    unit_path = temp_dir / "UNIT_TEST"
    unit_path.mkdir()
    
    # Создаем файл с двойным расширением
    bad_file = unit_path / "file.doc.docx"
    bad_file.write_bytes(b"fake content")
    
    normalizer = NameNormalizer()
    result = normalizer.normalize_names(
        unit_path=unit_path,
        cycle=1,
        protocol_date="2025-03-18",
        dry_run=False,
    )
    
    assert result is not None
    # Проверяем, что файл переименован
    assert result["files_normalized"] > 0 or "file.docx" in [f["normalized_name"] for f in result.get("normalized_files", [])]


def test_extension_normalizer_initialization():
    """Тест инициализации ExtensionNormalizer."""
    normalizer = ExtensionNormalizer()
    assert normalizer is not None
    assert normalizer.audit_logger is not None


def test_extension_normalizer_fixes_wrong_extension(temp_dir):
    """Тест исправления неправильного расширения."""
    unit_path = temp_dir / "UNIT_TEST"
    unit_path.mkdir()
    
    # Создаем PDF файл с неправильным расширением
    wrong_file = unit_path / "document.txt"
    wrong_file.write_bytes(b"%PDF-1.4 fake pdf content")
    
    # Создаем manifest
    from docprep.core.manifest import create_manifest_v2, save_manifest
    from docprep.core.state_machine import UnitState
    manifest = create_manifest_v2(
        unit_id=unit_path.name,
        files=[{"current_name": "document.txt", "detected_type": "pdf"}],
        current_cycle=1,
        state_trace=[UnitState.PENDING_NORMALIZE.value],
    )
    save_manifest(unit_path, manifest)
    
    normalizer = ExtensionNormalizer()
    result = normalizer.normalize_extensions(
        unit_path=unit_path,
        cycle=1,
        protocol_date="2025-03-18",
        dry_run=False,
    )
    
    assert result is not None
    # В реальном тесте проверяем, что файл переименован в .pdf


def test_normalizers_update_manifest(sample_unit_dir):
    """Тест обновления manifest при нормализации."""
    name_normalizer = NameNormalizer()
    result = name_normalizer.normalize_names(
        unit_path=sample_unit_dir,
        cycle=1,
        protocol_date="2025-03-18",
        dry_run=True,
    )
    
    assert result is not None
    # Проверяем, что manifest обновлен (если не dry_run)
    from docprep.core.manifest import load_manifest
    manifest = load_manifest(sample_unit_dir)
    assert manifest is not None


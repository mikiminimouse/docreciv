"""
Pytest конфигурация и фикстуры для тестов.
"""
import pytest
import tempfile
import shutil
from pathlib import Path
import json
from datetime import datetime
import os
from unittest.mock import patch

from docprep.core.manifest import create_manifest_v2
from docprep.core.state_machine import UnitState


# Test counter for unique IDs
_test_counter = 0


def get_unique_test_id():
    """Генерирует уникальный ID для теста."""
    global _test_counter
    _test_counter += 1
    return f"UNIT_TEST_{datetime.now().timestamp()}_{_test_counter}"


@pytest.fixture(autouse=True)
def isolate_test_environment(temp_dir, monkeypatch):
    """
    Изолирует тестовое окружение от production данных.
    Перенаправляет все пути данных во временную директорию.
    """
    test_base = temp_dir / "docprep_test"
    test_base.mkdir()

    # Monkeypatch пути для изоляции (используем правильные имена из config.py)
    monkeypatch.setattr("docprep.core.config.DATA_BASE_DIR", test_base / "Data")
    monkeypatch.setattr("docprep.core.config.PROCESSING_DIR", test_base / "Data" / "Processing")
    monkeypatch.setattr("docprep.core.config.MERGE_DIR", test_base / "Data" / "Merge")
    monkeypatch.setattr("docprep.core.config.EXCEPTIONS_DIR", test_base / "Data" / "Exceptions")

    yield

    # Cleanup after test
    if test_base.exists():
        shutil.rmtree(test_base, ignore_errors=True)


@pytest.fixture
def temp_dir():
    """Создает временную директорию для тестов."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def sample_unit_dir(temp_dir):
    """Создает пример UNIT директории с файлами."""
    unit_id = "UNIT_000123"
    unit_path = temp_dir / unit_id
    unit_path.mkdir(parents=True)
    
    # Создаем тестовые файлы
    (unit_path / "test.pdf").write_bytes(b"%PDF-1.4 fake pdf content")
    (unit_path / "document.docx").write_bytes(b"PK\x03\x04 fake docx content")
    
    # Создаем manifest
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
            {
                "original_name": "document.docx",
                "current_name": "document.docx",
                "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "detected_type": "docx",
                "needs_ocr": False,
                "sha256": "test_hash_docx",
                "size": 200,
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


@pytest.fixture
def sample_archive_unit(temp_dir):
    """Создает UNIT с архивом."""
    unit_id = "UNIT_000456"
    unit_path = temp_dir / unit_id
    unit_path.mkdir(parents=True)

    # Создаем ZIP архив
    import zipfile
    zip_path = unit_path / "archive.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", "Content 1")
        zf.writestr("file2.txt", "Content 2")

    return unit_path


@pytest.fixture
def sample_archive_unit_alt(temp_dir):
    """Создает UNIT с архивом (альтернативный ID для избежания конфликтов)."""
    unit_id = "UNIT_000457"
    unit_path = temp_dir / unit_id
    unit_path.mkdir(parents=True)

    # Создаем ZIP архив
    import zipfile
    zip_path = unit_path / "archive.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", "Content 1")
        zf.writestr("file2.txt", "Content 2")

    return unit_path


@pytest.fixture
def sample_mixed_unit(temp_dir):
    """Создает UNIT со смешанными типами файлов."""
    unit_id = "UNIT_000789"
    unit_path = temp_dir / unit_id
    unit_path.mkdir(parents=True)
    
    # Создаем файлы разных типов
    (unit_path / "doc.pdf").write_bytes(b"%PDF-1.4 fake pdf")
    (unit_path / "image.jpg").write_bytes(b"\xff\xd8\xff fake jpg")
    (unit_path / "archive.zip").write_bytes(b"PK\x03\x04 fake zip")
    
    return unit_path


@pytest.fixture
def sample_input_structure(temp_dir):
    """Создает структуру Input директории с несколькими UNIT."""
    input_dir = temp_dir / "Input" / datetime.now().strftime("%Y-%m-%d")
    input_dir.mkdir(parents=True)
    
    # Создаем несколько UNIT
    for i in range(3):
        unit_id = f"UNIT_{i:06d}"
        unit_path = input_dir / unit_id
        unit_path.mkdir()
        (unit_path / f"file_{i}.pdf").write_bytes(b"%PDF-1.4 fake pdf")
    
    return input_dir


@pytest.fixture
def sample_pending_structure(temp_dir):
    """Создает структуру Pending директории."""
    date_str = datetime.now().strftime("%Y-%m-%d")
    pending_dir = temp_dir / "Processing" / date_str / "Pending_1"
    
    # Создаем поддиректории
    convert_dir = pending_dir / "convert" / "docx"
    extract_dir = pending_dir / "archives" / "zip"
    normalize_dir = pending_dir / "normalize" / "pdf"
    
    for dir_path in [convert_dir, extract_dir, normalize_dir]:
        dir_path.mkdir(parents=True)
    
    # Создаем UNIT в каждой поддиректории
    for subdir, unit_id in [(convert_dir, "UNIT_001"), (extract_dir, "UNIT_002"), (normalize_dir, "UNIT_003")]:
        unit_path = subdir / unit_id
        unit_path.mkdir()
        (unit_path / "test.txt").write_bytes(b"test content")
    
    return pending_dir


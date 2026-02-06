"""
Тесты для core/contract.py - генерация контрактов для Docling.
"""
import pytest
import json
import tempfile
from pathlib import Path

from docprep.core.contract import (
    generate_contract_from_manifest,
    save_contract,
    load_contract,
    calculate_file_checksum,
    estimate_processing_cost,
)


class TestEstimateProcessingCost:
    """Тесты для estimate_processing_cost."""

    def test_pdf_text_cost(self):
        """PDF text должен иметь низкую стоимость."""
        result = estimate_processing_cost("pdf_text", page_count=10)
        assert result["cpu_seconds_estimate"] > 0
        assert result["cost_usd_estimate"] >= 0

    def test_pdf_scan_cost_higher_than_text(self):
        """PDF scan должен быть дороже чем pdf_text."""
        text_cost = estimate_processing_cost("pdf_text", page_count=10)
        scan_cost = estimate_processing_cost("pdf_scan", page_count=10)
        assert scan_cost["cpu_seconds_estimate"] > text_cost["cpu_seconds_estimate"]

    def test_unknown_route_has_default_cost(self):
        """Неизвестный route должен иметь дефолтную стоимость."""
        result = estimate_processing_cost("unknown_route", page_count=1)
        assert result["cpu_seconds_estimate"] > 0


class TestCalculateFileChecksum:
    """Тесты для calculate_file_checksum."""

    def test_calculates_sha256(self, tmp_path):
        """Должен корректно вычислять SHA256."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, World!")

        checksum = calculate_file_checksum(test_file)

        assert len(checksum) == 64  # SHA256 hex = 64 символа
        assert checksum == "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"

    def test_returns_empty_for_nonexistent_file(self, tmp_path):
        """Должен вернуть пустую строку для несуществующего файла."""
        nonexistent = tmp_path / "nonexistent.txt"
        checksum = calculate_file_checksum(nonexistent)
        assert checksum == ""


class TestGenerateContractFromManifest:
    """Тесты для generate_contract_from_manifest."""

    @pytest.fixture
    def unit_with_xml(self, tmp_path):
        """Создает UNIT с XML файлом."""
        unit_dir = tmp_path / "UNIT_test_xml"
        unit_dir.mkdir()

        # Создаем XML файл
        xml_file = unit_dir / "settings.xml"
        xml_file.write_text('<?xml version="1.0"?><root></root>')

        # Создаем manifest
        manifest = {
            "unit_id": "UNIT_test_xml",
            "protocol_id": "P001",
            "protocol_date": "2025-03-04",
            "correlation_id": "test-correlation-id",
            "files": [
                {
                    "original_name": "settings.xml",
                    "current_name": "settings.xml",
                    "detected_type": "",  # Пустой detected_type
                    "mime_detected": "application/xml",
                    "pages_or_parts": 1,
                    "needs_ocr": False,
                    "transformations": [],
                }
            ],
            "processing": {
                "route": "xml",
                "current_cycle": 1,
            },
        }

        manifest_path = unit_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        return unit_dir, manifest

    @pytest.fixture
    def unit_with_xlsx(self, tmp_path):
        """Создает UNIT с XLSX файлом."""
        unit_dir = tmp_path / "UNIT_test_xlsx"
        unit_dir.mkdir()

        # Создаем фейковый XLSX файл
        xlsx_file = unit_dir / "data.xlsx"
        xlsx_file.write_bytes(b"PK\x03\x04")  # ZIP header (XLSX это ZIP)

        manifest = {
            "unit_id": "UNIT_test_xlsx",
            "protocol_id": "P002",
            "protocol_date": "2025-03-04",
            "correlation_id": "test-correlation-xlsx",
            "files": [
                {
                    "original_name": "data.xlsx",
                    "current_name": "data.xlsx",
                    "detected_type": "",  # Пустой detected_type
                    "mime_detected": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "pages_or_parts": 1,
                    "needs_ocr": False,
                    "transformations": [],
                }
            ],
            "processing": {
                "route": "xlsx",
                "current_cycle": 1,
            },
        }

        manifest_path = unit_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        return unit_dir, manifest

    @pytest.fixture
    def unit_with_jpeg(self, tmp_path):
        """Создает UNIT с JPEG файлом."""
        unit_dir = tmp_path / "UNIT_test_jpeg"
        unit_dir.mkdir()

        # Создаем фейковый JPEG
        jpeg_file = unit_dir / "photo.jpg"
        jpeg_file.write_bytes(b"\xff\xd8\xff\xe0")  # JPEG magic bytes

        manifest = {
            "unit_id": "UNIT_test_jpeg",
            "protocol_id": "P003",
            "protocol_date": "2025-03-04",
            "correlation_id": "test-correlation-jpeg",
            "files": [
                {
                    "original_name": "photo.jpg",
                    "current_name": "photo.jpg",
                    "detected_type": "",  # Пустой detected_type
                    "mime_detected": "image/jpeg",
                    "pages_or_parts": 1,
                    "needs_ocr": True,
                    "transformations": [],
                }
            ],
            "processing": {
                "route": "image_ocr",
                "current_cycle": 1,
            },
        }

        manifest_path = unit_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        return unit_dir, manifest

    @pytest.fixture
    def unit_with_docx(self, tmp_path):
        """Создает UNIT с DOCX файлом."""
        unit_dir = tmp_path / "UNIT_test_docx"
        unit_dir.mkdir()

        docx_file = unit_dir / "document.docx"
        docx_file.write_bytes(b"PK\x03\x04")

        manifest = {
            "unit_id": "UNIT_test_docx",
            "protocol_id": "P004",
            "protocol_date": "2025-03-04",
            "correlation_id": "test-correlation-docx",
            "files": [
                {
                    "original_name": "document.docx",
                    "current_name": "document.docx",
                    "detected_type": "docx",  # Корректный detected_type
                    "mime_detected": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    "pages_or_parts": 5,
                    "needs_ocr": False,
                    "transformations": [],
                }
            ],
            "processing": {
                "route": "docx",
                "current_cycle": 1,
            },
        }

        manifest_path = unit_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        return unit_dir, manifest

    def test_xml_document_type_not_pdf(self, unit_with_xml):
        """XML файл должен иметь document_type='xml', НЕ 'pdf'."""
        unit_dir, manifest = unit_with_xml

        contract = generate_contract_from_manifest(unit_dir, manifest)

        # Критическая проверка: document_type должен быть 'xml'
        assert contract["document_profile"]["document_type"] == "xml", \
            f"Expected document_type='xml', got '{contract['document_profile']['document_type']}'"
        assert contract["routing"]["docling_route"] == "xml"

    def test_xlsx_document_type_not_pdf(self, unit_with_xlsx):
        """XLSX файл должен иметь document_type='xlsx', НЕ 'pdf'."""
        unit_dir, manifest = unit_with_xlsx

        contract = generate_contract_from_manifest(unit_dir, manifest)

        assert contract["document_profile"]["document_type"] == "xlsx", \
            f"Expected document_type='xlsx', got '{contract['document_profile']['document_type']}'"
        assert contract["routing"]["docling_route"] == "xlsx"
        assert contract["document_profile"]["has_tables"] is True

    def test_jpeg_document_type_is_image(self, unit_with_jpeg):
        """JPEG файл должен иметь document_type='jpeg' или 'image'."""
        unit_dir, manifest = unit_with_jpeg

        contract = generate_contract_from_manifest(unit_dir, manifest)

        # Допускаем 'jpeg', 'jpg', или 'image'
        assert contract["document_profile"]["document_type"] in ["jpeg", "jpg", "image"], \
            f"Expected document_type in ['jpeg', 'jpg', 'image'], got '{contract['document_profile']['document_type']}'"
        assert contract["routing"]["docling_route"] == "image_ocr"
        assert contract["document_profile"]["needs_ocr"] is True

    def test_docx_with_detected_type_preserved(self, unit_with_docx):
        """Если detected_type задан, он должен сохраниться."""
        unit_dir, manifest = unit_with_docx

        contract = generate_contract_from_manifest(unit_dir, manifest)

        assert contract["document_profile"]["document_type"] == "docx"
        assert contract["routing"]["docling_route"] == "docx"

    def test_contract_has_required_fields(self, unit_with_docx):
        """Контракт должен содержать все обязательные поля."""
        unit_dir, manifest = unit_with_docx

        contract = generate_contract_from_manifest(unit_dir, manifest)

        # Проверяем структуру контракта
        assert "contract_version" in contract
        assert "unit" in contract
        assert "source" in contract
        assert "document_profile" in contract
        assert "routing" in contract
        assert "processing_constraints" in contract
        assert "history" in contract
        assert "cost_estimation" in contract

        # Проверяем вложенные поля
        assert contract["unit"]["state"] == "READY_FOR_DOCLING"
        assert contract["source"]["checksum_sha256"] != ""
        assert contract["routing"]["pipeline_version"] == "2025-01"


class TestSaveAndLoadContract:
    """Тесты для save_contract и load_contract."""

    def test_save_and_load_contract(self, tmp_path):
        """Должен корректно сохранять и загружать контракт."""
        unit_dir = tmp_path / "UNIT_test"
        unit_dir.mkdir()

        contract = {
            "contract_version": "1.0",
            "unit": {"unit_id": "UNIT_test", "state": "READY_FOR_DOCLING"},
            "document_profile": {"document_type": "pdf"},
        }

        # Сохраняем
        saved_path = save_contract(unit_dir, contract)
        assert saved_path.exists()
        assert saved_path.name == "docprep.contract.json"

        # Загружаем
        loaded = load_contract(unit_dir)
        assert loaded == contract

    def test_load_contract_not_found(self, tmp_path):
        """Должен выбросить FileNotFoundError если контракт не найден."""
        unit_dir = tmp_path / "UNIT_empty"
        unit_dir.mkdir()

        with pytest.raises(FileNotFoundError):
            load_contract(unit_dir)

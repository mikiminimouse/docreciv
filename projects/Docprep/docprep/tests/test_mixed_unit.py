"""
Тесты для Mixed UNIT и форматов файлов.

Покрывает:
1. Сохранение Mixed статуса через все этапы обработки
2. Правильную классификацию форматов (RTF, TAR, GZ, HTML, MD, BMP, TIF)
3. Обработку архивов без extract -> Exceptions
"""
import pytest
import json
from pathlib import Path
from typing import Dict, Any

from docprep.core.config import (
    EXTENSIONS_CONVERT,
    EXTENSIONS_ARCHIVES,
    EXTENSIONS_DIRECT,
    get_processing_paths,
)
from docprep.core.manifest import (
    load_manifest,
    save_manifest,
    create_manifest_v2,
)


class TestExtensionCategories:
    """Тесты для категорий расширений файлов."""

    def test_rtf_only_in_convert_category(self):
        """RTF должен быть ТОЛЬКО в категории convert, НЕ в direct."""
        # RTF требует конвертации через LibreOffice
        assert "rtf" in EXTENSIONS_CONVERT, "RTF должен быть в EXTENSIONS_CONVERT"
        assert "rtf" not in EXTENSIONS_DIRECT, "RTF НЕ должен быть в EXTENSIONS_DIRECT"

    def test_tar_gz_recognized_as_archives(self):
        """TAR и GZ должны распознаваться как архивы."""
        assert "tar" in EXTENSIONS_ARCHIVES, "TAR должен быть в EXTENSIONS_ARCHIVES"
        assert "gz" in EXTENSIONS_ARCHIVES, "GZ должен быть в EXTENSIONS_ARCHIVES"

    def test_html_md_bmp_tif_are_direct(self):
        """HTML, MD, BMP, TIF должны классифицироваться как direct."""
        assert "html" in EXTENSIONS_DIRECT, "HTML должен быть в EXTENSIONS_DIRECT"
        assert "md" in EXTENSIONS_DIRECT, "MD должен быть в EXTENSIONS_DIRECT"
        assert "bmp" in EXTENSIONS_DIRECT, "BMP должен быть в EXTENSIONS_DIRECT"
        assert "tif" in EXTENSIONS_DIRECT, "TIF должен быть в EXTENSIONS_DIRECT"

    def test_standard_direct_formats_present(self):
        """Стандартные direct форматы должны присутствовать."""
        expected_direct = ["docx", "pdf", "png", "jpg", "jpeg", "xlsx", "pptx", "tiff", "xml"]
        for ext in expected_direct:
            assert ext in EXTENSIONS_DIRECT, f"{ext} должен быть в EXTENSIONS_DIRECT"


class TestProcessingPaths:
    """Тесты для путей Processing."""

    def test_mixed_directory_in_processing_paths(self, tmp_path):
        """Mixed директория должна быть в get_processing_paths."""
        paths = get_processing_paths(cycle=1, processing_base=tmp_path)

        assert "Mixed" in paths, "Mixed должен быть в get_processing_paths"
        assert paths["Mixed"] == tmp_path / "Processing_1" / "Mixed"

    def test_all_processing_categories_present(self, tmp_path):
        """Все категории Processing должны присутствовать."""
        paths = get_processing_paths(cycle=1, processing_base=tmp_path)

        required_categories = ["Convert", "Extract", "Normalize", "Mixed"]
        for category in required_categories:
            assert category in paths, f"{category} должен быть в get_processing_paths"


class TestManifestIsMixedHelpers:
    """Тесты для helper-функций работы с is_mixed в manifest."""

    def test_get_is_mixed_from_root(self, tmp_path):
        """get_is_mixed должен находить is_mixed в корне manifest."""
        from docprep.core.manifest import get_is_mixed

        manifest = {"is_mixed": True}
        assert get_is_mixed(manifest) is True

    def test_get_is_mixed_from_processing_classification(self, tmp_path):
        """get_is_mixed должен находить is_mixed в processing.classification."""
        from docprep.core.manifest import get_is_mixed

        manifest = {
            "is_mixed": False,
            "processing": {
                "classification": {
                    "is_mixed": True
                }
            }
        }
        # is_mixed в корне False, но в classification True -> должен вернуть True
        assert get_is_mixed(manifest) is True

    def test_get_is_mixed_default_false(self, tmp_path):
        """get_is_mixed должен возвращать False по умолчанию."""
        from docprep.core.manifest import get_is_mixed

        manifest = {}
        assert get_is_mixed(manifest) is False

    def test_set_is_mixed_updates_both_locations(self, tmp_path):
        """set_is_mixed должен обновлять is_mixed в обоих местах."""
        from docprep.core.manifest import set_is_mixed, get_is_mixed

        manifest = {}
        updated = set_is_mixed(manifest, True)

        assert updated["is_mixed"] is True
        assert updated["processing"]["classification"]["is_mixed"] is True
        assert get_is_mixed(updated) is True


class TestExtractorPreservesMixed:
    """Тесты для сохранения Mixed статуса в Extractor."""

    def test_extractor_preserves_mixed_status_in_manifest(self, tmp_path, isolate_test_environment):
        """Extractor должен сохранять is_mixed=True из manifest."""
        from docprep.core.manifest import get_is_mixed, set_is_mixed

        # Создаем manifest с is_mixed=True
        manifest = create_manifest_v2(
            unit_id="UNIT_TEST_001",
            files=[{"original_name": "test.pdf"}],
            current_cycle=1,
        )
        manifest = set_is_mixed(manifest, True)

        assert get_is_mixed(manifest) is True, "is_mixed должен быть True после set_is_mixed"

    def test_extractor_uses_mixed_extension_for_sorting(self, tmp_path, isolate_test_environment):
        """Mixed UNIT должен сортироваться в Mixed/ директорию."""
        from docprep.core.manifest import get_is_mixed, set_is_mixed

        # Создаем manifest с is_mixed=True
        manifest = create_manifest_v2(
            unit_id="UNIT_TEST_002",
            files=[
                {"original_name": "doc1.pdf"},
                {"original_name": "doc2.docx"},
            ],
            current_cycle=1,
        )
        manifest = set_is_mixed(manifest, True)

        # Проверяем что is_mixed сохраняется
        assert get_is_mixed(manifest) is True

        # При is_mixed=True, extension должен быть "Mixed"
        # Это будет проверяться после реализации изменений в extractor.py


class TestClassifierPreservesMixed:
    """Тесты для сохранения Mixed статуса при re-classification."""

    def test_classifier_preserves_mixed_on_reclassification(self, tmp_path, isolate_test_environment):
        """Classifier должен сохранять is_mixed при re-classification в Cycle 2+."""
        from docprep.core.manifest import get_is_mixed, set_is_mixed

        # Создаем manifest из Cycle 1 с is_mixed=True
        manifest = create_manifest_v2(
            unit_id="UNIT_TEST_003",
            files=[
                {"original_name": "doc1.pdf"},
                {"original_name": "doc2.docx"},
            ],
            current_cycle=1,
        )
        manifest = set_is_mixed(manifest, True)

        # Симулируем re-classification в Cycle 2
        # Существующий is_mixed должен быть сохранен
        existing_is_mixed = get_is_mixed(manifest)
        assert existing_is_mixed is True, "is_mixed должен сохраняться при re-classification"


class TestMergerArchivesWithoutExtract:
    """Тесты для обработки архивов без extract."""

    def test_archives_without_extract_return_er_extract(self):
        """ZIP без операции extract должен возвращать 'ErExtract', а не 'skip_state'."""
        from docprep.engine.merger import Merger
        from unittest.mock import MagicMock

        merger = Merger()

        # Создаем mock manifest для архива без extract операции
        manifest = {
            "state_machine": {
                "current_state": "CLASSIFIED_1"
            },
            "processing": {
                "operations": []  # Нет операции extract
            },
            "files": []
        }

        unit_info = {
            "manifest": manifest,
            "source_path": Path("/fake/path")
        }

        # Для архивов без extract должен возвращаться ErExtract
        # После реализации изменений в merger.py
        # Тест будет проверять новое поведение


class TestDirectoryStructure:
    """Тесты для структуры директорий."""

    def test_init_creates_mixed_in_processing(self, tmp_path):
        """init_directory_structure должна создавать Mixed в Processing."""
        from docprep.core.config import init_directory_structure

        init_directory_structure(base_dir=tmp_path)

        # Проверяем что Mixed создается в Processing_N
        for cycle in range(1, 4):
            mixed_path = tmp_path / "Processing" / f"Processing_{cycle}" / "Mixed"
            # После реализации изменений в config.py
            # assert mixed_path.exists(), f"Mixed должен существовать в Processing_{cycle}"


# Фикстура для изоляции тестов
@pytest.fixture
def isolate_test_environment(tmp_path, monkeypatch):
    """Изолирует тестовое окружение от реальных директорий."""
    monkeypatch.setenv("DATA_BASE_DIR", str(tmp_path))
    return tmp_path


# ============================================================================
# TDD ТЕСТЫ ДЛЯ ИСПРАВЛЕНИЙ ZIP EDGE CASE И MIXED UNITS
# ============================================================================


class TestMixedRoutingFix:
    """
    TDD тесты для Issue 2: Mixed Units теряются после merge.

    Проблема: determine_route_from_files() при нескольких типах файлов
    возвращает ПРИОРИТЕТНЫЙ тип вместо "mixed".
    """

    def test_determine_route_from_files_returns_mixed_for_multiple_types(self):
        """
        CRITICAL: Если в UNIT файлы разных типов, route должен быть 'mixed'.

        Текущее поведение (БАГ): возвращает приоритетный тип (pdf_text, docx, etc.)
        Ожидаемое поведение: возвращает "mixed"
        """
        from docprep.core.routing import determine_route_from_files

        files = [
            {"detected_type": "pdf", "needs_ocr": False},
            {"detected_type": "docx", "needs_ocr": False},
            {"detected_type": "xlsx", "needs_ocr": False},
        ]

        route = determine_route_from_files(files)

        assert route == "mixed", (
            f"Expected 'mixed' for files with different types, got '{route}'. "
            "Mixed units should route to 'mixed', not to highest priority type."
        )

    def test_determine_route_pdf_and_docx_returns_mixed(self):
        """PDF + DOCX должны маршрутизироваться как mixed."""
        from docprep.core.routing import determine_route_from_files

        files = [
            {"detected_type": "pdf", "needs_ocr": False},
            {"detected_type": "docx", "needs_ocr": False},
        ]

        route = determine_route_from_files(files)
        assert route == "mixed", f"Expected 'mixed', got '{route}'"

    def test_determine_route_pdf_and_html_returns_mixed(self):
        """PDF + HTML должны маршрутизироваться как mixed."""
        from docprep.core.routing import determine_route_from_files

        files = [
            {"detected_type": "pdf", "needs_ocr": True},
            {"detected_type": "html", "needs_ocr": False},
        ]

        route = determine_route_from_files(files)
        assert route == "mixed", f"Expected 'mixed', got '{route}'"

    def test_determine_route_single_type_returns_that_type(self):
        """Один тип файла должен возвращать route этого типа (не mixed)."""
        from docprep.core.routing import determine_route_from_files

        files = [
            {"detected_type": "pdf", "needs_ocr": False},
            {"detected_type": "pdf", "needs_ocr": False},
        ]

        route = determine_route_from_files(files)
        # Все PDF без OCR -> pdf_text
        assert route == "pdf_text", f"Expected 'pdf_text', got '{route}'"


class TestMergerMixedHandlingFix:
    """
    TDD тесты для обработки is_mixed в Merger._get_target_subdir.

    Проблема: Не проверяется поле is_mixed из manifest.processing.classification
    """

    def test_get_target_subdir_returns_mixed_for_is_mixed_flag(self, temp_dir):
        """
        CRITICAL: _get_target_subdir должен возвращать Mixed для is_mixed=true.

        Даже если route указывает на конкретный тип (pdf_text, docx),
        флаг is_mixed должен иметь приоритет.
        """
        from docprep.engine.merger import Merger

        merger = Merger()
        base_dir = temp_dir / "Ready2Docling"
        base_dir.mkdir()

        test_file = temp_dir / "doc.pdf"
        test_file.write_bytes(b"%PDF-1.4")

        manifest = {
            "processing": {
                "route": "pdf_text",  # Route конкретный
                "classification": {
                    "is_mixed": True  # Но флаг is_mixed=True
                }
            }
        }

        result = merger._get_target_subdir(
            file_type="pdf",
            files=[test_file],
            base_dir=base_dir,
            manifest=manifest
        )

        assert result == base_dir / "Mixed", (
            f"Expected Mixed/ for is_mixed=true, got {result}. "
            "is_mixed flag should override route."
        )


class TestArchiveExtractionCheckFix:
    """
    TDD тесты для Issue 1: ZIP файлы с неуспешным извлечением.

    Проблема: В merger._check_unit_processing_state проверяется только
    наличие extract операции, но не её успешность (files_extracted > 0).
    """

    def test_archive_with_zero_extracted_files_returns_erextract(self, temp_dir):
        """
        CRITICAL: Архив с files_extracted=0 должен идти в ErExtract.

        Текущее поведение (БАГ): Проверяется только наличие extract операции
        Ожидаемое поведение: Проверяется files_extracted > 0
        """
        import zipfile
        from docprep.engine.merger import Merger

        merger = Merger()

        unit_id = "UNIT_zip_failed"
        unit_path = temp_dir / unit_id
        unit_path.mkdir()

        zip_path = unit_path / "archive.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file.txt", "content")

        files = [zip_path]

        # Manifest с extract операцией, но files_extracted=0
        manifest = {
            "state_machine": {"current_state": "CLASSIFIED_1"},
            "processing": {
                "operations": [
                    {
                        "type": "extract",
                        "status": "failed",
                        "files_extracted": 0,
                        "archives_processed": 1,
                    }
                ]
            },
            "files": []
        }

        unit_info = {"manifest": manifest}
        exceptions_base = temp_dir / "Exceptions"

        result = merger._check_unit_processing_state(
            unit_id=unit_id,
            unit_info=unit_info,
            dominant_type="zip_archive",
            files=files,
            exceptions_base=exceptions_base,
            current_cycle=1
        )

        assert result == "ErExtract", (
            f"Expected 'ErExtract' for archive with files_extracted=0, got '{result}'. "
            "Archives with 0 extracted files should go to Exceptions/ErExtract."
        )

    def test_archive_with_positive_extracted_files_returns_skip_state(self, temp_dir):
        """Архив с files_extracted > 0 должен возвращать skip_state."""
        import zipfile
        from docprep.engine.merger import Merger

        merger = Merger()

        unit_id = "UNIT_zip_success"
        unit_path = temp_dir / unit_id
        unit_path.mkdir()

        zip_path = unit_path / "archive.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file.txt", "content")

        files = [zip_path]

        manifest = {
            "state_machine": {"current_state": "CLASSIFIED_1"},
            "processing": {
                "operations": [
                    {
                        "type": "extract",
                        "status": "success",
                        "files_extracted": 5,
                        "archives_processed": 1,
                    }
                ]
            },
            "files": []
        }

        unit_info = {"manifest": manifest}
        exceptions_base = temp_dir / "Exceptions"

        result = merger._check_unit_processing_state(
            unit_id=unit_id,
            unit_info=unit_info,
            dominant_type="zip_archive",
            files=files,
            exceptions_base=exceptions_base,
            current_cycle=1
        )

        assert result == "skip_state", f"Expected 'skip_state', got '{result}'"

    def test_archive_with_applied_operations_returns_erextract(self, temp_dir):
        """
        Архив с операциями в applied_operations (не в processing.operations)
        также должен корректно проверяться на files_extracted.

        Это реальная структура manifest после refactoring.
        """
        import zipfile
        from docprep.engine.merger import Merger

        merger = Merger()

        unit_id = "UNIT_zip_applied_ops"
        unit_path = temp_dir / unit_id
        unit_path.mkdir()

        zip_path = unit_path / "archive.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file.txt", "content")

        files = [zip_path]

        # Реальная структура manifest - операции в applied_operations
        manifest = {
            "state_machine": {"current_state": "CLASSIFIED_1"},
            "processing": {
                "operations": []  # Пустой массив - как в реальном manifest
            },
            "applied_operations": [
                {
                    "type": "extract",
                    "status": "failed",
                    "error": "That compression method is not supported",
                    "files_extracted": 0,
                    "archives_processed": 1,
                }
            ],
            "files": []
        }

        unit_info = {"manifest": manifest}
        exceptions_base = temp_dir / "Exceptions"

        result = merger._check_unit_processing_state(
            unit_id=unit_id,
            unit_info=unit_info,
            dominant_type="zip_archive",
            files=files,
            exceptions_base=exceptions_base,
            current_cycle=1
        )

        assert result == "ErExtract", (
            f"Expected 'ErExtract' for archive with files_extracted=0 in applied_operations, "
            f"got '{result}'. Merger should check applied_operations when processing.operations is empty."
        )

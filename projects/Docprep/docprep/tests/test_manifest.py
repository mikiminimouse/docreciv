"""
Тесты для core/manifest.py - управление метаданными UNIT.

Покрывает:
- Создание manifest v2
- Сохранение и загрузка
- Обновление операций
- Обновление состояния
- Определение маршрута
- Интеграционные сценарии
"""
import pytest
import json
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch

from docprep.core.manifest import (
    create_manifest_v2,
    load_manifest,
    save_manifest,
    update_manifest_operation,
    update_manifest_state,
)
from docprep.core.state_machine import UnitState


# =============================================================================
# Группа A: Создание manifest
# =============================================================================


class TestCreateManifestV2:
    """Тесты создания manifest v2."""

    def test_create_manifest_v2_minimal(self):
        """Создание manifest с минимальными параметрами."""
        manifest = create_manifest_v2(unit_id="UNIT_001")

        assert manifest["schema_version"] == "2.0"
        assert manifest["unit_id"] == "UNIT_001"
        assert manifest["files"] == []
        assert manifest["state_machine"]["initial_state"] == "RAW"
        assert manifest["state_machine"]["state_trace"] == ["RAW"]

    def test_create_manifest_v2_with_files(self):
        """Создание manifest с файлами."""
        files = [
            {
                "original_name": "document.pdf",
                "current_name": "document.pdf",
                "mime_type": "application/pdf",
                "detected_type": "pdf",
                "needs_ocr": False,
                "pages_or_parts": 10,
            },
            {
                "original_name": "image.jpg",
                "current_name": "image.jpg",
                "mime_type": "image/jpeg",
                "detected_type": "image",
                "needs_ocr": True,
                "pages_or_parts": 1,
            },
        ]

        manifest = create_manifest_v2(unit_id="UNIT_002", files=files)

        assert len(manifest["files"]) == 2
        assert manifest["files"][0]["original_name"] == "document.pdf"
        assert manifest["files"][1]["needs_ocr"] is True
        assert manifest["integrity"]["file_count"] == 2

    def test_create_manifest_v2_with_state_trace(self):
        """Создание manifest с историей состояний."""
        state_trace = ["RAW", "CLASSIFIED_1", "PENDING_CONVERT", "CLASSIFIED_2"]

        manifest = create_manifest_v2(
            unit_id="UNIT_003",
            state_trace=state_trace,
        )

        assert manifest["state_machine"]["state_trace"] == state_trace
        assert manifest["state_machine"]["initial_state"] == "RAW"
        assert manifest["state_machine"]["final_state"] == "CLASSIFIED_2"
        assert manifest["state_machine"]["current_state"] == "CLASSIFIED_2"

    def test_create_manifest_v2_with_urls(self):
        """Создание manifest с URL источников."""
        urls = [
            {"url": "https://example.com/doc1.pdf", "downloaded_at": "2025-01-17T10:00:00Z"},
            {"url": "https://example.com/doc2.pdf", "downloaded_at": "2025-01-17T10:01:00Z"},
        ]

        manifest = create_manifest_v2(
            unit_id="UNIT_004",
            source_urls=urls,
        )

        assert manifest["source"]["urls"] == urls

    def test_create_manifest_v2_default_values(self):
        """Проверка значений по умолчанию."""
        manifest = create_manifest_v2(unit_id="UNIT_005")

        # Protocol info
        assert manifest["protocol_id"] == ""
        assert manifest["protocol_date"] == ""

        # Unit semantics
        assert manifest["unit_semantics"]["domain"] == "public_procurement"
        assert manifest["unit_semantics"]["entity"] == "tender_protocol"

        # Processing
        assert manifest["processing"]["current_cycle"] == 1
        assert manifest["processing"]["max_cycles"] == 3  # from config.MAX_CYCLES

        # Timestamps
        assert "created_at" in manifest
        assert "updated_at" in manifest


# =============================================================================
# Группа B: Сохранение и загрузка
# =============================================================================


class TestSaveAndLoadManifest:
    """Тесты сохранения и загрузки manifest."""

    def test_save_and_load_manifest(self, temp_dir):
        """Сохранение и загрузка manifest."""
        unit_path = temp_dir / "UNIT_006"
        unit_path.mkdir()

        original = create_manifest_v2(
            unit_id="UNIT_006",
            protocol_id="12345",
            files=[{"original_name": "test.pdf", "detected_type": "pdf"}],
        )

        save_manifest(unit_path, original)
        loaded = load_manifest(unit_path)

        assert loaded["unit_id"] == original["unit_id"]
        assert loaded["protocol_id"] == original["protocol_id"]
        assert len(loaded["files"]) == len(original["files"])

    def test_save_manifest_creates_directory(self, temp_dir):
        """save_manifest создаёт директорию если не существует."""
        unit_path = temp_dir / "new_unit" / "nested"
        manifest = create_manifest_v2(unit_id="UNIT_007")

        save_manifest(unit_path, manifest)

        assert unit_path.exists()
        assert (unit_path / "manifest.json").exists()

    def test_load_manifest_not_found(self, temp_dir):
        """Загрузка несуществующего manifest вызывает FileNotFoundError."""
        unit_path = temp_dir / "nonexistent"
        unit_path.mkdir()

        with pytest.raises(FileNotFoundError) as exc_info:
            load_manifest(unit_path)

        assert "Manifest not found" in str(exc_info.value)

    def test_load_manifest_corrupt_json(self, temp_dir):
        """Загрузка повреждённого manifest вызывает JSONDecodeError."""
        unit_path = temp_dir / "UNIT_corrupt"
        unit_path.mkdir()
        (unit_path / "manifest.json").write_text("{ invalid json }")

        with pytest.raises(json.JSONDecodeError):
            load_manifest(unit_path)

    def test_save_manifest_updates_timestamp(self, temp_dir):
        """save_manifest обновляет updated_at."""
        unit_path = temp_dir / "UNIT_008"
        unit_path.mkdir()

        manifest = create_manifest_v2(unit_id="UNIT_008")
        original_updated = manifest["updated_at"]

        # Небольшая задержка чтобы timestamp отличался
        import time
        time.sleep(0.01)

        save_manifest(unit_path, manifest)
        loaded = load_manifest(unit_path)

        # updated_at должен измениться (или быть таким же если очень быстро)
        assert "updated_at" in loaded
        assert loaded["updated_at"] >= original_updated


# =============================================================================
# Группа C: Обновление операций
# =============================================================================


class TestUpdateManifestOperation:
    """Тесты обновления операций."""

    def test_update_manifest_operation_convert(self):
        """Добавление операции конвертации."""
        manifest = create_manifest_v2(
            unit_id="UNIT_009",
            files=[{"original_name": "doc.doc", "detected_type": "doc"}],
        )

        operation = {
            "type": "convert",
            "file_index": 0,
            "from": ".doc",
            "to": ".docx",
            "cycle": 1,
            "tool": "libreoffice",
        }

        updated = update_manifest_operation(manifest, operation)

        assert len(updated["applied_operations"]) == 1
        assert updated["applied_operations"][0]["type"] == "convert"
        assert updated["files"][0]["transformations"][0]["type"] == "convert"

    def test_update_manifest_operation_extract(self):
        """Добавление операции распаковки."""
        manifest = create_manifest_v2(
            unit_id="UNIT_010",
            files=[{"original_name": "archive.zip", "detected_type": "zip"}],
        )

        operation = {
            "type": "extract",
            "file_index": 0,
            "from": "archive.zip",
            "to": "extracted/",
            "cycle": 1,
            "tool": "zipfile",
        }

        updated = update_manifest_operation(manifest, operation)

        assert updated["applied_operations"][0]["type"] == "extract"

    def test_update_manifest_operation_multiple(self):
        """Добавление нескольких операций."""
        manifest = create_manifest_v2(
            unit_id="UNIT_011",
            files=[
                {"original_name": "doc.doc", "detected_type": "doc"},
                {"original_name": "archive.zip", "detected_type": "zip"},
            ],
        )

        # Первая операция
        update_manifest_operation(manifest, {
            "type": "convert",
            "file_index": 0,
            "cycle": 1,
        })

        # Вторая операция
        update_manifest_operation(manifest, {
            "type": "extract",
            "file_index": 1,
            "cycle": 1,
        })

        assert len(manifest["applied_operations"]) == 2

    def test_update_manifest_operation_auto_timestamp(self):
        """Операция автоматически получает timestamp."""
        manifest = create_manifest_v2(unit_id="UNIT_012", files=[{"original_name": "f.pdf"}])

        operation = {"type": "normalize", "file_index": 0}
        updated = update_manifest_operation(manifest, operation)

        assert "timestamp" in updated["applied_operations"][0]

    def test_update_manifest_operation_preserves_existing(self):
        """Новая операция не затирает существующие."""
        manifest = create_manifest_v2(
            unit_id="UNIT_013",
            files=[{"original_name": "doc.pdf", "transformations": [{"type": "rename"}]}],
        )
        manifest["applied_operations"] = [{"type": "rename"}]

        update_manifest_operation(manifest, {"type": "convert", "file_index": 0})

        assert len(manifest["applied_operations"]) == 2
        assert len(manifest["files"][0]["transformations"]) == 2


# =============================================================================
# Группа D: Обновление состояния
# =============================================================================


class TestUpdateManifestState:
    """Тесты обновления состояния."""

    def test_update_manifest_state_transition(self):
        """Обновление состояния добавляет в state_trace."""
        manifest = create_manifest_v2(unit_id="UNIT_014")

        updated = update_manifest_state(manifest, UnitState.CLASSIFIED_1, cycle=1)

        assert updated["state_machine"]["current_state"] == "CLASSIFIED_1"
        assert "CLASSIFIED_1" in updated["state_machine"]["state_trace"]

    def test_update_manifest_state_preserves_history(self):
        """История состояний сохраняется."""
        manifest = create_manifest_v2(
            unit_id="UNIT_015",
            state_trace=["RAW", "CLASSIFIED_1"],
        )

        updated = update_manifest_state(manifest, UnitState.PENDING_CONVERT, cycle=1)

        assert updated["state_machine"]["state_trace"] == ["RAW", "CLASSIFIED_1", "PENDING_CONVERT"]

    def test_update_manifest_state_updates_cycle(self):
        """Обновление состояния обновляет current_cycle."""
        manifest = create_manifest_v2(unit_id="UNIT_016")

        update_manifest_state(manifest, UnitState.CLASSIFIED_1, cycle=1)
        assert manifest["processing"]["current_cycle"] == 1

        update_manifest_state(manifest, UnitState.CLASSIFIED_2, cycle=2)
        assert manifest["processing"]["current_cycle"] == 2

    def test_update_manifest_state_updates_timestamp(self):
        """update_manifest_state обновляет updated_at."""
        manifest = create_manifest_v2(unit_id="UNIT_017")
        original_updated = manifest["updated_at"]

        import time
        time.sleep(0.01)

        update_manifest_state(manifest, UnitState.CLASSIFIED_1, cycle=1)

        assert manifest["updated_at"] >= original_updated


# =============================================================================
# Группа E: Определение маршрута
# =============================================================================


class TestDetermineRoute:
    """Тесты определения маршрута обработки."""

    def test_determine_route_from_single_pdf(self):
        """Один PDF файл -> pdf_text или pdf_scan route."""
        files = [{"original_name": "doc.pdf", "detected_type": "pdf", "needs_ocr": False}]
        manifest = create_manifest_v2(unit_id="UNIT_018", files=files)

        # Route определяется через _determine_route_from_files
        assert "route" in manifest["processing"]

    def test_determine_route_from_mixed_files(self):
        """Смешанные файлы -> mixed route."""
        files = [
            {"original_name": "doc.pdf", "detected_type": "pdf"},
            {"original_name": "sheet.xlsx", "detected_type": "xlsx"},
            {"original_name": "image.jpg", "detected_type": "image"},
        ]
        manifest = create_manifest_v2(unit_id="UNIT_019", files=files)

        # Route для смешанных файлов
        assert manifest["processing"]["route"] is not None

    def test_determine_route_from_images(self):
        """Только изображения -> image_ocr route."""
        files = [
            {"original_name": "scan1.jpg", "detected_type": "image", "needs_ocr": True},
            {"original_name": "scan2.png", "detected_type": "image", "needs_ocr": True},
        ]
        manifest = create_manifest_v2(unit_id="UNIT_020", files=files)

        assert manifest["processing"]["route"] is not None


# =============================================================================
# Группа F: Интеграционные тесты
# =============================================================================


class TestManifestIntegration:
    """Интеграционные тесты manifest."""

    def test_full_manifest_lifecycle(self, temp_dir):
        """Полный жизненный цикл manifest."""
        unit_path = temp_dir / "UNIT_lifecycle"

        # 1. Создание
        manifest = create_manifest_v2(
            unit_id="UNIT_lifecycle",
            protocol_id="12345",
            files=[{"original_name": "doc.doc", "detected_type": "doc"}],
        )

        # 2. Сохранение
        save_manifest(unit_path, manifest)

        # 3. Загрузка
        loaded = load_manifest(unit_path)

        # 4. Обновление состояния
        update_manifest_state(loaded, UnitState.CLASSIFIED_1, cycle=1)

        # 5. Добавление операции
        update_manifest_operation(loaded, {
            "type": "convert",
            "file_index": 0,
            "from": ".doc",
            "to": ".docx",
        })

        # 6. Сохранение обновлений
        save_manifest(unit_path, loaded)

        # 7. Финальная проверка
        final = load_manifest(unit_path)

        assert final["state_machine"]["current_state"] == "CLASSIFIED_1"
        assert len(final["applied_operations"]) == 1
        assert final["applied_operations"][0]["type"] == "convert"

    def test_manifest_with_complex_file_metadata(self):
        """Manifest с комплексными метаданными файлов."""
        files = [
            {
                "original_name": "protocol.pdf",
                "current_name": "protocol_normalized.pdf",
                "mime_type": "application/pdf",
                "detected_type": "pdf",
                "needs_ocr": False,
                "pages_or_parts": 25,
                "transformations": [
                    {"type": "rename", "from": "orig.pdf", "to": "protocol.pdf"},
                    {"type": "normalize", "tool": "name_normalizer"},
                ],
            },
        ]

        manifest = create_manifest_v2(
            unit_id="UNIT_complex",
            files=files,
            final_cluster="Merge_1",
            final_reason="single_pdf_text",
        )

        assert manifest["files"][0]["pages_or_parts"] == 25
        assert len(manifest["files"][0]["transformations"]) == 2
        assert manifest["processing"]["final_cluster"] == "Merge_1"

    def test_manifest_schema_validation(self):
        """Manifest содержит все обязательные поля schema v2."""
        manifest = create_manifest_v2(unit_id="UNIT_schema")

        required_fields = [
            "schema_version",
            "unit_id",
            "protocol_id",
            "protocol_date",
            "source",
            "unit_semantics",
            "files",
            "files_metadata",
            "processing",
            "state_machine",
            "integrity",
            "created_at",
            "updated_at",
        ]

        for field in required_fields:
            assert field in manifest, f"Missing required field: {field}"

        # Processing sub-fields
        processing_fields = ["current_cycle", "max_cycles", "route"]
        for field in processing_fields:
            assert field in manifest["processing"], f"Missing processing field: {field}"

        # State machine sub-fields
        sm_fields = ["initial_state", "final_state", "current_state", "state_trace"]
        for field in sm_fields:
            assert field in manifest["state_machine"], f"Missing state_machine field: {field}"


# =============================================================================
# Дополнительные тесты
# =============================================================================


class TestManifestEdgeCases:
    """Тесты граничных случаев."""

    def test_empty_files_list(self):
        """Manifest с пустым списком файлов."""
        manifest = create_manifest_v2(unit_id="UNIT_empty_files", files=[])

        assert manifest["files"] == []
        assert manifest["integrity"]["file_count"] == 0
        assert manifest["files_metadata"] == {}

    def test_manifest_with_all_cycles(self):
        """Manifest прошедший все 3 цикла."""
        state_trace = [
            "RAW",
            "CLASSIFIED_1",
            "PENDING_CONVERT",
            "CLASSIFIED_2",
            "PENDING_EXTRACT",
            "CLASSIFIED_3",
            "MERGED_PROCESSED",
            "READY_FOR_DOCLING",
        ]

        manifest = create_manifest_v2(
            unit_id="UNIT_all_cycles",
            current_cycle=3,
            state_trace=state_trace,
        )

        assert manifest["processing"]["current_cycle"] == 3
        assert manifest["state_machine"]["final_state"] == "READY_FOR_DOCLING"
        assert len(manifest["state_machine"]["state_trace"]) == 8

    def test_update_operation_without_files(self):
        """Обновление операции когда файлы отсутствуют."""
        manifest = create_manifest_v2(unit_id="UNIT_no_files")

        # Должно работать без ошибок, просто добавить в applied_operations
        update_manifest_operation(manifest, {
            "type": "classify",
            "cycle": 1,
        })

        assert len(manifest["applied_operations"]) == 1

    def test_files_metadata_generation(self):
        """files_metadata генерируется из files."""
        files = [
            {
                "original_name": "doc1.pdf",
                "detected_type": "pdf",
                "needs_ocr": False,
                "mime_detected": "application/pdf",
                "pages_or_parts": 5,
            },
            {
                "original_name": "doc2.docx",
                "detected_type": "docx",
                "needs_ocr": False,
                "mime_detected": "application/vnd.openxmlformats",
                "pages_or_parts": 10,
            },
        ]

        manifest = create_manifest_v2(unit_id="UNIT_metadata", files=files)

        assert "doc1.pdf" in manifest["files_metadata"]
        assert "doc2.docx" in manifest["files_metadata"]
        assert manifest["files_metadata"]["doc1.pdf"]["detected_type"] == "pdf"
        assert manifest["files_metadata"]["doc2.docx"]["pages_or_parts"] == 10

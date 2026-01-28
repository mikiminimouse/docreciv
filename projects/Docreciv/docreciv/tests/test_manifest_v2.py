"""
Тесты для manifest.py - валидация manifest v2 согласно PRD раздел 14.
"""
import pytest
import json
from datetime import datetime
import sys
from pathlib import Path

# Добавляем путь к модулю
sys.path.insert(0, str(Path(__file__).parent.parent))

from router.manifest import create_manifest_v2, update_manifest_v2


class TestManifestV2:
    """Тесты для manifest v2."""
    
    def test_create_manifest_v2_structure(self):
        """Тест структуры manifest v2 согласно PRD раздел 14.2."""
        manifest = create_manifest_v2(
            unit_id="UNIT_TEST",
            protocol_id="PROTOCOL_123",
            protocol_date="2025-03-18",  # Дата протокола из БД
            files=[{
                "original_name": "test.doc",
                "current_name": "test.docx",
                "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "detected_type": "docx",
                "needs_ocr": False,
                "transformations": [{
                    "type": "convert",
                    "from": "doc",
                    "to": "docx",
                    "cycle": 1,
                    "tool": "libreoffice",
                    "timestamp": datetime.utcnow().isoformat()
                }]
            }],
            current_cycle=2,
            state_trace=["RAW_INPUT", "CLASSIFIED_1", "PENDING_1", "CLASSIFIED_2"]
        )
        
        # Проверяем обязательные поля
        assert manifest["schema_version"] == "2.0"
        assert manifest["unit_id"] == "UNIT_TEST"
        assert manifest["protocol_id"] == "PROTOCOL_123"
        assert manifest["protocol_date"] == "2025-03-18"  # Дата протокола из БД
        
        # Проверяем секцию processing
        assert "processing" in manifest
        assert manifest["processing"]["current_cycle"] == 2
        assert manifest["processing"]["max_cycles"] == 3
        
        # Проверяем секцию state_machine
        assert "state_machine" in manifest
        assert "initial_state" in manifest["state_machine"]
        assert "final_state" in manifest["state_machine"]
        assert "state_trace" in manifest["state_machine"]
        assert manifest["state_machine"]["state_trace"] == ["RAW_INPUT", "CLASSIFIED_1", "PENDING_1", "CLASSIFIED_2"]
        
        # Проверяем секцию files
        assert "files" in manifest
        assert len(manifest["files"]) == 1
        assert "transformations" in manifest["files"][0]
    
    def test_manifest_v2_with_unit_semantics(self):
        """Тест manifest v2 с unit_semantics."""
        manifest = create_manifest_v2(
            unit_id="UNIT_TEST",
            unit_semantics={
                "domain": "public_procurement",
                "entity": "tender_protocol",
                "expected_content": ["protocol", "attachments"]
            }
        )
        
        assert "unit_semantics" in manifest
        assert manifest["unit_semantics"]["domain"] == "public_procurement"
        assert manifest["unit_semantics"]["entity"] == "tender_protocol"
    
    def test_manifest_v2_with_source_urls(self):
        """Тест manifest v2 с source URLs."""
        manifest = create_manifest_v2(
            unit_id="UNIT_TEST",
            source_urls=[{
                "url": "https://example.gov/doc1",
                "downloaded_at": "2025-03-18T09:12:33Z",
                "http_status": 200
            }]
        )
        
        assert "source" in manifest
        assert "urls" in manifest["source"]
        assert len(manifest["source"]["urls"]) == 1
        assert manifest["source"]["urls"][0]["url"] == "https://example.gov/doc1"
    
    def test_update_manifest_v2(self):
        """Тест обновления manifest v2."""
        manifest = create_manifest_v2(
            unit_id="UNIT_TEST",
            current_cycle=1,
            state_trace=["RAW_INPUT", "CLASSIFIED_1"]
        )
        
        # Обновляем state_trace и cycle
        updated = update_manifest_v2(
            manifest,
            state_trace=["RAW_INPUT", "CLASSIFIED_1", "PENDING_1", "CLASSIFIED_2"],
            current_cycle=2,
            final_cluster="Merge_2",
            final_reason="converted"
        )
        
        assert updated["processing"]["current_cycle"] == 2
        assert updated["processing"]["final_cluster"] == "Merge_2"
        assert updated["processing"]["final_reason"] == "converted"
        assert len(updated["state_machine"]["state_trace"]) == 4
    
    def test_manifest_v2_integrity(self):
        """Тест секции integrity в manifest v2."""
        manifest = create_manifest_v2(
            unit_id="UNIT_TEST",
            files=[{
                "original_name": "test.pdf",
                "current_name": "test.pdf",
                "mime_type": "application/pdf",
                "detected_type": "pdf",
                "needs_ocr": False,
                "transformations": []
            }]
        )
        
        assert "integrity" in manifest
        assert "file_count" in manifest["integrity"]
        assert manifest["integrity"]["file_count"] == 1


"""
Unit тесты для синхронизации протоколов.
Тестируют функции извлечения URL, генерации ID и обработки документов.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from router.protocol_sync import (
    _extract_urls_from_attachments,
    _generate_unit_id,
    sync_protocols_for_date
)


class TestExtractUrlsFromAttachments:
    """Тесты для функции извлечения URL из вложений."""

    def test_extract_urls_dict_structure(self):
        """Тест извлечения URL из структуры attachments как dict."""
        raw_doc = {
            "attachments": {
                "document": [
                    {
                        "url": "https://example.com/doc1.pdf",
                        "fileName": "doc1.pdf",
                        "guid": "guid1",
                        "contentUid": "content1",
                        "description": "Document 1"
                    },
                    {
                        "url": "https://example.com/doc2.pdf",
                        "fileName": "doc2.pdf",
                        "guid": "guid2",
                        "contentUid": "content2",
                        "description": "Document 2"
                    }
                ]
            },
            "loadDate": datetime(2024, 12, 17, 12, 0, 0)
        }

        urls, load_date = _extract_urls_from_attachments(raw_doc)

        assert len(urls) == 2
        assert urls[0]["url"] == "https://example.com/doc1.pdf"
        assert urls[0]["fileName"] == "doc1.pdf"
        assert urls[1]["url"] == "https://example.com/doc2.pdf"
        assert load_date == datetime(2024, 12, 17, 12, 0, 0)

    def test_extract_urls_single_dict_document(self):
        """Тест извлечения URL когда document - это dict, а не list."""
        raw_doc = {
            "attachments": {
                "document": {
                    "url": "https://example.com/single.pdf",
                    "fileName": "single.pdf",
                    "guid": "single-guid",
                    "contentUid": "single-content",
                    "description": "Single document"
                }
            }
        }

        urls, load_date = _extract_urls_from_attachments(raw_doc)

        assert len(urls) == 1
        assert urls[0]["url"] == "https://example.com/single.pdf"
        assert urls[0]["fileName"] == "single.pdf"

    def test_extract_urls_list_structure(self):
        """Тест извлечения URL из структуры attachments как list."""
        raw_doc = {
            "attachments": [
                {
                    "url": "https://example.com/list1.pdf",
                    "fileName": "list1.pdf",
                    "guid": "list1-guid"
                },
                {
                    "url": "https://example.com/list2.pdf",
                    "fileName": "list2.pdf",
                    "guid": "list2-guid"
                }
            ]
        }

        urls, load_date = _extract_urls_from_attachments(raw_doc)

        assert len(urls) == 2
        assert urls[0]["url"] == "https://example.com/list1.pdf"
        assert urls[1]["url"] == "https://example.com/list2.pdf"

    def test_extract_urls_alternative_url_fields(self):
        """Тест извлечения URL из альтернативных полей."""
        raw_doc = {
            "attachments": {
                "document": [
                    {
                        "downloadUrl": "https://example.com/download.pdf",
                        "fileName": "download.pdf"
                    },
                    {
                        "fileUrl": "https://example.com/file.pdf",
                        "name": "file.pdf"
                    },
                    {
                        "url": "https://example.com/normal.pdf",
                        "fileName": "normal.pdf"
                    }
                ]
            }
        }

        urls, load_date = _extract_urls_from_attachments(raw_doc)

        assert len(urls) == 3
        assert urls[0]["url"] == "https://example.com/download.pdf"
        assert urls[1]["url"] == "https://example.com/file.pdf"
        assert urls[2]["url"] == "https://example.com/normal.pdf"

    def test_extract_urls_no_attachments(self):
        """Тест обработки документа без вложений."""
        raw_doc = {
            "loadDate": datetime(2024, 12, 17, 10, 30, 0)
        }

        urls, load_date = _extract_urls_from_attachments(raw_doc)

        assert len(urls) == 0
        assert load_date == datetime(2024, 12, 17, 10, 30, 0)

    def test_extract_urls_empty_attachments(self):
        """Тест обработки пустых вложений."""
        raw_doc = {
            "attachments": {
                "document": []
            }
        }

        urls, load_date = _extract_urls_from_attachments(raw_doc)

        assert len(urls) == 0

    def test_extract_urls_invalid_structure(self):
        """Тест обработки некорректной структуры вложений."""
        raw_doc = {
            "attachments": "invalid_string"
        }

        urls, load_date = _extract_urls_from_attachments(raw_doc)

        assert len(urls) == 0

    def test_extract_urls_missing_url(self):
        """Тест обработки вложений без URL."""
        raw_doc = {
            "attachments": {
                "document": [
                    {
                        "fileName": "no_url.pdf",
                        "guid": "no-url-guid"
                    },
                    {
                        "url": "https://example.com/valid.pdf",
                        "fileName": "valid.pdf"
                    }
                ]
            }
        }

        urls, load_date = _extract_urls_from_attachments(raw_doc)

        assert len(urls) == 1
        assert urls[0]["url"] == "https://example.com/valid.pdf"


class TestGenerateUnitId:
    """Тесты для функции генерации unit_id."""

    def test_generate_unit_id_format(self):
        """Тест формата генерируемого unit_id."""
        unit_id = _generate_unit_id()

        assert unit_id.startswith("UNIT_")
        assert len(unit_id) == 21  # "UNIT_" + 16 hex chars
        # Проверим, что после "UNIT_" идут только hex символы
        hex_part = unit_id[5:]
        assert len(hex_part) == 16
        int(hex_part, 16)  # Должно быть валидным hex числом

    def test_generate_unit_id_uniqueness(self):
        """Тест уникальности генерируемых unit_id."""
        ids = set()
        for _ in range(1000):
            unit_id = _generate_unit_id()
            assert unit_id not in ids
            ids.add(unit_id)

        assert len(ids) == 1000


class TestSyncProtocolsForDate:
    """Тесты для основной функции синхронизации."""

    @patch('router.protocol_sync.get_remote_mongo_mcp_client')
    @patch('router.protocol_sync.get_local_mongo_client')
    def test_sync_protocols_connection_failure_remote(self, mock_local, mock_remote):
        """Тест обработки ошибки подключения к удаленной MongoDB."""
        mock_remote.return_value = None

        result = sync_protocols_for_date()

        assert result["status"] == "error"
        assert "Не удалось подключиться к удалённой MongoDB" in result["message"]

    @patch('router.protocol_sync.get_remote_mongo_mcp_client')
    @patch('router.protocol_sync.get_local_mongo_client')
    def test_sync_protocols_connection_failure_local(self, mock_local, mock_remote):
        """Тест обработки ошибки подключения к локальной MongoDB."""
        mock_remote.return_value = MagicMock()
        mock_local.return_value = None

        result = sync_protocols_for_date()

        assert result["status"] == "error"
        assert "Не удалось подключиться к локальной MongoDB" in result["message"]

        # Проверяем, что удаленный клиент был закрыт
        mock_remote.return_value.close.assert_called_once()

    @patch('router.protocol_sync.get_remote_mongo_mcp_client')
    @patch('router.protocol_sync.get_local_mongo_client')
    def test_sync_protocols_success(self, mock_local, mock_remote):
        """Тест успешной синхронизации."""
        # Моки для MongoDB клиентов
        remote_client = MagicMock()
        local_client = MagicMock()
        mock_remote.return_value = remote_client
        mock_local.return_value = local_client

        # Мок для коллекций
        remote_db = MagicMock()
        remote_coll = MagicMock()
        local_db = MagicMock()
        local_coll = MagicMock()

        remote_client.__getitem__.return_value = remote_db
        remote_db.__getitem__.return_value = remote_coll
        local_client.__getitem__.return_value = local_db
        local_db.__getitem__.return_value = local_coll

        # Мок для курсора с тестовыми данными
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = [
            {
                "purchaseInfo": {"purchaseNoticeNumber": "12345678901234567890"},
                "attachments": {
                    "document": [{
                        "url": "https://example.com/test.pdf",
                        "fileName": "test.pdf"
                    }]
                },
                "loadDate": datetime(2024, 12, 17, 12, 0, 0)
            }
        ]
        remote_coll.find.return_value = mock_cursor
        remote_coll.find.return_value.sort.return_value = mock_cursor

        # Мок для проверки дубликатов (документ не существует)
        local_coll.find_one.return_value = None

        # Запуск синхронизации
        result = sync_protocols_for_date(datetime(2024, 12, 17), limit=10)

        # Проверки
        assert result["status"] == "success"
        assert result["inserted"] == 1
        assert result["scanned"] == 1
        assert result["skipped_existing"] == 0

        # Проверяем, что insert_one был вызван
        local_coll.insert_one.assert_called_once()

        # Проверяем, что клиенты были закрыты
        remote_client.close.assert_called_once()
        local_client.close.assert_called_once()

    @patch('router.protocol_sync.get_remote_mongo_mcp_client')
    @patch('router.protocol_sync.get_local_mongo_client')
    def test_sync_protocols_duplicate_skipping(self, mock_local, mock_remote):
        """Тест пропуска дубликатов."""
        # Моки для MongoDB клиентов
        remote_client = MagicMock()
        local_client = MagicMock()
        mock_remote.return_value = remote_client
        mock_local.return_value = local_client

        # Мок для коллекций
        remote_db = MagicMock()
        remote_coll = MagicMock()
        local_db = MagicMock()
        local_coll = MagicMock()

        remote_client.__getitem__.return_value = remote_db
        remote_db.__getitem__.return_value = remote_coll
        local_client.__getitem__.return_value = local_db
        local_db.__getitem__.return_value = local_coll

        # Мок для курсора с тестовыми данными
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = [
            {
                "purchaseInfo": {"purchaseNoticeNumber": "12345678901234567890"},
                "attachments": {
                    "document": [{
                        "url": "https://example.com/test.pdf",
                        "fileName": "test.pdf"
                    }]
                },
                "loadDate": datetime(2024, 12, 17, 12, 0, 0)
            }
        ]
        remote_coll.find.return_value = mock_cursor
        remote_coll.find.return_value.sort.return_value = mock_cursor

        # Мок для проверки дубликатов (документ уже существует)
        local_coll.find_one.return_value = {"_id": "existing_id"}

        # Запуск синхронизации
        result = sync_protocols_for_date(datetime(2024, 12, 17), limit=10)

        # Проверки
        assert result["status"] == "success"
        assert result["inserted"] == 0
        assert result["scanned"] == 1
        assert result["skipped_existing"] == 1

        # Проверяем, что insert_one НЕ был вызван
        local_coll.insert_one.assert_not_called()

    @patch('router.protocol_sync.get_remote_mongo_mcp_client')
    @patch('router.protocol_sync.get_local_mongo_client')
    def test_sync_protocols_no_urls(self, mock_local, mock_remote):
        """Тест обработки документа без URL."""
        # Моки для MongoDB клиентов
        remote_client = MagicMock()
        local_client = MagicMock()
        mock_remote.return_value = remote_client
        mock_local.return_value = local_client

        # Мок для коллекций
        remote_db = MagicMock()
        remote_coll = MagicMock()
        local_db = MagicMock()
        local_coll = MagicMock()

        remote_client.__getitem__.return_value = remote_db
        remote_db.__getitem__.return_value = remote_coll
        local_client.__getitem__.return_value = local_db
        local_db.__getitem__.return_value = local_coll

        # Мок для курсора с документом без вложений
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = [
            {
                "purchaseInfo": {"purchaseNoticeNumber": "12345678901234567890"},
                "attachments": {},  # Пустые вложения
                "loadDate": datetime(2024, 12, 17, 12, 0, 0)
            }
        ]
        remote_coll.find.return_value = mock_cursor
        remote_coll.find.return_value.sort.return_value = mock_cursor

        # Мок для проверки дубликатов
        local_coll.find_one.return_value = None

        # Запуск синхронизации
        result = sync_protocols_for_date(datetime(2024, 12, 17), limit=10)

        # Проверки
        assert result["status"] == "success"
        assert result["inserted"] == 0  # Не должно быть вставок без URL
        assert result["scanned"] == 1

        # Проверяем, что insert_one НЕ был вызван
        local_coll.insert_one.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__])

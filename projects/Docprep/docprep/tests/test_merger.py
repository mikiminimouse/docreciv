"""
Тесты для engine/merger.py - модуль объединения UNIT в Ready2Docling.

Фокус: Исправление директории "other" - файлы с неизвестными расширениями
должны сортироваться по имени расширения (csv/, txt/, html/), а не в other/.
"""
import pytest
import json
from pathlib import Path
from datetime import datetime

from docprep.engine.merger import Merger, _sanitize_extension
from docprep.core.manifest import create_manifest_v2
from docprep.core.state_machine import UnitState


class TestSanitizeExtension:
    """Тесты для helper-функции _sanitize_extension."""

    def test_normal_extension(self):
        """Обычное расширение возвращается как есть (lowercase)."""
        assert _sanitize_extension("csv") == "csv"
        assert _sanitize_extension("CSV") == "csv"
        assert _sanitize_extension("TXT") == "txt"

    def test_extension_with_dot(self):
        """Расширение с точкой в начале - точка удаляется."""
        assert _sanitize_extension(".csv") == "csv"
        assert _sanitize_extension(".PDF") == "pdf"

    def test_empty_extension_raises(self):
        """Пустое расширение вызывает ValueError - баг классификатора."""
        with pytest.raises(ValueError, match="classifier bug"):
            _sanitize_extension("")

    def test_only_dot_raises(self):
        """Только точка вызывает ValueError."""
        with pytest.raises(ValueError, match="classifier bug"):
            _sanitize_extension(".")

    def test_dangerous_chars_removed(self):
        """Опасные символы для filesystem удаляются."""
        # Символы < > : " / \ | ? * и null должны удаляться
        assert _sanitize_extension("test<>name") == "testname"
        assert _sanitize_extension('te"st') == "test"
        assert _sanitize_extension("te/st") == "test"
        assert _sanitize_extension("te\\st") == "test"
        assert _sanitize_extension("te|st") == "test"
        assert _sanitize_extension("te?st") == "test"
        assert _sanitize_extension("te*st") == "test"
        assert _sanitize_extension("te:st") == "test"

    def test_extension_truncated_if_too_long(self):
        """Слишком длинное расширение обрезается до 10 символов."""
        long_ext = "a" * 20
        result = _sanitize_extension(long_ext)
        assert len(result) == 10
        assert result == "a" * 10

    def test_extension_becomes_empty_after_sanitization_raises(self):
        """Если после санитизации расширение пустое - ValueError."""
        # Расширение только из опасных символов
        with pytest.raises(ValueError, match="classifier bug"):
            _sanitize_extension("<>:\"/\\|?*")


class TestMergerUnknownExtensions:
    """Тесты для обработки неизвестных расширений в Merger."""

    @pytest.fixture
    def merger(self):
        """Создает экземпляр Merger."""
        return Merger()

    @pytest.fixture
    def setup_merge_dirs(self, temp_dir):
        """Создает структуру директорий для merge."""
        merge_dir = temp_dir / "Merge" / "Direct"
        merge_dir.mkdir(parents=True)
        ready_dir = temp_dir / "Ready2Docling"
        ready_dir.mkdir(parents=True)
        exceptions_dir = temp_dir / "Exceptions"
        exceptions_dir.mkdir(parents=True)
        return merge_dir, ready_dir, exceptions_dir

    def _create_unit_with_file(
        self, merge_dir: Path, unit_id: str, filename: str,
        content: bytes = b"test content", state: str = "CLASSIFIED_1"
    ) -> Path:
        """Создает UNIT с одним файлом и манифестом."""
        unit_path = merge_dir / unit_id
        unit_path.mkdir(parents=True)

        # Создаем файл
        file_path = unit_path / filename
        file_path.write_bytes(content)

        # Создаем manifest
        ext = Path(filename).suffix.lower().lstrip(".")
        manifest = create_manifest_v2(
            unit_id=unit_id,
            protocol_id=unit_id.replace("UNIT_", ""),
            protocol_date=datetime.now().strftime("%Y-%m-%d"),
            files=[
                {
                    "original_name": filename,
                    "current_name": filename,
                    "mime_type": "application/octet-stream",
                    "detected_type": ext if ext else "unknown",
                    "needs_ocr": False,
                    "sha256": "test_hash",
                    "size": len(content),
                    "transformations": [],
                },
            ],
            current_cycle=1,
            state_trace=[UnitState.RAW.value, state],
        )
        manifest["state_machine"]["current_state"] = state

        manifest_path = unit_path / "manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

        return unit_path

    def test_csv_creates_csv_directory(self, merger, setup_merge_dirs):
        """CSV файл должен попадать в csv/, а не в other/."""
        merge_dir, ready_dir, _ = setup_merge_dirs

        self._create_unit_with_file(merge_dir, "UNIT_csv001", "data.csv")

        result = merger.collect_units(
            source_dirs=[merge_dir],
            target_dir=ready_dir,
            cycle=1,
        )

        # Проверяем что UNIT обработан
        assert result["units_processed"] >= 1

        # Проверяем что создана директория csv/, а не other/
        csv_dir = ready_dir / "csv"
        other_dir = ready_dir / "other"

        assert csv_dir.exists(), f"Expected csv/ directory, got: {list(ready_dir.iterdir())}"
        assert not other_dir.exists(), "other/ directory should not be created"
        assert (csv_dir / "UNIT_csv001").exists()

    def test_txt_creates_txt_directory(self, merger, setup_merge_dirs):
        """TXT файл должен попадать в txt/, а не в other/."""
        merge_dir, ready_dir, _ = setup_merge_dirs

        self._create_unit_with_file(merge_dir, "UNIT_txt001", "readme.txt")

        result = merger.collect_units(
            source_dirs=[merge_dir],
            target_dir=ready_dir,
            cycle=1,
        )

        assert result["units_processed"] >= 1

        txt_dir = ready_dir / "txt"
        other_dir = ready_dir / "other"

        assert txt_dir.exists(), f"Expected txt/ directory, got: {list(ready_dir.iterdir())}"
        assert not other_dir.exists(), "other/ directory should not be created"
        assert (txt_dir / "UNIT_txt001").exists()

    def test_xlsx_creates_xlsx_directory(self, merger, setup_merge_dirs):
        """XLSX файл должен попадать в xlsx/ (известный тип)."""
        merge_dir, ready_dir, _ = setup_merge_dirs

        # XLSX с правильным magic bytes (PK для ZIP-based format)
        xlsx_content = b"PK\x03\x04 fake xlsx content"
        self._create_unit_with_file(merge_dir, "UNIT_xlsx001", "data.xlsx", xlsx_content)

        result = merger.collect_units(
            source_dirs=[merge_dir],
            target_dir=ready_dir,
            cycle=1,
        )

        assert result["units_processed"] >= 1

        xlsx_dir = ready_dir / "xlsx"
        other_dir = ready_dir / "other"

        assert xlsx_dir.exists(), f"Expected xlsx/ directory, got: {list(ready_dir.iterdir())}"
        assert not other_dir.exists(), "other/ directory should not be created"

    def test_html_creates_html_directory(self, merger, setup_merge_dirs):
        """HTML файл должен попадать в html/, а не в other/."""
        merge_dir, ready_dir, _ = setup_merge_dirs

        html_content = b"<!DOCTYPE html><html><body>Test</body></html>"
        self._create_unit_with_file(merge_dir, "UNIT_html001", "page.html", html_content)

        result = merger.collect_units(
            source_dirs=[merge_dir],
            target_dir=ready_dir,
            cycle=1,
        )

        assert result["units_processed"] >= 1

        html_dir = ready_dir / "html"
        other_dir = ready_dir / "other"

        assert html_dir.exists(), f"Expected html/ directory, got: {list(ready_dir.iterdir())}"
        assert not other_dir.exists(), "other/ directory should not be created"

    def test_json_creates_json_directory(self, merger, setup_merge_dirs):
        """JSON файл должен попадать в json/, а не в other/."""
        merge_dir, ready_dir, _ = setup_merge_dirs

        json_content = b'{"key": "value"}'
        self._create_unit_with_file(merge_dir, "UNIT_json001", "data.json", json_content)

        result = merger.collect_units(
            source_dirs=[merge_dir],
            target_dir=ready_dir,
            cycle=1,
        )

        assert result["units_processed"] >= 1

        json_dir = ready_dir / "json"
        other_dir = ready_dir / "other"

        assert json_dir.exists(), f"Expected json/ directory, got: {list(ready_dir.iterdir())}"
        assert not other_dir.exists(), "other/ directory should not be created"


class TestMergerDetermineUnitType:
    """Тесты для метода _determine_unit_type."""

    @pytest.fixture
    def merger(self):
        return Merger()

    def test_empty_unit_raises_error(self, merger, temp_dir):
        """Пустой UNIT должен вызывать ValueError - баг классификатора."""
        unit_dir = temp_dir / "UNIT_empty001"
        unit_dir.mkdir()
        # Создаем только manifest, без контент-файлов
        manifest = {"files": []}
        (unit_dir / "manifest.json").write_text(json.dumps(manifest))

        with pytest.raises(ValueError, match="classifier bug"):
            merger._determine_unit_type(unit_dir)

    def test_files_without_extension_raises_error(self, merger, temp_dir):
        """Файлы без расширения должны вызывать ValueError - баг классификатора."""
        unit_dir = temp_dir / "UNIT_noext001"
        unit_dir.mkdir()
        # Файл без расширения
        (unit_dir / "datafile").write_bytes(b"content")

        with pytest.raises(ValueError, match="classifier bug"):
            merger._determine_unit_type(unit_dir)

    def test_unknown_extension_returns_extension_name(self, merger, temp_dir):
        """Неизвестное расширение возвращает имя расширения, не 'other'."""
        unit_dir = temp_dir / "UNIT_custom001"
        unit_dir.mkdir()
        (unit_dir / "data.customext").write_bytes(b"content")

        result = merger._determine_unit_type(unit_dir)
        assert result == "customext"
        assert result != "other"

    def test_known_extension_returns_mapped_type(self, merger, temp_dir):
        """Известные расширения возвращают mapped типы."""
        unit_dir = temp_dir / "UNIT_pdf001"
        unit_dir.mkdir()
        (unit_dir / "document.pdf").write_bytes(b"%PDF-1.4 content")

        result = merger._determine_unit_type(unit_dir)
        assert result == "pdf"

    def test_csv_extension_returns_csv(self, merger, temp_dir):
        """CSV расширение возвращает 'csv', не 'other'."""
        unit_dir = temp_dir / "UNIT_csv001"
        unit_dir.mkdir()
        (unit_dir / "data.csv").write_bytes(b"a,b,c\n1,2,3")

        result = merger._determine_unit_type(unit_dir)
        assert result == "csv"


class TestMergerGetTargetSubdir:
    """Тесты для метода _get_target_subdir."""

    @pytest.fixture
    def merger(self):
        return Merger()

    def test_unknown_type_uses_extension(self, merger, temp_dir):
        """Неизвестный тип использует расширение файла."""
        base_dir = temp_dir / "Ready2Docling"
        base_dir.mkdir()

        # Создаем файл с неизвестным типом
        test_file = temp_dir / "data.xyz"
        test_file.write_bytes(b"content")

        result = merger._get_target_subdir(
            file_type="unknown",
            files=[test_file],
            base_dir=base_dir,
            manifest=None
        )

        # Должен вернуть base_dir/xyz, не base_dir/other
        assert result == base_dir / "xyz"

    def test_known_type_uses_mapping(self, merger, temp_dir):
        """Известный тип использует стандартный маппинг."""
        base_dir = temp_dir / "Ready2Docling"
        base_dir.mkdir()

        test_file = temp_dir / "document.docx"
        test_file.write_bytes(b"PK content")

        result = merger._get_target_subdir(
            file_type="docx",
            files=[test_file],
            base_dir=base_dir,
            manifest=None
        )

        assert result == base_dir / "docx"

    def test_csv_type_creates_csv_subdir(self, merger, temp_dir):
        """CSV файл создает csv/ поддиректорию."""
        base_dir = temp_dir / "Ready2Docling"
        base_dir.mkdir()

        test_file = temp_dir / "data.csv"
        test_file.write_bytes(b"a,b\n1,2")

        result = merger._get_target_subdir(
            file_type="csv",
            files=[test_file],
            base_dir=base_dir,
            manifest=None
        )

        # csv не в type_to_dir, поэтому должен использовать расширение
        assert result == base_dir / "csv"


class TestMergerArchiveFiltering:
    """Тесты для фильтрации архивов - архивы НЕ должны попадать в Ready2Docling."""

    @pytest.fixture
    def merger(self):
        return Merger()

    @pytest.fixture
    def setup_merge_dirs(self, temp_dir):
        """Создает структуру директорий для merge."""
        merge_dir = temp_dir / "Merge" / "Direct"
        merge_dir.mkdir(parents=True)
        ready_dir = temp_dir / "Ready2Docling"
        ready_dir.mkdir(parents=True)
        exceptions_dir = temp_dir / "Exceptions"
        exceptions_dir.mkdir(parents=True)
        return merge_dir, ready_dir, exceptions_dir

    def _create_unit_with_archive(
        self, merge_dir: Path, unit_id: str, archive_name: str = "archive.zip",
        state: str = "CLASSIFIED_1", with_extract_operation: bool = True
    ) -> Path:
        """Создает UNIT с архивом и manifest."""
        import zipfile

        unit_path = merge_dir / unit_id
        unit_path.mkdir(parents=True)

        # Создаем ZIP архив
        archive_path = unit_path / archive_name
        with zipfile.ZipFile(archive_path, "w") as zf:
            zf.writestr("content.txt", "extracted content")

        # Создаем manifest
        operations = []
        if with_extract_operation:
            operations = [{"type": "extract", "status": "success"}]

        manifest = create_manifest_v2(
            unit_id=unit_id,
            protocol_id=unit_id.replace("UNIT_", ""),
            protocol_date=datetime.now().strftime("%Y-%m-%d"),
            files=[
                {
                    "original_name": archive_name,
                    "current_name": archive_name,
                    "mime_type": "application/zip",
                    "detected_type": "zip_archive",
                    "needs_ocr": False,
                    "sha256": "test_hash",
                    "size": 100,
                    "transformations": [{"type": "extract", "status": "success"}] if with_extract_operation else [],
                },
            ],
            current_cycle=1,
            state_trace=[UnitState.RAW.value, state],
        )
        manifest["state_machine"]["current_state"] = state
        manifest["processing"] = {"operations": operations}

        manifest_path = unit_path / "manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

        return unit_path

    def test_archive_returns_skip_state(self, merger, setup_merge_dirs):
        """Архив с extract операцией должен возвращать skip_state и НЕ попадать в Ready2Docling."""
        merge_dir, ready_dir, _ = setup_merge_dirs

        # Создаем UNIT с архивом, который был извлечен
        self._create_unit_with_archive(
            merge_dir, "UNIT_zip001", "archive.zip",
            state="CLASSIFIED_1", with_extract_operation=True
        )

        result = merger.collect_units(
            source_dirs=[merge_dir],
            target_dir=ready_dir,
            cycle=1,
        )

        # ZIP директория НЕ должна существовать в Ready2Docling
        zip_dir = ready_dir / "zip"
        assert not zip_dir.exists(), f"zip/ directory should NOT exist in Ready2Docling, got: {list(ready_dir.iterdir())}"

    def test_archive_without_extract_goes_to_exceptions(self, merger, setup_merge_dirs):
        """Архив без extract операции должен идти в Exceptions/ErExtract."""
        merge_dir, ready_dir, exceptions_dir = setup_merge_dirs

        # Создаем UNIT с архивом БЕЗ extract операции
        self._create_unit_with_archive(
            merge_dir, "UNIT_zip002", "archive.zip",
            state="CLASSIFIED_1", with_extract_operation=False
        )

        result = merger.collect_units(
            source_dirs=[merge_dir],
            target_dir=ready_dir,
            cycle=1,
        )

        # ZIP НЕ должен быть в Ready2Docling
        zip_dir = ready_dir / "zip"
        assert not zip_dir.exists(), "zip/ should not exist in Ready2Docling"

    def test_check_unit_processing_state_returns_skip_for_archives(self, merger, temp_dir):
        """_check_unit_processing_state должен возвращать skip_state для извлеченных архивов."""
        import zipfile

        # Создаем UNIT с архивом
        unit_id = "UNIT_archive001"
        unit_path = temp_dir / unit_id
        unit_path.mkdir()

        archive_path = unit_path / "archive.zip"
        with zipfile.ZipFile(archive_path, "w") as zf:
            zf.writestr("file.txt", "content")

        files = [archive_path]

        # Создаем unit_info с manifest, показывающим успешную распаковку
        manifest = {
            "state_machine": {"current_state": "CLASSIFIED_1"},
            "processing": {
                "operations": [{"type": "extract", "status": "success"}]
            },
            "files": [
                {
                    "original_name": "archive.zip",
                    "current_name": "archive.zip",
                    "detected_type": "zip_archive",
                    "transformations": [{"type": "extract", "status": "success"}]
                }
            ]
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

        # Должен вернуть skip_state для извлеченного архива
        assert result == "skip_state", f"Expected 'skip_state' for extracted archive, got '{result}'"

    def test_zip_or_office_with_failed_extraction_goes_to_erextract(self, merger, temp_dir):
        """zip_or_office с files_extracted=0 должен идти в ErExtract."""
        import zipfile

        # Создаем UNIT с архивом
        unit_id = "UNIT_zipoffice001"
        unit_path = temp_dir / unit_id
        unit_path.mkdir()

        archive_path = unit_path / "archive.zip"
        with zipfile.ZipFile(archive_path, "w") as zf:
            zf.writestr("file.txt", "content")

        files = [archive_path]

        # Создаем manifest с неудачной распаковкой (как в реальных данных)
        manifest = {
            "state_machine": {"current_state": "CLASSIFIED_1"},
            "processing": {},
            "files": [
                {
                    "original_name": "archive.zip",
                    "current_name": "archive.zip",
                    "detected_type": "zip_or_office",
                    "transformations": [
                        {"type": "extract", "status": "failed", "files_extracted": 0},
                        {"type": "extract", "archives_processed": 1, "files_extracted": 0}
                    ]
                }
            ]
        }

        unit_info = {"manifest": manifest}
        exceptions_base = temp_dir / "Exceptions"

        result = merger._check_unit_processing_state(
            unit_id=unit_id,
            unit_info=unit_info,
            dominant_type="zip_or_office",
            files=files,
            exceptions_base=exceptions_base,
            current_cycle=1
        )

        # Должен вернуть ErExtract для архива с неудачной распаковкой
        assert result == "ErExtract", f"Expected 'ErExtract' for failed extraction, got '{result}'"

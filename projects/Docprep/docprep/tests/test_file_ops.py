"""
Unit тесты для file_ops утилит.
"""
import pytest
from pathlib import Path
import zipfile

from docprep.utils.file_ops import (
    detect_file_type,
    calculate_sha256,
    sanitize_filename,
    get_file_size,
)


def test_detect_file_type_pdf(temp_dir):
    """Тест детекции PDF файла."""
    pdf_file = temp_dir / "test.pdf"
    pdf_file.write_bytes(b"%PDF-1.4 fake pdf content")
    
    result = detect_file_type(pdf_file)
    
    assert result["detected_type"] == "pdf"
    assert result["mime_type"].startswith("application/pdf")
    assert "sha256" in result


def test_detect_file_type_zip(temp_dir):
    """Тест детекции ZIP архива."""
    zip_file = temp_dir / "test.zip"
    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.writestr("file.txt", "content")
    
    result = detect_file_type(zip_file)
    
    assert result["is_archive"] is True
    assert result["detected_type"] == "zip_archive"


def test_detect_file_type_fake_doc(temp_dir):
    """Тест детекции fake_doc (ZIP с расширением .doc)."""
    fake_doc = temp_dir / "fake.doc"
    with zipfile.ZipFile(fake_doc, "w") as zf:
        zf.writestr("file.txt", "content")
    
    result = detect_file_type(fake_doc)
    
    # Должен определить как архив, а не doc
    assert result.get("is_fake_doc", False) or result["is_archive"] is True


def test_detect_file_type_docx(temp_dir):
    """Тест детекции DOCX файла."""
    docx_file = temp_dir / "test.docx"
    # Создаем минимальный DOCX (ZIP с [Content_Types].xml)
    with zipfile.ZipFile(docx_file, "w") as zf:
        zf.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types></Types>')
        zf.writestr("word/document.xml", "<document></document>")
    
    result = detect_file_type(docx_file)
    
    # Должен определить как docx, а не zip
    assert result["detected_type"] == "docx" or result["is_archive"] is False


def test_calculate_sha256(temp_dir):
    """Тест вычисления SHA256."""
    test_file = temp_dir / "test.txt"
    test_file.write_text("test content")
    
    sha256 = calculate_sha256(test_file)
    
    assert sha256 is not None
    assert len(sha256) == 64  # SHA256 hex длина
    assert isinstance(sha256, str)


def test_sanitize_filename():
    """Тест санитизации имени файла."""
    dangerous = "../file.txt"
    safe = sanitize_filename(dangerous)
    
    assert "../" not in safe
    assert ".." not in safe or safe.count("..") == 0


def test_get_file_size(temp_dir):
    """Тест получения размера файла."""
    test_file = temp_dir / "test.txt"
    test_file.write_text("test content")

    size = get_file_size(test_file)

    assert size > 0
    assert isinstance(size, int)


class TestHTMLDetection:
    """Тесты для детекции HTML файлов по magic bytes."""

    def test_html_with_doc_extension(self, temp_dir):
        """HTML файл с расширением .doc должен определяться как html."""
        html_file = temp_dir / "test.doc"
        html_file.write_bytes(b"<!DOCTYPE html><html><body>Test</body></html>")

        result = detect_file_type(html_file)

        # Должен определить как html, несмотря на расширение .doc
        assert result["detected_type"] == "html", f"Expected html, got {result['detected_type']}"

    def test_html_lowercase_doctype(self, temp_dir):
        """HTML с lowercase <!doctype должен определяться как html."""
        html_file = temp_dir / "test.doc"
        html_file.write_bytes(b"<!doctype html><html><body>Test</body></html>")

        result = detect_file_type(html_file)

        assert result["detected_type"] == "html"

    def test_html_tag_only(self, temp_dir):
        """HTML начинающийся с <html> должен определяться как html."""
        html_file = temp_dir / "test.doc"
        html_file.write_bytes(b"<html><head></head><body>Test</body></html>")

        result = detect_file_type(html_file)

        assert result["detected_type"] == "html"

    def test_html_with_bom(self, temp_dir):
        """HTML с BOM (Byte Order Mark) должен определяться как html."""
        # UTF-8 BOM + HTML
        html_file = temp_dir / "test.doc"
        html_file.write_bytes(b"\xef\xbb\xbf<!DOCTYPE html><html><body>Test</body></html>")

        result = detect_file_type(html_file)

        assert result["detected_type"] == "html"

    def test_xml_detection(self, temp_dir):
        """XML файл должен определяться как xml."""
        xml_file = temp_dir / "test.doc"
        xml_file.write_bytes(b"<?xml version='1.0'?><root><element>Test</element></root>")

        result = detect_file_type(xml_file)

        # Может определиться как xml или html - оба допустимы
        assert result["detected_type"] in ["xml", "html"]

    def test_real_doc_not_html(self, temp_dir):
        """Настоящий DOC файл (OLE2) НЕ должен определяться как html."""
        # OLE2 signature
        doc_file = temp_dir / "test.doc"
        doc_file.write_bytes(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1" + b"\x00" * 100)

        result = detect_file_type(doc_file)

        # Должен определить как doc (OLE2), а не html
        assert result["detected_type"] != "html"
        assert result["detected_type"] in ["doc", "ole2", "unknown"]


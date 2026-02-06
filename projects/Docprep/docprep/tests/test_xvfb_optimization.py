"""
Unit tests for Xvfb optimization.
"""
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from docprep.core.libreoffice_converter import LibreOfficeConverter, RobustDocumentConverter
from docprep.core.optimized_xvfb_manager import XvfbDisplayPool, get_xvfb_pool, cleanup_xvfb_pool


def test_libreoffice_converter_initialization():
    """Test LibreOfficeConverter initialization with optimized Xvfb pool."""
    converter = LibreOfficeConverter()
    assert converter is not None
    assert converter.display_pool is not None
    assert converter.timeout == 300


def test_libreoffice_converter_mock_mode():
    """Test LibreOfficeConverter in mock mode."""
    converter = LibreOfficeConverter(mock_mode=True)
    assert converter.mock_mode is True


def test_robust_document_converter_initialization():
    """Test RobustDocumentConverter initialization."""
    converter = RobustDocumentConverter()
    assert converter is not None
    assert converter.libreoffice is not None


def test_robust_document_converter_mock_mode():
    """Test RobustDocumentConverter in mock mode."""
    converter = RobustDocumentConverter(mock_mode=True)
    assert converter.libreoffice.mock_mode is True


def test_xvfb_display_pool_singleton():
    """Test XvfbDisplayPool singleton behavior."""
    # Clean up any existing instance
    cleanup_xvfb_pool()
    
    # Get first instance
    pool1 = get_xvfb_pool()
    assert isinstance(pool1, XvfbDisplayPool)
    
    # Get second instance - should be the same
    pool2 = get_xvfb_pool()
    assert pool1 is pool2
    
    # Clean up
    cleanup_xvfb_pool()


def test_xvfb_display_pool_acquire_release():
    """Test Xvfb display acquisition and release."""
    pool = XvfbDisplayPool(min_displays=1, max_displays=2, base_display=100)
    
    # Acquire a display
    display1 = pool.acquire_display()
    assert display1 is not None
    assert isinstance(display1, int)
    
    # Acquire another display
    display2 = pool.acquire_display()
    assert display2 is not None
    assert display2 != display1
    
    # Release displays
    pool.release_display(display1)
    pool.release_display(display2)
    
    # Stats should show proper usage
    stats = pool.get_resource_stats()
    assert stats['total_created'] >= 2
    assert stats['total_reused'] >= 0


@patch('docprep.core.libreoffice_converter.subprocess.run')
def test_libreoffice_converter_convert_file_mock_success(mock_subprocess_run):
    """Test LibreOfficeConverter.convert_file with mocked subprocess."""
    # Mock subprocess to return success
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_subprocess_run.return_value = mock_result
    
    # Create converter
    converter = LibreOfficeConverter(mock_mode=True)
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(suffix='.doc', delete=False) as tmp:
        tmp_path = Path(tmp.name)
        tmp_path.write_text("test content")
    
    try:
        # Convert file
        result = converter.convert_file(tmp_path)
        assert result is not None
        assert result.exists()
        
        # Check that the result file has the correct extension
        assert result.suffix == '.docx'
    finally:
        # Clean up
        tmp_path.unlink(missing_ok=True)
        if result and result.exists():
            result.unlink(missing_ok=True)


def test_libreoffice_converter_convert_unsupported_format():
    """Test LibreOfficeConverter with unsupported format."""
    converter = LibreOfficeConverter(mock_mode=True)
    
    # Create temporary file with unsupported extension
    with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as tmp:
        tmp_path = Path(tmp.name)
        tmp_path.write_text("test content")
    
    try:
        # Convert file - should return None for unsupported format
        result = converter.convert_file(tmp_path)
        assert result is None
    finally:
        # Clean up
        tmp_path.unlink(missing_ok=True)


def test_robust_document_converter_fallback():
    """Test RobustDocumentConverter fallback behavior."""
    converter = RobustDocumentConverter(mock_mode=True)
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(suffix='.doc', delete=False) as tmp:
        tmp_path = Path(tmp.name)
        tmp_path.write_text("test content")
    
    try:
        # Convert file - should succeed with mock mode
        result = converter.convert_document(tmp_path)
        assert result is not None
        assert result.exists()
    finally:
        # Clean up
        tmp_path.unlink(missing_ok=True)
        if result and result.exists():
            result.unlink(missing_ok=True)
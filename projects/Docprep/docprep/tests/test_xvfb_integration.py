"""
Integration tests for Xvfb optimization with the full converter system.
"""
import pytest
import tempfile
from pathlib import Path

from docprep.engine.converter import Converter
from docprep.core.libreoffice_converter import RobustDocumentConverter


def test_converter_headless_initialization():
    """Test Converter initialization with headless mode."""
    converter = Converter(use_headless=True, mock_mode=True)
    assert converter.use_headless is True
    assert converter.mock_mode is True
    assert hasattr(converter, 'headless_converter')
    assert isinstance(converter.headless_converter, RobustDocumentConverter)


def test_robust_converter_with_xvfb_pool():
    """Test that RobustDocumentConverter uses the optimized Xvfb pool."""
    # Create converter with mock mode
    converter = RobustDocumentConverter(mock_mode=True)
    
    # Verify that the underlying LibreOffice converter uses Xvfb pool
    assert hasattr(converter.libreoffice, 'display_pool')
    assert converter.libreoffice.mock_mode is True
    
    # Test conversion with a dummy file
    with tempfile.NamedTemporaryFile(suffix='.doc', delete=False) as tmp:
        tmp_path = Path(tmp.name)
        tmp_path.write_text("test content")
    
    try:
        # Convert file - should work with mock mode
        result = converter.convert_document(tmp_path)
        assert result is not None
        assert result.exists()
        assert result.suffix == '.docx'  # Should be converted to docx
    finally:
        # Clean up
        tmp_path.unlink(missing_ok=True)
        if result and result.exists():
            result.unlink(missing_ok=True)
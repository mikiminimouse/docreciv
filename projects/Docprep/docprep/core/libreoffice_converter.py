"""
LibreOffice Converter for headless document conversion.

Solves the "dconf-CRITICAL unable to create file permission denied" issue.
Uses Xvfb + dconf-workaround for headless LibreOffice.
"""

import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional, Dict, Any
import logging

# Import the optimized Xvfb manager
from .optimized_xvfb_manager import get_xvfb_pool
from .exceptions import OperationError

logger = logging.getLogger(__name__)


class LibreOfficeConverter:
    """
    Document converter using LibreOffice in headless mode with optimized Xvfb management.

    Features:
    - Optimized Xvfb resource pooling with lazy initialization
    - dconf workaround for headless
    - Graceful error handling
    - Fallback strategies
    """

    # Supported input formats for conversion
    SUPPORTED_INPUT_FORMATS = {
        '.doc', '.docx', '.rtf', '.odt',  # Word documents
        '.xls', '.xlsx', '.ods',          # Excel documents
        '.ppt', '.pptx', '.odp',          # PowerPoint documents
    }

    # Conversion mapping: input_format -> target_format
    CONVERSION_MAPPING = {
        '.doc': '.docx',
        '.xls': '.xlsx',
        '.ppt': '.pptx',
        '.rtf': '.docx',  # RTF converted to DOCX
        '.odt': '.docx',  # ODT converted to DOCX
        '.ods': '.xlsx',  # ODS converted to XLSX
        '.odp': '.pptx',  # ODP converted to PPTX
    }

    def __init__(self, timeout: Optional[int] = None, mock_mode: bool = False):
        """
        Initialize converter with optimized Xvfb management.

        Args:
            timeout: Conversion timeout in seconds (default: from env or 5 min)
            mock_mode: Mock mode for testing without X11
        """
        # ОПТИМИЗАЦИЯ: Configurable timeout через environment variable
        # LIBREOFFICE_TIMEOUT_SEC позволяет настроить timeout без изменения кода
        default_timeout = int(os.getenv("LIBREOFFICE_TIMEOUT_SEC", "300"))
        self.timeout = timeout if timeout is not None else default_timeout
        self.mock_mode = mock_mode

        # Проверяем зависимости при инициализации
        self.libreoffice_available = False
        self.libreoffice_cmd = None
        self.xvfb_available = False
        self.java_available = False

        if not mock_mode:
            from ..utils.dependencies import DependencyChecker

            # Проверяем LibreOffice
            lo_cmd = DependencyChecker.check_system_tool("libreoffice")
            if not lo_cmd:
                logger.error(
                    "LibreOffice not found! Install: sudo apt-get install libreoffice-core"
                )
            else:
                self.libreoffice_available = True
                self.libreoffice_cmd = lo_cmd
                logger.debug(f"LibreOffice found: {lo_cmd}")

            # Проверяем Xvfb
            xvfb_cmd = DependencyChecker.check_system_tool("xvfb")
            if not xvfb_cmd:
                logger.error(
                    "Xvfb not found! Install: sudo apt-get install xvfb"
                )
            else:
                self.xvfb_available = True
                logger.debug(f"Xvfb found: {xvfb_cmd}")

            # ОПТИМИЗАЦИЯ: Проверка Java окружения (требуется для LibreOffice)
            self.java_available = self._check_java()
            if not self.java_available:
                logger.warning(
                    "Java not found! LibreOffice .doc conversion may fail. "
                    "Install: sudo apt-get install default-jre"
                )

        # Use the optimized Xvfb display pool
        self.display_pool = get_xvfb_pool()

        if mock_mode:
            logger.info("LibreOfficeConverter initialized in MOCK mode")
        else:
            logger.info(
                f"LibreOfficeConverter initialized with optimized Xvfb pool "
                f"(timeout={self.timeout}s, java={'ok' if self.java_available else 'missing'})"
            )

    def _check_java(self) -> bool:
        """
        Проверяет наличие Java в системе.

        Returns:
            True если Java доступен, иначе False
        """
        try:
            result = subprocess.run(
                ["java", "-version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            # Java пишет version в stderr
            if result.returncode == 0:
                version_line = result.stderr.split('\n')[0]
                logger.debug(f"Java found: {version_line}")
                return True
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        return False

    def convert_file(self, input_file: Path, output_dir: Optional[Path] = None) -> Optional[Path]:
        """
        Convert document to appropriate target format using optimized Xvfb management.

        Args:
            input_file: Input file path
            output_dir: Output directory (default: same as input)

        Returns:
            Path to converted file or None on error

        Raises:
            OperationError: Если зависимости не установлены
        """
        if not input_file.exists():
            logger.error(f"Input file does not exist: {input_file}")
            return None

        # Проверяем зависимости (если не mock mode)
        if not self.mock_mode:
            if not self.libreoffice_available:
                raise OperationError(
                    "LibreOffice not installed. Install: sudo apt-get install libreoffice-core libreoffice-writer libreoffice-calc libreoffice-impress",
                    operation="convert",
                    operation_details={"missing": "system_tool", "tool": "libreoffice"}
                )
            if not self.xvfb_available:
                raise OperationError(
                    "Xvfb not installed for headless mode. Install: sudo apt-get install xvfb",
                    operation="convert",
                    operation_details={"missing": "system_tool", "tool": "xvfb"}
                )

        input_ext = input_file.suffix.lower()
        if input_ext not in self.SUPPORTED_INPUT_FORMATS:
            logger.warning(f"Unsupported format: {input_ext}")
            return None

        # Determine target format
        target_ext = self.CONVERSION_MAPPING.get(input_ext)
        if not target_ext:
            logger.error(f"No conversion mapping for {input_ext}")
            return None

        # If file already in target format, do nothing
        if input_ext == target_ext:
            logger.info(f"File already in target format: {input_file.name}")
            return input_file

        # Determine output directory
        if output_dir is None:
            output_dir = input_file.parent
        else:
            output_dir.mkdir(parents=True, exist_ok=True)

        # Mock mode: simulate conversion
        if self.mock_mode:
            return self._mock_conversion(input_file, output_dir, target_ext)

        try:
            # Use optimized Xvfb display pool
            display_num = self.display_pool.acquire_display()
            if display_num is None:
                raise OperationError(
                    "Cannot acquire Xvfb display from pool. Try increasing XVFB_DEFAULT_MAX_DISPLAYS",
                    operation="convert",
                    operation_details={"issue": "xvfb_pool_exhausted"}
                )

            try:
                # Start Xvfb if needed
                if not self.display_pool.start_xvfb_for_display(display_num):
                    raise OperationError(
                        f"Failed to start Xvfb on display :{display_num}",
                        operation="convert",
                        operation_details={"issue": "xvfb_start_failed", "display": display_num}
                    )

                # Get environment for this display
                env = self.display_pool.get_environment_for_display(display_num)

                # Run conversion
                output_file = self._run_conversion_with_env(input_file, output_dir, target_ext, env)

                if output_file and output_file.exists():
                    logger.info(f"Converted: {input_file.name} -> {output_file.name}")
                    return output_file
                else:
                    logger.error("Conversion failed: output file not found")
                    return None

            finally:
                # Always release the display back to the pool
                self.display_pool.release_display(display_num)

        except OperationError:
            # Re-raise OperationError as-is
            raise
        except subprocess.TimeoutExpired:
            logger.error(f"Conversion timeout ({self.timeout}s): {input_file.name}")
            return None
        except Exception as e:
            logger.error(f"Conversion error: {e}")
            return None

    def _mock_conversion(self, input_file: Path, output_dir: Path, target_ext: str) -> Optional[Path]:
        """Mock conversion for testing."""
        logger.info(f"[MOCK] Converting {input_file.name} to {target_ext}")

        # Simulate processing time
        time.sleep(0.1)

        # Create mock output file
        output_file = output_dir / f"{input_file.stem}{target_ext}"

        try:
            content = input_file.read_text(encoding='utf-8', errors='ignore')
            output_file.write_text(f"MOCK CONVERTED CONTENT\nfrom {input_file.name}\nTarget: {target_ext}\n\n{content[:100]}...")
            logger.info(f"[MOCK] Created mock file: {output_file}")
            return output_file
        except Exception as e:
            logger.error(f"[MOCK] Failed to create mock file: {e}")
            return None

    def _run_conversion_with_env(self, input_file: Path, output_dir: Path, target_ext: str, env: Dict[str, str]) -> Optional[Path]:
        """Run conversion via subprocess with specific environment."""
        # Determine LibreOffice format (without dot)
        libreoffice_format = target_ext[1:]

        # Expected output file
        expected_output = output_dir / f"{input_file.stem}{target_ext}"

        # LibreOffice command - используем обнаруженную команду или 'libreoffice' как fallback
        lo_cmd = self.libreoffice_cmd if self.libreoffice_cmd else 'libreoffice'
        cmd = [
            lo_cmd,
            '--headless',
            '--convert-to', libreoffice_format,
            '--outdir', str(output_dir),
            str(input_file)
        ]

        logger.debug(f"Running: {' '.join(cmd)} with DISPLAY=:{env.get('DISPLAY')}")

        # Execute conversion
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=self.timeout
        )

        # Check result
        if result.returncode == 0:
            if expected_output.exists():
                return expected_output
            else:
                # Sometimes LibreOffice creates file with different name
                output_files = list(output_dir.glob(f"{input_file.stem}.*"))
                if output_files:
                    for found_file in output_files:
                        if found_file.suffix.lower() == target_ext.lower():
                            return found_file
                    return output_files[0]
                else:
                    logger.error(f"Output file with extension {target_ext} not found in {output_dir}")
                    return None
        else:
            logger.error(f"LibreOffice failed (exit code {result.returncode})")
            if result.stderr:
                logger.error(f"Error output: {result.stderr[:200]}...")
            return None


class RobustDocumentConverter:
    """Robust converter with fallback strategies and optimized Xvfb management."""

    def __init__(self, mock_mode: bool = False):
        self.libreoffice = LibreOfficeConverter(mock_mode=mock_mode)
        self.fallbacks = [
            self._try_libreoffice,
            self._try_python_docx,
            self._try_pypdf2,
        ]

    def convert_document(self, input_file: Path, output_dir: Optional[Path] = None) -> Optional[Path]:
        """Convert document with fallback strategies."""
        logger.info(f"Converting: {input_file.name}")

        last_error = None

        for converter in self.fallbacks:
            try:
                result = converter(input_file, output_dir)
                if result:
                    logger.info(f"Conversion successful with {converter.__name__}")
                    return result
            except Exception as e:
                logger.warning(f"{converter.__name__} failed: {e}")
                last_error = e
                continue

        logger.error(f"All conversion methods failed for {input_file.name}")
        if last_error:
            logger.error(f"Last error: {last_error}")
        return None

    def _try_libreoffice(self, input_file: Path, output_dir: Path) -> Optional[Path]:
        """Try LibreOffice conversion."""
        return self.libreoffice.convert_file(input_file, output_dir)

    def _try_python_docx(self, input_file: Path, output_dir: Path) -> Optional[Path]:
        """Fallback for .doc files."""
        try:
            if input_file.suffix.lower() not in ['.doc']:
                return None

            try:
                import docx
                from docx2pdf import convert
            except ImportError:
                logger.debug("python-docx not available, skipping fallback")
                return None

            output_file = output_dir / f"{input_file.stem}.pdf"
            convert(str(input_file), str(output_file))

            if output_file.exists():
                return output_file

        except Exception as e:
            logger.debug(f"python-docx fallback failed: {e}")

        return None

    def _try_pypdf2(self, input_file: Path, output_dir: Path) -> Optional[Path]:
        """Fallback file handler."""
        try:
            output_file = output_dir / input_file.name
            output_file.write_bytes(input_file.read_bytes())
            logger.debug(f"Copied file as fallback: {input_file.name}")
            return output_file
        except Exception as e:
            logger.debug(f"File copy fallback failed: {e}")
        return None
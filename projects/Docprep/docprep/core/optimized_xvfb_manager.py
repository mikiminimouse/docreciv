"""
Optimized Xvfb Manager for lazy initialization and resource pooling.
Addresses critical refactoring task: Xvfb optimization.
"""

import os
import subprocess
import time
import threading
import logging
from pathlib import Path
from typing import Optional, Dict, Set
from contextlib import contextmanager

from .constants import (
    XVFB_DEFAULT_MIN_DISPLAYS,
    XVFB_DEFAULT_MAX_DISPLAYS,
    XVFB_DEFAULT_BASE_DISPLAY,
    XVFB_ACQUIRE_TIMEOUT_SEC,
    XVFB_PROCESS_TERMINATE_TIMEOUT_SEC,
    XVFB_PGREP_TIMEOUT_SEC,
    XVFB_STARTUP_DELAY_SEC,
    XVFB_RETRY_INTERVAL_SEC,
    LIBREOFFICE_DEFAULT_TIMEOUT_SEC,
)

logger = logging.getLogger(__name__)


class XvfbDisplayPool:
    """
    Resource pool for Xvfb displays with lazy initialization.
    Implements critical refactoring task: Xvfb optimization.
    """

    def __init__(
        self,
        min_displays: int = XVFB_DEFAULT_MIN_DISPLAYS,
        max_displays: int = XVFB_DEFAULT_MAX_DISPLAYS,
        base_display: int = XVFB_DEFAULT_BASE_DISPLAY
    ):
        """
        Initialize Xvfb display pool.

        Args:
            min_displays: Minimum number of displays to keep ready
            max_displays: Maximum number of displays to create
            base_display: Base display number to start from
        """
        self.min_displays = min_displays
        self.max_displays = max_displays
        self.base_display = base_display
        
        # Thread-safe display tracking
        self._available_displays: Set[int] = set()
        self._used_displays: Dict[int, subprocess.Popen] = {}
        self._display_lock = threading.RLock()
        
        # Resource usage tracking
        self._resource_stats = {
            'total_created': 0,
            'total_reused': 0,
            'peak_usage': 0
        }
        
        # Pre-populate minimum displays
        self._ensure_min_displays()
        
        logger.info(f"XvfbDisplayPool initialized (min={min_displays}, max={max_displays}, base={base_display})")

    def _ensure_min_displays(self):
        """Ensure minimum number of displays are available."""
        with self._display_lock:
            needed = self.min_displays - len(self._available_displays)
            for _ in range(needed):
                display_num = self._get_next_available_display()
                if display_num is not None:
                    self._available_displays.add(display_num)
                    self._resource_stats['total_created'] += 1

    def _get_next_available_display(self) -> Optional[int]:
        """Get next available display number."""
        with self._display_lock:
            # Try to reuse existing displays first
            for display_num in range(self.base_display, self.base_display + self.max_displays):
                if display_num not in self._used_displays and display_num not in self._available_displays:
                    return display_num
            return None

    def acquire_display(self, timeout: int = XVFB_ACQUIRE_TIMEOUT_SEC) -> Optional[int]:
        """
        Acquire an Xvfb display for use.

        Args:
            timeout: Timeout in seconds to wait for display

        Returns:
            Display number or None if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            with self._display_lock:
                # Try to get available display
                if self._available_displays:
                    display_num = self._available_displays.pop()
                    self._used_displays[display_num] = None  # Will be set when process starts
                    self._update_peak_usage()
                    logger.debug(f"Acquired existing display :{display_num}")
                    self._resource_stats['total_reused'] += 1
                    return display_num
                
                # Try to create new display
                if len(self._used_displays) + len(self._available_displays) < self.max_displays:
                    display_num = self._get_next_available_display()
                    if display_num is not None:
                        self._used_displays[display_num] = None
                        self._update_peak_usage()
                        logger.debug(f"Allocated new display :{display_num}")
                        self._resource_stats['total_created'] += 1
                        return display_num
            
            # Wait a bit before retry
            time.sleep(XVFB_RETRY_INTERVAL_SEC)
        
        logger.warning("Timeout waiting for Xvfb display")
        return None

    def release_display(self, display_num: int):
        """
        Release an Xvfb display back to the pool.

        Args:
            display_num: Display number to release
        """
        with self._display_lock:
            if display_num in self._used_displays:
                # Terminate process if running
                process = self._used_displays[display_num]
                if process and process.poll() is None:
                    try:
                        process.terminate()
                        process.wait(timeout=XVFB_PROCESS_TERMINATE_TIMEOUT_SEC)
                        logger.debug(f"Terminated Xvfb process for display :{display_num}")
                    except Exception as e:
                        logger.warning(f"Error terminating Xvfb for display :{display_num}: {e}")
                
                del self._used_displays[display_num]
                self._available_displays.add(display_num)
                logger.debug(f"Released display :{display_num}")

    def _update_peak_usage(self):
        """Update peak usage statistics."""
        current_usage = len(self._used_displays)
        if current_usage > self._resource_stats['peak_usage']:
            self._resource_stats['peak_usage'] = current_usage

    def start_xvfb_for_display(self, display_num: int) -> bool:
        """
        Start Xvfb process for specific display.

        Args:
            display_num: Display number to start Xvfb for

        Returns:
            True if successful, False otherwise
        """
        try:
            with self._display_lock:
                if display_num not in self._used_displays:
                    logger.error(f"Display :{display_num} not acquired")
                    return False
                
                # Check if already running
                result = subprocess.run(
                    ['pgrep', '-f', f'Xvfb.*:{display_num}'],
                    capture_output=True,
                    timeout=XVFB_PGREP_TIMEOUT_SEC
                )
                
                if result.returncode == 0:
                    logger.debug(f"Xvfb already running on :{display_num}")
                    return True
                
                # Start Xvfb
                logger.info(f"Starting Xvfb on :{display_num}")
                process = subprocess.Popen([
                    'Xvfb', f':{display_num}',
                    '-screen', '0', '1024x768x24',
                    '-ac'
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                # Store process
                self._used_displays[display_num] = process
                
                # Wait for startup
                time.sleep(XVFB_STARTUP_DELAY_SEC)
                
                # Verify it's running
                result = subprocess.run(
                    ['pgrep', '-f', f'Xvfb.*:{display_num}'],
                    capture_output=True,
                    timeout=XVFB_PGREP_TIMEOUT_SEC
                )
                
                if result.returncode == 0:
                    logger.info(f"Xvfb started successfully on :{display_num}")
                    return True
                else:
                    logger.error(f"Failed to start Xvfb on :{display_num}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error starting Xvfb on :{display_num}: {e}")
            return False

    def get_environment_for_display(self, display_num: int) -> Dict[str, str]:
        """
        Get environment variables for specific display.

        Args:
            display_num: Display number

        Returns:
            Environment variables dictionary

        ИСПРАВЛЕНИЕ: Включаем текущее окружение процесса для обеспечения
        доступности PATH, JAVA_HOME и других необходимых переменных.
        """
        # Копируем текущее окружение
        env = os.environ.copy()

        # Переопределяем/добавляем специфичные для Xvfb переменные
        env.update({
            'DISPLAY': f':{display_num}',
            'DCONF_PROFILE': '/dev/null',
        })

        # ОПТИМИЗАЦИЯ: Убеждаемся что JAVA_HOME установлен (если доступен)
        # Это критично для корректной работы LibreOffice с .doc файлами
        if 'JAVA_HOME' not in env:
            # Пытаемся найти Java в стандартных locations
            java_paths = [
                '/usr/lib/jvm/default-java',
                '/usr/lib/jvm/java-17-openjdk-amd64',
                '/usr/lib/jvm/java-11-openjdk-amd64',
                '/usr/java/default',
            ]
            for path in java_paths:
                if Path(path).exists():
                    env['JAVA_HOME'] = path
                    logger.debug(f"Set JAVA_HOME={path}")
                    break

        return env

    def get_resource_stats(self) -> Dict[str, int]:
        """Get resource usage statistics."""
        with self._display_lock:
            return {
                **self._resource_stats,
                'current_usage': len(self._used_displays),
                'available': len(self._available_displays),
                'total_active': len(self._used_displays) + len(self._available_displays)
            }

    def cleanup(self):
        """Clean up all Xvfb processes."""
        with self._display_lock:
            # Terminate all running processes
            for display_num, process in self._used_displays.items():
                if process and process.poll() is None:
                    try:
                        process.terminate()
                        process.wait(timeout=XVFB_PROCESS_TERMINATE_TIMEOUT_SEC)
                        logger.info(f"Terminated Xvfb process for display :{display_num}")
                    except Exception as e:
                        logger.warning(f"Error terminating Xvfb for display :{display_num}: {e}")
            
            # Clear tracking
            self._used_displays.clear()
            self._available_displays.clear()
            
            logger.info("XvfbDisplayPool cleaned up")


class OptimizedLibreOfficeConverter:
    """
    Optimized LibreOffice converter with lazy Xvfb initialization.
    Implements critical refactoring task: Xvfb optimization.
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

    def __init__(self, display_pool: XvfbDisplayPool, timeout: int = LIBREOFFICE_DEFAULT_TIMEOUT_SEC, mock_mode: bool = False):
        """
        Initialize optimized converter.

        Args:
            display_pool: Xvfb display pool instance
            timeout: Conversion timeout in seconds
            mock_mode: Mock mode for testing
        """
        self.display_pool = display_pool
        self.timeout = timeout
        self.mock_mode = mock_mode
        
        logger.info(f"OptimizedLibreOfficeConverter initialized (timeout={timeout}s, mock={mock_mode})")

    @contextmanager
    def _acquire_display_context(self):
        """Context manager for acquiring and releasing Xvfb display."""
        display_num = None
        try:
            display_num = self.display_pool.acquire_display()
            if display_num is None:
                raise RuntimeError("Failed to acquire Xvfb display")
            
            # Start Xvfb if needed
            if not self.display_pool.start_xvfb_for_display(display_num):
                raise RuntimeError(f"Failed to start Xvfb on :{display_num}")
            
            yield display_num
            
        finally:
            if display_num is not None:
                self.display_pool.release_display(display_num)

    def convert_file(self, input_file: Path, output_dir: Optional[Path] = None) -> Optional[Path]:
        """
        Convert document with lazy Xvfb initialization.

        Args:
            input_file: Input file path
            output_dir: Output directory

        Returns:
            Path to converted file or None on error
        """
        if not input_file.exists():
            logger.error(f"Input file does not exist: {input_file}")
            return None

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

        # Lazy Xvfb initialization and conversion
        try:
            with self._acquire_display_context() as display_num:
                env = self.display_pool.get_environment_for_display(display_num)
                output_file = self._run_conversion_with_env(input_file, output_dir, target_ext, env)
                
                if output_file and output_file.exists():
                    logger.info(f"Converted: {input_file.name} -> {output_file.name}")
                    return output_file
                else:
                    logger.error("Conversion failed: output file not found")
                    return None

        except subprocess.TimeoutExpired:
            logger.error(f"Conversion timeout ({self.timeout}s): {input_file.name}")
            return None
        except Exception as e:
            logger.error(f"Conversion error: {e}")
            return None

    def _run_conversion_with_env(self, input_file: Path, output_dir: Path, target_ext: str, env: Dict[str, str]) -> Optional[Path]:
        """Run conversion with specific environment."""
        # Determine LibreOffice format (without dot)
        libreoffice_format = target_ext[1:]

        # Expected output file
        expected_output = output_dir / f"{input_file.stem}{target_ext}"

        # LibreOffice command
        cmd = [
            'libreoffice',
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


# Singleton instance for global use
_xvfb_pool_instance: Optional[XvfbDisplayPool] = None
_xvfb_pool_lock = threading.Lock()


def get_xvfb_pool() -> XvfbDisplayPool:
    """
    Get singleton Xvfb display pool instance.
    
    Returns:
        XvfbDisplayPool instance
    """
    global _xvfb_pool_instance
    
    with _xvfb_pool_lock:
        if _xvfb_pool_instance is None:
            _xvfb_pool_instance = XvfbDisplayPool()
        return _xvfb_pool_instance


def cleanup_xvfb_pool():
    """Clean up global Xvfb pool instance."""
    global _xvfb_pool_instance
    
    with _xvfb_pool_lock:
        if _xvfb_pool_instance is not None:
            _xvfb_pool_instance.cleanup()
            _xvfb_pool_instance = None
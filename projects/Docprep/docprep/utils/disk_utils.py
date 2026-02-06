"""
Утилиты для работы с диском
"""
import shutil
from pathlib import Path
from typing import Tuple
import logging

logger = logging.getLogger(__name__)


def check_disk_space(path: Path, required_gb: float = 10.0) -> Tuple[bool, str]:
    """
    Проверяет наличие свободного места на диске.

    Args:
        path: Путь для проверки
        required_gb: Минимальное требуемое место в GB

    Returns:
        (bool, str): (достаточно_места, сообщение)
    """
    try:
        stat = shutil.disk_usage(path)
        free_gb = stat.free / (1024 ** 3)

        if free_gb < required_gb:
            msg = f"Insufficient disk space: {free_gb:.2f} GB free, {required_gb:.2f} GB required"
            logger.warning(msg)
            return False, msg

        msg = f"Disk space OK: {free_gb:.2f} GB free"
        logger.info(msg)
        return True, msg

    except Exception as e:
        msg = f"Failed to check disk space: {e}"
        logger.error(msg)
        return False, msg


def estimate_unit_size(unit_path: Path) -> int:
    """
    Оценивает размер UNIT в байтах.

    Args:
        unit_path: Путь к UNIT директории

    Returns:
        int: Размер в байтах
    """
    total_size = 0
    try:
        for item in unit_path.rglob("*"):
            if item.is_file():
                total_size += item.stat().st_size
    except Exception as e:
        logger.warning(f"Failed to estimate size for {unit_path}: {e}")

    return total_size

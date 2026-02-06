#!/usr/bin/env python3
"""
Migration script: Обновление manifest v2.0 → v2.1 для trace системы.

Добавляет:
- registration_number (из unit.meta.json или MongoDB)
- trace секция с component, stage, timestamp
- Обновляет schema_version с "2.0" на "2.1"

Usage:
    python -m docprep.scripts.migrate_manifest_v21 /path/to/Data/YYYY-MM-DD
"""
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

# Добавляем project root в path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from docprep.core.manifest import (
    load_manifest,
    save_manifest,
    get_registration_number,
)
from docprep.utils.paths import find_all_units


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def migrate_manifest_to_v21(unit_path: Path, dry_run: bool = False) -> Dict[str, Any]:
    """
    Мигрирует manifest.json до версии 2.1.

    Args:
        unit_path: Path к директории UNIT
        dry_run: Если True, только показывает изменения

    Returns:
        Словарь с результатом миграции:
        - success: True если миграция успешна
        - changes: список изменений
        - error: сообщение об ошибке если есть
    """
    result = {
        "success": False,
        "changes": [],
        "error": None,
    }

    manifest_file = unit_path / "manifest.json"

    if not manifest_file.exists():
        result["error"] = "manifest.json not found"
        return result

    try:
        # Загружаем manifest
        manifest = load_manifest(unit_path)

        current_version = manifest.get("schema_version", "2.0")
        if current_version == "2.1":
            result["success"] = True
            result["changes"].append("Already at v2.1")
            return result

        changes = []

        # 1. Обновляем schema_version
        if manifest.get("schema_version") != "2.1":
            manifest["schema_version"] = "2.1"
            changes.append("schema_version: 2.0 → 2.1")

        # 2. Добавляем registration_number
        if "registration_number" not in manifest:
            reg_num = get_registration_number(unit_path)
            if reg_num:
                manifest["registration_number"] = reg_num
                changes.append(f"registration_number: {reg_num[:8]}...")
            else:
                manifest["registration_number"] = ""
                changes.append("registration_number: (empty)")

        # 3. Добавляем trace секцию
        if "trace" not in manifest:
            registration_number = manifest.get("registration_number", "")
            primary_id = registration_number or manifest["unit_id"]

            manifest["trace"] = {
                "primary_id": primary_id,
                "component": "docprep",
                "stage": "preprocessing",
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
            changes.append(f"trace: added (primary_id={primary_id[:8]}...)")

        result["changes"] = changes

        if not dry_run:
            # Сохраняем обновленный manifest
            save_manifest(unit_path, manifest)
            logger.info(f"Migrated {unit_path.name}: {', '.join(changes)}")
        else:
            logger.info(f"[DRY RUN] Would migrate {unit_path.name}: {', '.join(changes)}")

        result["success"] = True

    except Exception as e:
        result["error"] = str(e)
        logger.warning(f"Failed to migrate {unit_path.name}: {e}")

    return result


def migrate_directory(
    directory: Path,
    dry_run: bool = False,
    max_units: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Мигрирует все manifest в директории.

    Args:
        directory: Path к директории с UNIT (например, Ready2Docling/YYYY-MM-DD)
        dry_run: Если True, только показывает изменения
        max_units: Максимальное количество UNIT для обработки

    Returns:
        Статистика миграции
    """
    stats = {
        "total": 0,
        "migrated": 0,
        "already_v21": 0,
        "failed": 0,
        "errors": [],
    }

    logger.info(f"{'='*60}")
    logger.info(f"MIGRATION: manifest v2.0 → v2.1")
    logger.info(f"{'='*60}")
    logger.info(f"Directory: {directory}")
    logger.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info(f"{'='*60}")

    # Находим все UNIT
    units = find_all_units(directory)
    stats["total"] = len(units)

    if max_units:
        units = units[:max_units]
        logger.info(f"Processing first {max_units} of {stats['total']} units")

    if not units:
        logger.warning(f"No units found in {directory}")
        return stats

    for unit_path in units:
        result = migrate_manifest_to_v21(unit_path, dry_run=dry_run)

        if result["success"]:
            if "Already at v2.1" in result.get("changes", []):
                stats["already_v21"] += 1
            else:
                stats["migrated"] += 1
        else:
            stats["failed"] += 1
            stats["errors"].append({
                "unit": unit_path.name,
                "error": result.get("error"),
            })

    # Вывод статистики
    logger.info(f"{'='*60}")
    logger.info(f"MIGRATION COMPLETE")
    logger.info(f"{'='*60}")
    logger.info(f"Total units:     {stats['total']}")
    logger.info(f"Migrated:        {stats['migrated']}")
    logger.info(f"Already v2.1:    {stats['already_v21']}")
    logger.info(f"Failed:          {stats['failed']}")

    if stats["errors"]:
        logger.warning(f"Errors:")
        for err in stats["errors"][:5]:  # Первые 5 ошибок
            logger.warning(f"  - {err['unit']}: {err['error']}")
        if len(stats["errors"]) > 5:
            logger.warning(f"  ... and {len(stats['errors']) - 5} more")

    return stats


def main():
    """Точка входа для CLI."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Migrate manifest v2.0 → v2.1 for trace system"
    )
    parser.add_argument(
        "directory",
        type=Path,
        help="Path to directory with UNITs (e.g., /path/to/Ready2Docling/2026-01-27)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show changes without writing"
    )
    parser.add_argument(
        "--max-units",
        type=int,
        default=None,
        help="Maximum number of units to process"
    )

    args = parser.parse_args()

    if not args.directory.exists():
        logger.error(f"Directory not found: {args.directory}")
        sys.exit(1)

    migrate_directory(
        directory=args.directory,
        dry_run=args.dry_run,
        max_units=args.max_units,
    )


if __name__ == "__main__":
    main()

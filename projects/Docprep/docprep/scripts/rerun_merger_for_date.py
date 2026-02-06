#!/usr/bin/env python3
"""
Скрипт для перезапуска merger с обновленной фильтрацией.

Перезапускает процесс объединения UNIT из Merge директорий в Ready2Docling
с применением новой фильтрации неподдерживаемых форматов.

Использование:
    python final_preprocessing/docprep/scripts/rerun_merger_for_date.py 2025-03-04
"""
import sys
import logging
import argparse
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional

# Добавляем путь к проекту
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from final_preprocessing.docprep.core.config import get_data_paths, get_cycle_paths
from final_preprocessing.docprep.engine.merger import Merger

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def backup_ready2docling(ready2docling_dir: Path) -> Optional[Path]:
    """
    Создает резервную копию Ready2Docling директории.
    
    Args:
        ready2docling_dir: Путь к Ready2Docling директории
        
    Returns:
        Путь к резервной копии или None
    """
    if not ready2docling_dir.exists():
        logger.info(f"Ready2Docling directory does not exist: {ready2docling_dir}")
        return None
    
    # Создаем имя бэкапа с timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = ready2docling_dir.parent / f"Ready2Docling_backup_{timestamp}"
    
    logger.info(f"Creating backup of Ready2Docling to {backup_dir}")
    try:
        shutil.copytree(ready2docling_dir, backup_dir)
        logger.info(f"Backup created successfully: {backup_dir}")
        return backup_dir
    except Exception as e:
        logger.error(f"Failed to create backup: {e}")
        return None


def clear_ready2docling(ready2docling_dir: Path, confirm: bool = False) -> bool:
    """
    Очищает Ready2Docling директорию.
    
    Args:
        ready2docling_dir: Путь к Ready2Docling директории
        confirm: Подтверждение очистки
        
    Returns:
        True если очистка успешна
    """
    if not confirm:
        logger.warning("Clear operation requires confirmation. Use --clear-confirm flag.")
        return False
    
    if not ready2docling_dir.exists():
        logger.info(f"Ready2Docling directory does not exist: {ready2docling_dir}")
        return True
    
    logger.info(f"Clearing Ready2Docling directory: {ready2docling_dir}")
    try:
        # Удаляем содержимое, но оставляем директорию
        for item in ready2docling_dir.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        logger.info("Ready2Docling directory cleared successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to clear Ready2Docling directory: {e}")
        return False


def get_merge_directories(data_paths: dict) -> list:
    """
    Получает список директорий Merge для обработки.
    
    Args:
        data_paths: Словарь с путями от get_data_paths
        
    Returns:
        Список путей к Merge директориям
    """
    merge_dirs = []
    
    # Добавляем Merge_0/Direct для direct файлов
    merge_0_dir = data_paths["merge"] / "Merge_0"
    if merge_0_dir.exists():
        merge_dirs.append(merge_0_dir)
        logger.info(f"Found Merge_0: {merge_0_dir}")
    
    # Добавляем все Merge_N (1, 2, 3)
    for cycle_num in range(1, 4):
        cycle_paths = get_cycle_paths(
            cycle_num,
            data_paths["processing"],
            data_paths["merge"],
            data_paths.get("exceptions")
        )
        merge_dir = cycle_paths["merge"]
        if merge_dir.exists():
            unit_count = len(list(merge_dir.rglob("UNIT_*")))
            if unit_count > 0:
                merge_dirs.append(merge_dir)
                logger.info(f"Found Merge_{cycle_num}: {merge_dir} ({unit_count} UNITs)")
    
    return merge_dirs


def main():
    parser = argparse.ArgumentParser(
        description="Перезапуск merger с обновленной фильтрацией"
    )
    parser.add_argument(
        "date",
        type=str,
        help="Дата в формате YYYY-MM-DD"
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Создать резервную копию Ready2Docling перед обработкой"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Очистить Ready2Docling перед обработкой (требует --clear-confirm)"
    )
    parser.add_argument(
        "--clear-confirm",
        action="store_true",
        help="Подтверждение очистки Ready2Docling"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Режим проверки без выполнения операций"
    )
    
    args = parser.parse_args()
    
    # Валидация даты
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
        sys.exit(1)
    
    # Получаем пути
    data_paths = get_data_paths(args.date)
    ready2docling_dir = data_paths["ready2docling"]
    
    logger.info(f"Processing date: {args.date}")
    logger.info(f"Ready2Docling directory: {ready2docling_dir}")
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No actual changes will be made")
    
    # Создаем резервную копию если нужно
    backup_dir = None
    if args.backup and not args.dry_run:
        backup_dir = backup_ready2docling(ready2docling_dir)
    
    # Очищаем Ready2Docling если нужно
    if args.clear:
        if not args.clear_confirm:
            logger.error("Clear operation requires --clear-confirm flag")
            sys.exit(1)
        if not args.dry_run:
            if not clear_ready2docling(ready2docling_dir, confirm=True):
                logger.error("Failed to clear Ready2Docling")
                sys.exit(1)
        else:
            logger.info(f"DRY RUN: Would clear {ready2docling_dir}")
    
    # Получаем Merge директории
    merge_dirs = get_merge_directories(data_paths)
    
    if not merge_dirs:
        logger.warning("No Merge directories found with UNITs")
        sys.exit(0)
    
    logger.info(f"Found {len(merge_dirs)} Merge directories to process")
    
    if args.dry_run:
        logger.info("DRY RUN: Would run merger with updated filtering")
        sys.exit(0)
    
    # Запускаем merger
    logger.info("Starting merger with updated filtering...")
    merger = Merger()
    
    try:
        result = merger.collect_units(merge_dirs, ready2docling_dir)
        
        logger.info("=" * 60)
        logger.info("MERGER RESULTS")
        logger.info("=" * 60)
        logger.info(f"Units processed: {result.get('units_processed', 0)}")
        logger.info(f"Units skipped: {result.get('units_skipped', 0)}")
        
        if result.get('errors'):
            logger.warning(f"Errors: {len(result['errors'])}")
            for error in result['errors'][:10]:  # Показываем первые 10 ошибок
                logger.warning(f"  {error.get('unit_id', 'unknown')}: {error.get('error', 'unknown error')}")
        
        logger.info("=" * 60)
        
        # Проверяем количество contracts
        if ready2docling_dir.exists():
            contract_count = len(list(ready2docling_dir.rglob("docprep.contract.json")))
            unit_count = len(list(ready2docling_dir.rglob("UNIT_*")))
            logger.info(f"Contracts generated: {contract_count} / {unit_count} UNITs")
            
            if contract_count < unit_count:
                logger.warning(
                    f"Some UNITs are missing contracts. "
                    f"Run generate_contracts_for_ready2docling.py to generate missing contracts."
                )
        
        logger.info("Merger completed successfully")
        
    except Exception as e:
        logger.error(f"Merger failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()


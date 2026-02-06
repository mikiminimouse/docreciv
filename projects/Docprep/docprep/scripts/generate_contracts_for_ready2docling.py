#!/usr/bin/env python3
"""
Утилита для генерации docprep.contract.json для существующих UNIT в Ready2Docling.

Используется для обработки старых данных, где contract файлы еще не были созданы.
"""
import sys
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Any

# Добавляем путь к проекту
_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

from docprep.core.manifest import load_manifest
from docprep.core.contract import generate_contract_from_manifest, save_contract, load_contract

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def find_unit_directories(ready2docling_dir: Path) -> List[Path]:
    """
    Находит все UNIT директории в Ready2Docling.
    
    Args:
        ready2docling_dir: Путь к директории Ready2Docling
        
    Returns:
        Список путей к UNIT директориям
    """
    unit_dirs = list(ready2docling_dir.rglob("UNIT_*"))
    # Фильтруем только директории
    unit_dirs = [d for d in unit_dirs if d.is_dir()]
    return sorted(unit_dirs)


def generate_contract_for_unit(unit_path: Path, skip_existing: bool = True) -> Dict[str, Any]:
    """
    Генерирует contract для UNIT.
    
    Args:
        unit_path: Путь к UNIT директории
        skip_existing: Пропускать UNIT которые уже имеют contract
        
    Returns:
        Словарь с результатами:
        - success: bool
        - unit_id: str
        - action: str (generated/skipped/error)
        - error: str (если была ошибка)
    """
    unit_id = unit_path.name
    result = {
        "success": False,
        "unit_id": unit_id,
        "action": "error",
        "error": None,
    }
    
    # Проверяем наличие contract
    contract_path = unit_path / "docprep.contract.json"
    if contract_path.exists() and skip_existing:
        logger.debug(f"Contract already exists for {unit_id}, skipping")
        result["success"] = True
        result["action"] = "skipped"
        return result
    
    try:
        # Загружаем manifest
        manifest = load_manifest(unit_path)
        
        # Находим главный файл
        files_dir = unit_path / "files"
        if not files_dir.exists():
            files_dir = unit_path
        
        # Ищем файлы документа (не служебные)
        all_files = [
            f for f in files_dir.rglob("*")
            if f.is_file() and f.name not in ["manifest.json", "audit.log.jsonl", "docprep.contract.json", "raw_url_map.json", "unit.meta.json"]
        ]
        
        if not all_files:
            raise ValueError(f"No document files found in unit {unit_id}")
        
        # Определяем главный файл
        main_file = None
        manifest_files = manifest.get("files", [])
        
        # Пытаемся найти главный файл по типу из manifest
        for file_info in manifest_files:
            current_name = file_info.get("current_name", "")
            original_name = file_info.get("original_name", "")
            
            # Ищем файл с именем из manifest
            for file_path in all_files:
                if file_path.name in [current_name, original_name]:
                    # Приоритет файлам документов
                    ext = file_path.suffix.lower()
                    if ext in [".pdf", ".docx", ".xlsx", ".pptx", ".html", ".xml", ".rtf", ".jpg", ".jpeg", ".png", ".tiff"]:
                        main_file = file_path
                        break
            
            if main_file:
                break
        
        # Fallback: первый файл документа
        if not main_file:
            main_file = all_files[0]
        
        # Генерируем contract
        contract = generate_contract_from_manifest(
            unit_path=unit_path,
            manifest=manifest,
            main_file_path=main_file,
        )
        
        # Сохраняем contract
        save_contract(unit_path, contract)
        
        logger.info(f"Generated contract for {unit_id} (route: {contract.get('routing', {}).get('docling_route', 'unknown')})")
        result["success"] = True
        result["action"] = "generated"
        
    except FileNotFoundError as e:
        error_msg = f"Manifest not found: {e}"
        logger.error(f"Failed to generate contract for {unit_id}: {error_msg}")
        result["error"] = error_msg
    except ValueError as e:
        error_msg = f"Validation error: {e}"
        logger.error(f"Failed to generate contract for {unit_id}: {error_msg}")
        result["error"] = error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(f"Failed to generate contract for {unit_id}: {error_msg}", exc_info=True)
        result["error"] = error_msg
    
    return result


def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(
        description="Generate docprep.contract.json files for UNIT in Ready2Docling"
    )
    parser.add_argument(
        "ready2docling_dir",
        type=Path,
        help="Path to Ready2Docling directory (e.g., Data/2025-03-04/Ready2Docling)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of units to process (default: all)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip units that already have contract (default: True)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate contracts even if they exist",
    )
    
    args = parser.parse_args()
    
    ready2docling_dir = Path(args.ready2docling_dir).resolve()
    
    if not ready2docling_dir.exists():
        logger.error(f"Ready2Docling directory not found: {ready2docling_dir}")
        return 1
    
    if not ready2docling_dir.is_dir():
        logger.error(f"Path is not a directory: {ready2docling_dir}")
        return 1
    
    logger.info(f"Searching for UNIT directories in {ready2docling_dir}")
    
    # Находим все UNIT
    unit_dirs = find_unit_directories(ready2docling_dir)
    
    if not unit_dirs:
        logger.warning(f"No UNIT directories found in {ready2docling_dir}")
        return 0
    
    # Применяем лимит
    if args.limit:
        unit_dirs = unit_dirs[:args.limit]
    
    logger.info(f"Found {len(unit_dirs)} unit(s) to process")
    
    # Генерируем contract для каждого UNIT
    results = {
        "total": len(unit_dirs),
        "generated": 0,
        "skipped": 0,
        "errors": 0,
        "details": [],
    }
    
    for unit_dir in unit_dirs:
        result = generate_contract_for_unit(
            unit_dir,
            skip_existing=args.skip_existing and not args.force
        )
        
        results["details"].append(result)
        
        if result["success"]:
            if result["action"] == "generated":
                results["generated"] += 1
            elif result["action"] == "skipped":
                results["skipped"] += 1
        else:
            results["errors"] += 1
    
    # Выводим статистику
    logger.info("=" * 60)
    logger.info("Contract generation summary:")
    logger.info(f"  Total units: {results['total']}")
    logger.info(f"  Generated: {results['generated']}")
    logger.info(f"  Skipped: {results['skipped']}")
    logger.info(f"  Errors: {results['errors']}")
    logger.info("=" * 60)
    
    # Выводим ошибки если есть
    if results["errors"] > 0:
        logger.warning("Units with errors:")
        for detail in results["details"]:
            if not detail["success"]:
                logger.warning(f"  - {detail['unit_id']}: {detail['error']}")
    
    return 0 if results["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())


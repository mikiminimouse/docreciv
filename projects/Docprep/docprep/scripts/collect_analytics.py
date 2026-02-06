#!/usr/bin/env python3
"""
Скрипт для сбора аналитики и метрик обработки UNIT.
"""
import sys
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, Any

# Добавляем путь к модулям (из scripts в docprep, затем в final_preprocessing)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from docprep.core.manifest import load_manifest


def collect_analytics(data_dir: Path) -> Dict[str, Any]:
    """
    Собирает аналитику по обработке UNIT.
    
    Args:
        data_dir: Директория с данными (например, Data/2025-03-19)
    
    Returns:
        Словарь с аналитикой
    """
    analytics = {
        "input": {"total": 0, "units": []},
        "merge": {"total": 0, "by_category": defaultdict(int), "by_state": defaultdict(int), "units": []},
        "ready": {"total": 0, "by_type": defaultdict(int), "by_state": defaultdict(int), "units": []},
        "exceptions": {"total": 0, "by_category": defaultdict(int), "by_state": defaultdict(int), "units": []},
        "processing": {"total": 0, "by_type": defaultdict(int), "units": []},
        "conversion": {"total": 0, "success": 0, "failed": 0, "not_converted": 0},
        "extraction": {"total": 0, "success": 0, "failed": 0},
        "normalization": {"total": 0, "success": 0, "failed": 0},
    }
    
    # Input
    input_dir = data_dir / "Input"
    if input_dir.exists():
        input_units = list(input_dir.rglob("UNIT_*"))
        analytics["input"]["total"] = len([u for u in input_units if u.is_dir()])
    
    # Merge
    merge_dir = data_dir / "Merge"
    if merge_dir.exists():
        merge_units = list(merge_dir.rglob("UNIT_*"))
        merge_units = [u for u in merge_units if u.is_dir()]
        analytics["merge"]["total"] = len(merge_units)
        
        for unit_path in merge_units:
            try:
                manifest = load_manifest(unit_path)
                category = manifest.get("processing", {}).get("classification", {}).get("category", "unknown")
                state = manifest.get("state_machine", {}).get("current_state", "unknown")
                analytics["merge"]["by_category"][category] += 1
                analytics["merge"]["by_state"][state] += 1
                
                # Проверяем операции конвертации
                operations = manifest.get("processing", {}).get("operations", [])
                for op in operations:
                    if op.get("type") == "convert":
                        analytics["conversion"]["total"] += 1
                        if op.get("to") == "docx" and op.get("from") == "doc":
                            analytics["conversion"]["success"] += 1
                        else:
                            analytics["conversion"]["failed"] += 1
            except Exception as e:
                pass
    
    # Ready2Docling
    ready_dir = data_dir / "Ready2Docling"
    if ready_dir.exists():
        ready_units = list(ready_dir.rglob("UNIT_*"))
        ready_units = [u for u in ready_units if u.is_dir()]
        analytics["ready"]["total"] = len(ready_units)
        
        for unit_path in ready_units:
            try:
                manifest = load_manifest(unit_path)
                state = manifest.get("state_machine", {}).get("current_state", "unknown")
                analytics["ready"]["by_state"][state] += 1
                
                # Определяем тип по файлам
                files = manifest.get("files", [])
                if files:
                    file_type = files[0].get("detected_type", "unknown")
                    analytics["ready"]["by_type"][file_type] += 1
            except Exception as e:
                pass
    
    # Exceptions
    exceptions_dir = data_dir / "Exceptions"
    if exceptions_dir.exists():
        exception_units = list(exceptions_dir.rglob("UNIT_*"))
        exception_units = [u for u in exception_units if u.is_dir()]
        analytics["exceptions"]["total"] = len(exception_units)
        
        for unit_path in exception_units:
            try:
                manifest = load_manifest(unit_path)
                category = manifest.get("processing", {}).get("classification", {}).get("category", "unknown")
                state = manifest.get("state_machine", {}).get("current_state", "unknown")
                analytics["exceptions"]["by_category"][category] += 1
                analytics["exceptions"]["by_state"][state] += 1
            except Exception as e:
                pass
    
    # Processing
    processing_dir = data_dir / "Processing"
    if processing_dir.exists():
        processing_units = list(processing_dir.rglob("UNIT_*"))
        processing_units = [u for u in processing_units if u.is_dir()]
        analytics["processing"]["total"] = len(processing_units)
    
    # Проверяем doc файлы в Converted
    converted_dir = merge_dir / "Merge_1" / "Converted" / "doc"
    if converted_dir.exists():
        doc_files = list(converted_dir.rglob("*.doc"))
        analytics["conversion"]["not_converted"] = len(doc_files)
    
    return analytics


def print_analytics(analytics: Dict[str, Any]):
    """Выводит аналитику в читаемом формате."""
    print("=" * 80)
    print("АНАЛИТИКА ОБРАБОТКИ UNIT")
    print("=" * 80)
    
    print(f"\n📥 Input: {analytics['input']['total']} UNIT")
    
    print(f"\n🔄 Merge: {analytics['merge']['total']} UNIT")
    print("  По категориям:")
    for cat, count in sorted(analytics['merge']['by_category'].items(), key=lambda x: -x[1]):
        print(f"    {cat}: {count}")
    print("  По состояниям:")
    for state, count in sorted(analytics['merge']['by_state'].items(), key=lambda x: -x[1]):
        print(f"    {state}: {count}")
    
    print(f"\n✅ Ready2Docling: {analytics['ready']['total']} UNIT")
    print("  По типам:")
    for ftype, count in sorted(analytics['ready']['by_type'].items(), key=lambda x: -x[1]):
        print(f"    {ftype}: {count}")
    print("  По состояниям:")
    for state, count in sorted(analytics['ready']['by_state'].items(), key=lambda x: -x[1]):
        print(f"    {state}: {count}")
    
    print(f"\n⚠️  Exceptions: {analytics['exceptions']['total']} UNIT")
    print("  По категориям:")
    for cat, count in sorted(analytics['exceptions']['by_category'].items(), key=lambda x: -x[1]):
        print(f"    {cat}: {count}")
    
    print(f"\n⚙️  Processing: {analytics['processing']['total']} UNIT")
    
    print(f"\n🔄 Конвертация:")
    print(f"  Всего операций: {analytics['conversion']['total']}")
    print(f"  Успешно: {analytics['conversion']['success']}")
    print(f"  Ошибок: {analytics['conversion']['failed']}")
    print(f"  Не конвертированы (doc в Converted): {analytics['conversion']['not_converted']}")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python collect_analytics.py <data_dir>")
        print("Example: python collect_analytics.py Data/2025-03-19")
        sys.exit(1)
    
    data_dir = Path(sys.argv[1])
    if not data_dir.exists():
        print(f"Error: Directory {data_dir} does not exist")
        sys.exit(1)
    
    analytics = collect_analytics(data_dir)
    print_analytics(analytics)


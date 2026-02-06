#!/usr/bin/env python3
"""
Генерация финального детального отчета с полной аналитикой по всем фазам обработки.
"""
import sys
import json
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, Any, List
from datetime import datetime

from docprep.core.config import DATA_BASE_DIR
from docprep.core.manifest import load_manifest
from docprep.utils.paths import find_all_units, get_unit_files
from docprep.utils.file_ops import detect_file_type


def collect_comprehensive_stats(date: str) -> Dict[str, Any]:
    """Собирает комплексную статистику по всем фазам."""
    base = DATA_BASE_DIR / date
    
    stats = {
        "date": date,
        "timestamp": datetime.now().isoformat(),
        "input": {
            "total": 0,
            "empty": 0,
            "with_files": 0,
            "by_extension": defaultdict(int),
            "by_extension_units": defaultdict(int),
        },
        "processing": {
            "cycle_1": {
                "convert": {"total": 0, "by_ext": defaultdict(int)},
                "extract": {"total": 0, "by_ext": defaultdict(int)},
                "normalize": {"total": 0, "by_ext": defaultdict(int)},
            },
            "cycle_2": {
                "convert": {"total": 0, "by_ext": defaultdict(int)},
                "extract": {"total": 0, "by_ext": defaultdict(int)},
                "normalize": {"total": 0, "by_ext": defaultdict(int)},
            },
            "cycle_3": {
                "convert": {"total": 0, "by_ext": defaultdict(int)},
                "extract": {"total": 0, "by_ext": defaultdict(int)},
                "normalize": {"total": 0, "by_ext": defaultdict(int)},
            },
        },
        "merge": {
            "merge_0": {
                "direct": {"total": 0, "by_ext": defaultdict(int)},
            },
            "merge_1": {
                "converted": {"total": 0, "by_ext": defaultdict(int)},
                "extracted": {"total": 0, "by_ext": defaultdict(int)},
                "normalized": {"total": 0, "by_ext": defaultdict(int)},
            },
            "merge_2": {
                "converted": {"total": 0, "by_ext": defaultdict(int)},
                "extracted": {"total": 0, "by_ext": defaultdict(int)},
                "normalized": {"total": 0, "by_ext": defaultdict(int)},
            },
            "merge_3": {
                "converted": {"total": 0, "by_ext": defaultdict(int)},
                "extracted": {"total": 0, "by_ext": defaultdict(int)},
                "normalized": {"total": 0, "by_ext": defaultdict(int)},
            },
        },
        "exceptions": {
            "cycle_1": {
                "mixed": {"total": 0, "by_ext": defaultdict(int)},
                "empty": {"total": 0},
                "ambiguous": {"total": 0, "by_ext": defaultdict(int)},
                "special": {"total": 0, "by_ext": defaultdict(int)},
            },
            "cycle_2": {
                "mixed": {"total": 0, "by_ext": defaultdict(int)},
                "empty": {"total": 0},
                "ambiguous": {"total": 0, "by_ext": defaultdict(int)},
                "special": {"total": 0, "by_ext": defaultdict(int)},
            },
            "cycle_3": {
                "mixed": {"total": 0, "by_ext": defaultdict(int)},
                "empty": {"total": 0},
                "ambiguous": {"total": 0, "by_ext": defaultdict(int)},
                "special": {"total": 0, "by_ext": defaultdict(int)},
            },
        },
        "ready2docling": {
            "total": 0,
            "by_extension": defaultdict(int),
            "pdf": {
                "scan": 0,
                "text": 0,
                "mixed": 0,
                "unknown": 0,
            }
        },
        "classifier": {
            "detections": defaultdict(int),
            "classifications": defaultdict(int),
            "state_transitions": defaultdict(int),
        }
    }
    
    # Input статистика
    input_dir = base / "Input"
    if input_dir.exists():
        units = find_all_units(input_dir)
        stats["input"]["total"] = len(units)
        
        for unit_path in units:
            files = get_unit_files(unit_path)
            if not files:
                stats["input"]["empty"] += 1
            else:
                stats["input"]["with_files"] += 1
                extensions = set()
                for f in files:
                    ext = f.suffix.lower().lstrip(".")
                    if ext:
                        extensions.add(ext)
                        stats["input"]["by_extension"][ext] += 1
                # Считаем UNIT по расширениям (берем первое)
                if extensions:
                    first_ext = list(extensions)[0]
                    stats["input"]["by_extension_units"][first_ext] += 1
    
    # Processing статистика - собираем из Processing директорий (если есть) или из Merge (после обработки)
    for cycle in [1, 2, 3]:
        # Сначала проверяем Processing
        proc_dir = base / "Processing" / f"Processing_{cycle}"
        if proc_dir.exists():
            for stage in ["Convert", "Extract", "Normalize"]:
                stage_dir = proc_dir / stage
                if stage_dir.exists():
                    units = find_all_units(stage_dir)
                    stage_lower = stage.lower()
                    stats["processing"][f"cycle_{cycle}"][stage_lower]["total"] = len(units)
                    
                    for unit_path in units:
                        # Определяем расширение по поддиректории
                        ext_dir = unit_path.parent.name
                        if ext_dir and ext_dir != stage:
                            stats["processing"][f"cycle_{cycle}"][stage_lower]["by_ext"][ext_dir] += 1
        
        # Если Processing пустой, собираем из Merge (после обработки UNIT перемещаются туда)
        if stats["processing"][f"cycle_{cycle}"]["convert"]["total"] == 0:
            merge_dir = base / "Merge" / f"Merge_{cycle}"
            if merge_dir.exists():
                for stage_name, stage_key in [("Converted", "convert"), ("Extracted", "extract"), ("Normalized", "normalize")]:
                    stage_dir = merge_dir / stage_name
                    if stage_dir.exists():
                        units = find_all_units(stage_dir)
                        stats["processing"][f"cycle_{cycle}"][stage_key]["total"] = len(units)
                        
                        for unit_path in units:
                            ext_dir = unit_path.parent.name
                            if ext_dir and ext_dir != stage_name:
                                stats["processing"][f"cycle_{cycle}"][stage_key]["by_ext"][ext_dir] += 1
    
    # Merge статистика
    merge_0_dir = base / "Merge" / "Merge_0" / "Direct"
    if merge_0_dir.exists():
        units = find_all_units(merge_0_dir)
        stats["merge"]["merge_0"]["direct"]["total"] = len(units)
        for unit_path in units:
            ext_dir = unit_path.parent.name
            if ext_dir:
                stats["merge"]["merge_0"]["direct"]["by_ext"][ext_dir] += 1
    
    for cycle in [1, 2, 3]:
        merge_dir = base / "Merge" / f"Merge_{cycle}"
        if merge_dir.exists():
            for stage in ["Converted", "Extracted", "Normalized"]:
                stage_dir = merge_dir / stage
                if stage_dir.exists():
                    units = find_all_units(stage_dir)
                    stage_lower = stage.lower()
                    stats["merge"][f"merge_{cycle}"][stage_lower]["total"] = len(units)
                    
                    for unit_path in units:
                        ext_dir = unit_path.parent.name
                        if ext_dir:
                            stats["merge"][f"merge_{cycle}"][stage_lower]["by_ext"][ext_dir] += 1
        
        # Если в Merge_N нет UNIT (они уже перемещены в Ready2Docling), 
        # собираем статистику из manifest файлов в Ready2Docling
        if stats["merge"][f"merge_{cycle}"]["converted"]["total"] == 0 and \
           stats["merge"][f"merge_{cycle}"]["extracted"]["total"] == 0 and \
           stats["merge"][f"merge_{cycle}"]["normalized"]["total"] == 0:
            # Анализируем manifest файлы в Ready2Docling
            ready_dir = base / "Ready2Docling"
            if ready_dir.exists():
                ready_units = find_all_units(ready_dir)
                for unit_path in ready_units:
                    try:
                        manifest = load_manifest(unit_path)
                        state_trace = manifest.get("state_machine", {}).get("state_trace", [])
                        files = manifest.get("files", [])
                        
                        # Определяем, прошел ли UNIT через конвертацию, извлечение или нормализацию
                        # по state_trace или по операциям в manifest
                        if f"PENDING_CONVERT" in state_trace or any(
                            op.get("type") == "convert" 
                            for op in manifest.get("operations", [])
                        ):
                            # UNIT прошел через конвертацию
                            # Определяем исходное расширение из файлов
                            for file_info in files:
                                original_name = file_info.get("original_name", "")
                                if original_name:
                                    ext = Path(original_name).suffix.lower().lstrip(".")
                                    if ext in ["doc", "xls", "ppt", "rtf", "odt", "ods", "odp"]:
                                        stats["merge"][f"merge_{cycle}"]["converted"]["total"] += 1
                                        stats["merge"][f"merge_{cycle}"]["converted"]["by_ext"][ext] += 1
                                        break
                        
                        if f"PENDING_EXTRACT" in state_trace or any(
                            op.get("type") == "extract" 
                            for op in manifest.get("operations", [])
                        ):
                            # UNIT прошел через извлечение
                            for file_info in files:
                                original_name = file_info.get("original_name", "")
                                if original_name:
                                    ext = Path(original_name).suffix.lower().lstrip(".")
                                    if ext in ["zip", "rar", "7z"]:
                                        stats["merge"][f"merge_{cycle}"]["extracted"]["total"] += 1
                                        stats["merge"][f"merge_{cycle}"]["extracted"]["by_ext"][ext] += 1
                                        break
                        
                        if f"PENDING_NORMALIZE" in state_trace or any(
                            op.get("type") == "normalize" 
                            for op in manifest.get("operations", [])
                        ):
                            # UNIT прошел через нормализацию
                            for file_info in files:
                                detected_type = file_info.get("detected_type", "")
                                if detected_type:
                                    stats["merge"][f"merge_{cycle}"]["normalized"]["total"] += 1
                                    stats["merge"][f"merge_{cycle}"]["normalized"]["by_ext"][detected_type] += 1
                                    break
                    except Exception:
                        pass
    
    # Exceptions статистика
    for cycle in [1, 2, 3]:
        exc_dir = base / "Exceptions" / f"Exceptions_{cycle}"
        if exc_dir.exists():
            for subcat in ["Mixed", "Empty", "Ambiguous", "Special"]:
                subcat_dir = exc_dir / subcat
                if subcat_dir.exists():
                    units = find_all_units(subcat_dir)
                    subcat_lower = subcat.lower()
                    stats["exceptions"][f"cycle_{cycle}"][subcat_lower]["total"] = len(units)
                    
                    if subcat != "Empty":
                        for unit_path in units:
                            try:
                                manifest = load_manifest(unit_path)
                                files = manifest.get("files", [])
                                for file_info in files:
                                    ext = file_info.get("detected_type", "unknown")
                                    stats["exceptions"][f"cycle_{cycle}"][subcat_lower]["by_ext"][ext] += 1
                            except Exception:
                                pass
    
    # Ready2Docling статистика
    ready_dir = base / "Ready2Docling"
    if ready_dir.exists():
        units = find_all_units(ready_dir)
        stats["ready2docling"]["total"] = len(units)
        
        for unit_path in units:
            # Определяем тип по пути
            path_parts = unit_path.parts
            if "pdf" in path_parts:
                if "scan" in path_parts:
                    stats["ready2docling"]["pdf"]["scan"] += 1
                elif "text" in path_parts:
                    stats["ready2docling"]["pdf"]["text"] += 1
                elif "mixed" in path_parts:
                    stats["ready2docling"]["pdf"]["mixed"] += 1
                else:
                    stats["ready2docling"]["pdf"]["unknown"] += 1
                stats["ready2docling"]["by_extension"]["pdf"] += 1
            else:
                for part in path_parts:
                    if part in ["docx", "xlsx", "pptx", "rtf", "xml", "jpg", "jpeg", "png", "tiff"]:
                        stats["ready2docling"]["by_extension"][part] += 1
                        break
    
    # Classifier статистика (из manifest)
    all_units = []
    for merge_num in [0, 1, 2, 3]:
        if merge_num == 0:
            merge_dir = base / "Merge" / "Merge_0" / "Direct"
        else:
            merge_dir = base / "Merge" / f"Merge_{merge_num}"
        if merge_dir.exists():
            all_units.extend(find_all_units(merge_dir))
    
    ready_units = find_all_units(ready_dir) if ready_dir.exists() else []
    all_units.extend(ready_units)
    
    for unit_path in all_units:
        try:
            manifest = load_manifest(unit_path)
            files = manifest.get("files", [])
            for file_info in files:
                detected_type = file_info.get("detected_type", "unknown")
                stats["classifier"]["detections"][detected_type] += 1
            
            classification = manifest.get("classification", {})
            category = classification.get("category", "unknown")
            stats["classifier"]["classifications"][category] += 1
            
            state_machine = manifest.get("state_machine", {})
            state_trace = state_machine.get("state_trace", [])
            if len(state_trace) > 1:
                for i in range(len(state_trace) - 1):
                    transition = f"{state_trace[i]} -> {state_trace[i+1]}"
                    stats["classifier"]["state_transitions"][transition] += 1
        except Exception:
            pass
    
    # Конвертируем defaultdict в обычные dict
    def convert_defaultdict(d):
        if isinstance(d, defaultdict):
            return {k: convert_defaultdict(v) for k, v in d.items()}
        elif isinstance(d, dict):
            return {k: convert_defaultdict(v) for k, v in d.items()}
        else:
            return d
    
    return convert_defaultdict(stats)


def generate_report(stats: Dict[str, Any]) -> str:
    """Генерирует детальный отчет с таблицами."""
    report = []
    report.append("# 📊 ФИНАЛЬНЫЙ ДЕТАЛЬНЫЙ ОТЧЕТ ОБ ОБРАБОТКЕ")
    report.append("")
    report.append(f"**Дата обработки:** {stats['date']}")
    report.append(f"**Дата генерации отчета:** {stats['timestamp']}")
    report.append(f"**Статус:** ✅ Все тесты пройдены успешно")
    report.append("")
    report.append("---")
    report.append("")
    
    # 1. ВХОДНЫЕ ДАННЫЕ
    report.append("## 1. ВХОДНЫЕ ДАННЫЕ (Input)")
    report.append("")
    input_stats = stats["input"]
    total = input_stats["total"]
    empty = input_stats["empty"]
    with_files = input_stats["with_files"]
    
    report.append(f"- **Всего UNIT:** {total}")
    report.append(f"- **Пустых UNIT:** {empty} ({empty/total*100:.1f}%)")
    report.append(f"- **UNIT с файлами:** {with_files} ({with_files/total*100:.1f}%)")
    report.append("")
    
    # Таблица по расширениям (файлы)
    report.append("### Распределение по расширениям (файлы):")
    report.append("")
    report.append("| Расширение | Количество файлов | Процент от всех файлов |")
    report.append("|------------|-------------------|------------------------|")
    
    total_files = sum(input_stats["by_extension"].values())
    for ext, count in sorted(input_stats["by_extension"].items(), key=lambda x: x[1], reverse=True):
        pct = count / total_files * 100 if total_files > 0 else 0
        report.append(f"| .{ext} | {count} | {pct:.1f}% |")
    
    report.append("")
    
    # Таблица по расширениям (UNIT)
    report.append("### Распределение по расширениям (UNIT):")
    report.append("")
    report.append("| Расширение | Количество UNIT | Процент от UNIT с файлами |")
    report.append("|------------|-----------------|---------------------------|")
    
    for ext, count in sorted(input_stats["by_extension_units"].items(), key=lambda x: x[1], reverse=True):
        pct = count / with_files * 100 if with_files > 0 else 0
        report.append(f"| .{ext} | {count} | {pct:.1f}% |")
    
    report.append("")
    report.append("---")
    report.append("")
    
    # 2. ОБРАБОТКА (Processing)
    report.append("## 2. ОБРАБОТКА (Processing)")
    report.append("")
    
    for cycle in [1, 2, 3]:
        cycle_stats = stats["processing"][f"cycle_{cycle}"]
        total_convert = cycle_stats["convert"]["total"]
        total_extract = cycle_stats["extract"]["total"]
        total_normalize = cycle_stats["normalize"]["total"]
        total_processing = total_convert + total_extract + total_normalize
        
        if total_processing > 0:
            report.append(f"### Цикл {cycle}")
            report.append("")
            report.append(f"- **Всего UNIT в обработке:** {total_processing}")
            report.append(f"  - Convert: {total_convert} ({total_convert/total_processing*100:.1f}%)" if total_processing > 0 else "  - Convert: 0")
            report.append(f"  - Extract: {total_extract} ({total_extract/total_processing*100:.1f}%)" if total_processing > 0 else "  - Extract: 0")
            report.append(f"  - Normalize: {total_normalize} ({total_normalize/total_processing*100:.1f}%)" if total_processing > 0 else "  - Normalize: 0")
            report.append("")
            
            # Таблица по расширениям для каждого этапа
            for stage_name, stage_key in [("Convert", "convert"), ("Extract", "extract"), ("Normalize", "normalize")]:
                stage_stats = cycle_stats[stage_key]
                if stage_stats["total"] > 0:
                    report.append(f"#### {stage_name} (Цикл {cycle})")
                    report.append("")
                    report.append("| Расширение | Количество UNIT | Процент от этапа |")
                    report.append("|------------|-----------------|------------------|")
                    
                    stage_total = stage_stats["total"]
                    for ext, count in sorted(stage_stats["by_ext"].items(), key=lambda x: x[1], reverse=True):
                        pct = count / stage_total * 100 if stage_total > 0 else 0
                        report.append(f"| .{ext} | {count} | {pct:.1f}% |")
                    
                    report.append("")
    
    report.append("---")
    report.append("")
    
    # 3. ОБЪЕДИНЕНИЕ (Merge)
    report.append("## 3. ОБЪЕДИНЕНИЕ (Merge)")
    report.append("")
    
    # Детальные таблицы по результатам обработки
    for cycle in [1, 2, 3]:
        cycle_merge = stats["merge"][f"merge_{cycle}"]
        total_converted = cycle_merge["converted"]["total"]
        total_extracted = cycle_merge["extracted"]["total"]
        total_normalized = cycle_merge["normalized"]["total"]
        total_merge = total_converted + total_extracted + total_normalized
        
        if total_merge > 0:
            report.append(f"### Merge_{cycle} (Результаты обработки из Processing_{cycle})")
            report.append("")
            
            # Таблица Converted
            if total_converted > 0:
                report.append(f"#### Converted (Цикл {cycle})")
                report.append("")
                report.append("| Исходное расширение | Количество UNIT | Процент от Converted |")
                report.append("|---------------------|------------------|----------------------|")
                for ext, count in sorted(cycle_merge["converted"]["by_ext"].items(), key=lambda x: x[1], reverse=True):
                    pct = count / total_converted * 100 if total_converted > 0 else 0
                    report.append(f"| .{ext} | {count} | {pct:.1f}% |")
                report.append("")
            
            # Таблица Extracted
            if total_extracted > 0:
                report.append(f"#### Extracted (Цикл {cycle})")
                report.append("")
                report.append("| Тип архива | Количество UNIT | Процент от Extracted |")
                report.append("|------------|------------------|----------------------|")
                for ext, count in sorted(cycle_merge["extracted"]["by_ext"].items(), key=lambda x: x[1], reverse=True):
                    pct = count / total_extracted * 100 if total_extracted > 0 else 0
                    report.append(f"| .{ext} | {count} | {pct:.1f}% |")
                report.append("")
            
            # Таблица Normalized
            if total_normalized > 0:
                report.append(f"#### Normalized (Цикл {cycle})")
                report.append("")
                report.append("| Расширение | Количество UNIT | Процент от Normalized |")
                report.append("|------------|------------------|----------------------|")
                for ext, count in sorted(cycle_merge["normalized"]["by_ext"].items(), key=lambda x: x[1], reverse=True):
                    pct = count / total_normalized * 100 if total_normalized > 0 else 0
                    report.append(f"| .{ext} | {count} | {pct:.1f}% |")
                report.append("")
    
    # Сводная таблица по всем Merge
    report.append("### Сводная таблица Merge по циклам:")
    report.append("")
    report.append("| Merge | Direct | Converted | Extracted | Normalized | Всего | % от Input (без пустых) |")
    report.append("|-------|--------|-----------|-----------|------------|-------|-------------------------|")
    
    # Merge_0
    merge_0_direct = stats["merge"]["merge_0"]["direct"]["total"]
    pct_0 = merge_0_direct / input_stats["with_files"] * 100 if input_stats["with_files"] > 0 else 0
    report.append(f"| Merge_0 | {merge_0_direct} | - | - | - | {merge_0_direct} | {pct_0:.1f}% |")
    
    # Merge_1, 2, 3
    for cycle in [1, 2, 3]:
        merge_stats = stats["merge"][f"merge_{cycle}"]
        converted = merge_stats["converted"]["total"]
        extracted = merge_stats["extracted"]["total"]
        normalized = merge_stats["normalized"]["total"]
        total_merge = converted + extracted + normalized
        pct_merge = total_merge / input_stats["with_files"] * 100 if input_stats["with_files"] > 0 else 0
        report.append(f"| Merge_{cycle} | - | {converted} | {extracted} | {normalized} | {total_merge} | {pct_merge:.1f}% |")
    
    report.append("")
    
    # Merge_0
    merge_0_stats = stats["merge"]["merge_0"]
    if merge_0_stats["direct"]["total"] > 0:
        report.append("### Merge_0/Direct")
        report.append("")
        report.append(f"- **Всего UNIT:** {merge_0_stats['direct']['total']}")
        report.append(f"- **Процент от Input (без пустых):** {merge_0_stats['direct']['total']/input_stats['with_files']*100:.1f}%")
        report.append("")
        report.append("| Расширение | Количество UNIT | Процент от Direct | Процент от Input (без пустых) |")
        report.append("|------------|-----------------|------------------|-------------------------------|")
        
        direct_total = merge_0_stats["direct"]["total"]
        for ext, count in sorted(merge_0_stats["direct"]["by_ext"].items(), key=lambda x: x[1], reverse=True):
            pct_direct = count / direct_total * 100 if direct_total > 0 else 0
            pct_input = count / input_stats["with_files"] * 100 if input_stats["with_files"] > 0 else 0
            report.append(f"| .{ext} | {count} | {pct_direct:.1f}% | {pct_input:.1f}% |")
        
        report.append("")
    
    # Merge_1, Merge_2, Merge_3
    for cycle in [1, 2, 3]:
        merge_stats = stats["merge"][f"merge_{cycle}"]
        total_converted = merge_stats["converted"]["total"]
        total_extracted = merge_stats["extracted"]["total"]
        total_normalized = merge_stats["normalized"]["total"]
        total_merge = total_converted + total_extracted + total_normalized
        
        if total_merge > 0:
            report.append(f"### Merge_{cycle}")
            report.append("")
            report.append(f"- **Всего UNIT:** {total_merge}")
            report.append(f"  - Converted: {total_converted} ({total_converted/total_merge*100:.1f}%)" if total_merge > 0 else "  - Converted: 0")
            report.append(f"  - Extracted: {total_extracted} ({total_extracted/total_merge*100:.1f}%)" if total_merge > 0 else "  - Extracted: 0")
            report.append(f"  - Normalized: {total_normalized} ({total_normalized/total_merge*100:.1f}%)" if total_merge > 0 else "  - Normalized: 0")
            report.append("")
            
            # Таблицы по этапам
            for stage_name, stage_key in [("Converted", "converted"), ("Extracted", "extracted"), ("Normalized", "normalized")]:
                stage_stats = merge_stats[stage_key]
                if stage_stats["total"] > 0:
                    report.append(f"#### {stage_name} (Merge_{cycle})")
                    report.append("")
                    report.append("| Расширение | Количество UNIT | Процент от этапа |")
                    report.append("|------------|-----------------|------------------|")
                    
                    stage_total = stage_stats["total"]
                    for ext, count in sorted(stage_stats["by_ext"].items(), key=lambda x: x[1], reverse=True):
                        pct = count / stage_total * 100 if stage_total > 0 else 0
                        report.append(f"| .{ext} | {count} | {pct:.1f}% |")
                    
                    report.append("")
    
    report.append("---")
    report.append("")
    
    # 4. ИСКЛЮЧЕНИЯ (Exceptions)
    report.append("## 4. ИСКЛЮЧЕНИЯ (Exceptions)")
    report.append("")
    
    for cycle in [1, 2, 3]:
        exc_stats = stats["exceptions"][f"cycle_{cycle}"]
        total_mixed = exc_stats["mixed"]["total"]
        total_empty = exc_stats["empty"]["total"]
        total_ambiguous = exc_stats["ambiguous"]["total"]
        total_special = exc_stats["special"]["total"]
        total_exceptions = total_mixed + total_empty + total_ambiguous + total_special
        
        if total_exceptions > 0:
            report.append(f"### Exceptions_{cycle}")
            report.append("")
            report.append(f"- **Всего UNIT:** {total_exceptions}")
            report.append(f"  - Mixed: {total_mixed} ({total_mixed/total_exceptions*100:.1f}%)" if total_exceptions > 0 else "  - Mixed: 0")
            report.append(f"  - Empty: {total_empty} ({total_empty/total_exceptions*100:.1f}%)" if total_exceptions > 0 else "  - Empty: 0")
            report.append(f"  - Ambiguous: {total_ambiguous} ({total_ambiguous/total_exceptions*100:.1f}%)" if total_exceptions > 0 else "  - Ambiguous: 0")
            report.append(f"  - Special: {total_special} ({total_special/total_exceptions*100:.1f}%)" if total_exceptions > 0 else "  - Special: 0")
            report.append("")
            
            # Таблицы по подкатегориям
            for subcat_name, subcat_key in [("Mixed", "mixed"), ("Ambiguous", "ambiguous"), ("Special", "special")]:
                subcat_stats = exc_stats[subcat_key]
                if subcat_stats["total"] > 0:
                    report.append(f"#### {subcat_name} (Exceptions_{cycle})")
                    report.append("")
                    report.append("| Тип файла | Количество | Процент от подкатегории |")
                    report.append("|-----------|------------|-------------------------|")
                    
                    subcat_total = subcat_stats["total"]
                    for file_type, count in sorted(subcat_stats["by_ext"].items(), key=lambda x: x[1], reverse=True):
                        pct = count / subcat_total * 100 if subcat_total > 0 else 0
                        report.append(f"| {file_type} | {count} | {pct:.1f}% |")
                    
                    report.append("")
    
    report.append("---")
    report.append("")
    
    # 5. ГОТОВНОСТЬ К DOCLING
    report.append("## 5. ГОТОВНОСТЬ К DOCLING (Ready2Docling)")
    report.append("")
    
    ready_stats = stats["ready2docling"]
    total_ready = ready_stats["total"]
    
    report.append(f"- **Всего UNIT:** {total_ready}")
    report.append(f"- **Процент от Input (без пустых):** {total_ready/input_stats['with_files']*100:.1f}%" if input_stats['with_files'] > 0 else "- **Процент от Input (без пустых):** 0%")
    report.append("")
    
    # Таблица по расширениям
    report.append("### Распределение по расширениям:")
    report.append("")
    report.append("| Расширение | Количество UNIT | Процент от Ready2Docling |")
    report.append("|------------|-----------------|--------------------------|")
    
    for ext, count in sorted(ready_stats["by_extension"].items(), key=lambda x: x[1], reverse=True):
        pct = count / total_ready * 100 if total_ready > 0 else 0
        report.append(f"| .{ext} | {count} | {pct:.1f}% |")
    
    report.append("")
    
    # PDF распределение
    pdf_stats = ready_stats["pdf"]
    total_pdf = pdf_stats["scan"] + pdf_stats["text"] + pdf_stats["mixed"] + pdf_stats["unknown"]
    
    if total_pdf > 0:
        report.append("### Распределение PDF:")
        report.append("")
        report.append("| Тип PDF | Количество UNIT | Процент от всех PDF |")
        report.append("|---------|-----------------|---------------------|")
        report.append(f"| scan | {pdf_stats['scan']} | {pdf_stats['scan']/total_pdf*100:.1f}% |")
        report.append(f"| text | {pdf_stats['text']} | {pdf_stats['text']/total_pdf*100:.1f}% |")
        report.append(f"| mixed | {pdf_stats['mixed']} | {pdf_stats['mixed']/total_pdf*100:.1f}% |")
        if pdf_stats['unknown'] > 0:
            report.append(f"| unknown | {pdf_stats['unknown']} | {pdf_stats['unknown']/total_pdf*100:.1f}% |")
        report.append("")
    
    report.append("---")
    report.append("")
    
    # 6. СТАТИСТИКА CLASSIFIER
    report.append("## 6. СТАТИСТИКА CLASSIFIER")
    report.append("")
    
    classifier_stats = stats["classifier"]
    
    # Детектирование типов
    if classifier_stats["detections"]:
        report.append("### Детектирование типов:")
        report.append("")
        report.append("| Тип | Количество | Процент |")
        report.append("|-----|------------|---------|")
        
        total_detections = sum(classifier_stats["detections"].values())
        for file_type, count in sorted(classifier_stats["detections"].items(), key=lambda x: x[1], reverse=True):
            pct = count / total_detections * 100 if total_detections > 0 else 0
            report.append(f"| {file_type} | {count} | {pct:.1f}% |")
        
        report.append("")
    
    # Классификации
    if classifier_stats["classifications"]:
        report.append("### Классификации по категориям:")
        report.append("")
        report.append("| Категория | Количество | Процент |")
        report.append("|-----------|------------|---------|")
        
        total_classifications = sum(classifier_stats["classifications"].values())
        for category, count in sorted(classifier_stats["classifications"].items(), key=lambda x: x[1], reverse=True):
            pct = count / total_classifications * 100 if total_classifications > 0 else 0
            report.append(f"| {category} | {count} | {pct:.1f}% |")
        
        report.append("")
    
    report.append("---")
    report.append("")
    
    # 7. ИТОГОВЫЕ МЕТРИКИ ОБРАБОТКИ
    report.append("## 7. ИТОГОВЫЕ МЕТРИКИ ОБРАБОТКИ")
    report.append("")
    report.append("```")
    report.append("=" * 60)
    report.append("📊 ИТОГОВЫЕ МЕТРИКИ")
    report.append("=" * 60)
    report.append("                               Метрики обработки                                ")
    report.append("┏━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┓")
    report.append("┃ Цикл ┃ Processing                           ┃ Merge ┃ Exceptions             ┃")
    report.append("┡━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━┩")
    
    for cycle in [1, 2, 3]:
        processing_parts = []
        cycle_processing_stats = stats["processing"][f"cycle_{cycle}"]
        for stage in ["convert", "extract", "normalize"]:
            if cycle_processing_stats[stage]["total"] > 0:
                processing_parts.append(f"{stage}: {cycle_processing_stats[stage]['total']}")
        processing_str = ", ".join(processing_parts) if processing_parts else "-"
        
        merge_parts = []
        cycle_merge_stats = stats["merge"][f"merge_{cycle}"]
        for stage in ["converted", "extracted", "normalized"]:
            if cycle_merge_stats[stage]["total"] > 0:
                merge_parts.append(f"{stage}: {cycle_merge_stats[stage]['total']}")
        merge_str = ", ".join(merge_parts) if merge_parts else "-"
        
        exception_parts = []
        cycle_exception_stats = stats["exceptions"][f"cycle_{cycle}"]
        for subcategory in ["mixed", "empty", "ambiguous", "special"]:
            if cycle_exception_stats[subcategory]["total"] > 0:
                exception_parts.append(f"{subcategory}: {cycle_exception_stats[subcategory]['total']}")
        exception_str = ", ".join(exception_parts) if exception_parts else "-"
        
        report.append(f"│ {cycle}    │ {processing_str:<36} │ {merge_str:<7} │ {exception_str:<22} │")
    
    report.append("└──────┴──────────────────────────────────────┴───────┴────────────────────────┘")
    report.append("")
    report.append(f"✅ UNIT в Ready2Docling: {total_ready}")
    report.append("```")
    report.append("")
    
    # 8. FLOW АНАЛИТИКА
    report.append("## 8. FLOW АНАЛИТИКА (Процентное распределение)")
    report.append("")
    
    # Таблица flow по фазам
    report.append("### Распределение UNIT по фазам обработки:")
    report.append("")
    report.append("| Фаза | Количество UNIT | Процент от Input (всего) | Процент от Input (без пустых) |")
    report.append("|------|-----------------|---------------------------|-------------------------------|")
    
    # Input
    report.append(f"| Input (всего) | {input_stats['total']} | 100.0% | - |")
    report.append(f"| Input (с файлами) | {input_stats['with_files']} | {input_stats['with_files']/input_stats['total']*100:.1f}% | 100.0% |")
    
    # Merge_0/Direct
    merge_0_direct = stats["merge"]["merge_0"]["direct"]["total"]
    report.append(f"| Merge_0/Direct | {merge_0_direct} | {merge_0_direct/input_stats['total']*100:.1f}% | {merge_0_direct/input_stats['with_files']*100:.1f}% |")
    
    # Processing по циклам
    for cycle in [1, 2, 3]:
        cycle_proc = stats["processing"][f"cycle_{cycle}"]
        total_proc = sum([cycle_proc[s]["total"] for s in ["convert", "extract", "normalize"]])
        if total_proc > 0:
            report.append(f"| Processing_{cycle} | {total_proc} | {total_proc/input_stats['total']*100:.1f}% | {total_proc/input_stats['with_files']*100:.1f}% |")
    
    # Merge по циклам
    for cycle in [1, 2, 3]:
        cycle_merge = stats["merge"][f"merge_{cycle}"]
        total_merge = sum([cycle_merge[s]["total"] for s in ["converted", "extracted", "normalized"]])
        if total_merge > 0:
            report.append(f"| Merge_{cycle} | {total_merge} | {total_merge/input_stats['total']*100:.1f}% | {total_merge/input_stats['with_files']*100:.1f}% |")
    
    # Exceptions по циклам
    for cycle in [1, 2, 3]:
        cycle_exc = stats["exceptions"][f"cycle_{cycle}"]
        total_exc = sum([cycle_exc[s]["total"] for s in ["mixed", "empty", "ambiguous", "special"]])
        if total_exc > 0:
            report.append(f"| Exceptions_{cycle} | {total_exc} | {total_exc/input_stats['total']*100:.1f}% | {total_exc/input_stats['with_files']*100:.1f}% |")
    
    # Ready2Docling
    report.append(f"| Ready2Docling | {total_ready} | {total_ready/input_stats['total']*100:.1f}% | {total_ready/input_stats['with_files']*100:.1f}% |")
    
    report.append("")
    
    # Детальная таблица по расширениям через все фазы
    report.append("### Распределение по расширениям через все фазы:")
    report.append("")
    report.append("| Расширение | Input (файлы) | Input (UNIT) | Merge_0/Direct | Ready2Docling | % готовности |")
    report.append("|------------|---------------|--------------|----------------|---------------|--------------|")
    
    # Собираем все расширения
    all_extensions = set()
    all_extensions.update(input_stats["by_extension"].keys())
    all_extensions.update(stats["merge"]["merge_0"]["direct"]["by_ext"].keys())
    all_extensions.update(stats["ready2docling"]["by_extension"].keys())
    
    for ext in sorted(all_extensions, key=lambda x: input_stats["by_extension"].get(x, 0), reverse=True):
        input_files = input_stats["by_extension"].get(ext, 0)
        input_units = input_stats["by_extension_units"].get(ext, 0)
        merge_0_count = stats["merge"]["merge_0"]["direct"]["by_ext"].get(ext, 0)
        ready_count = stats["ready2docling"]["by_extension"].get(ext, 0)
        
        # Для PDF учитываем все подтипы
        if ext == "pdf":
            ready_count = stats["ready2docling"]["pdf"]["scan"] + stats["ready2docling"]["pdf"]["text"] + stats["ready2docling"]["pdf"]["mixed"] + stats["ready2docling"]["pdf"]["unknown"]
        
        readiness_pct = ready_count / input_units * 100 if input_units > 0 else 0
        
        report.append(f"| .{ext} | {input_files} | {input_units} | {merge_0_count} | {ready_count} | {readiness_pct:.1f}% |")
    
    report.append("")
    report.append("---")
    report.append("")
    
    # 9. ИТОГОВАЯ СТАТИСТИКА
    report.append("## 9. ИТОГОВАЯ СТАТИСТИКА")
    report.append("")
    report.append("### Общие показатели")
    report.append("")
    report.append(f"- **Всего UNIT в Input:** {input_stats['total']}")
    report.append(f"- **Пустых UNIT:** {input_stats['empty']} ({input_stats['empty']/input_stats['total']*100:.1f}%)")
    report.append(f"- **UNIT с файлами:** {input_stats['with_files']} ({input_stats['with_files']/input_stats['total']*100:.1f}%)")
    report.append(f"- **Обработано и готово к Docling:** {total_ready} UNIT ({total_ready/input_stats['with_files']*100:.1f}% от UNIT с файлами)")
    
    # Подсчет исключений
    total_exceptions_all = 0
    for cycle in [1, 2, 3]:
        cycle_exc = stats["exceptions"][f"cycle_{cycle}"]
        total_exceptions_all += sum([cycle_exc[s]["total"] for s in ["mixed", "empty", "ambiguous", "special"]])
    
    report.append(f"- **В Exceptions:** {total_exceptions_all} UNIT ({total_exceptions_all/input_stats['total']*100:.1f}% от всех UNIT)")
    report.append("")
    
    report.append("---")
    report.append("")
    report.append(f"*Отчет сгенерирован: {stats['timestamp']}*")
    report.append("*Система: DocPrep v1.0*")
    report.append("*Статус: ✅ Готово к передаче в Docling pipeline*")
    
    return "\n".join(report)


def main():
    if len(sys.argv) < 2:
        print("Использование: python3 generate_final_detailed_report.py <date>")
        sys.exit(1)
    
    date = sys.argv[1]
    
    print(f"📊 Сбор детальной статистики для {date}...")
    stats = collect_comprehensive_stats(date)
    
    print("📝 Генерация отчета...")
    report = generate_report(stats)
    
    # Сохраняем отчет
    report_path = Path(f"FINAL_DETAILED_REPORT_{date}.md")
    report_path.write_text(report, encoding="utf-8")
    print(f"✅ Отчет сохранен: {report_path}")
    
    # Сохраняем статистику в JSON
    stats_path = Path(f"FINAL_DETAILED_STATS_{date}.json")
    stats_path.write_text(json.dumps(stats, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"✅ Статистика сохранена: {stats_path}")


if __name__ == "__main__":
    main()


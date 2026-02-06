#!/usr/bin/env python3
"""
Скрипт для перераспределения PDF units по подпапкам text/scan/mixed
на основе needs_ocr в manifest.
"""
import json
import shutil
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from docprep.core.config import get_data_paths


def redistribute_pdf(date: str):
    """Перераспределяет PDF units для указанной даты."""
    data_paths = get_data_paths(date)
    ready = data_paths['ready2docling']

    print(f'ПЕРЕРАСПРЕДЕЛЕНИЕ PDF ДЛЯ {date}')
    print('=' * 50)

    pdf_text = ready / 'pdf' / 'text'
    pdf_scan = ready / 'pdf' / 'scan'
    pdf_mixed = ready / 'pdf' / 'mixed'

    pdf_scan.mkdir(parents=True, exist_ok=True)
    pdf_mixed.mkdir(parents=True, exist_ok=True)

    stats = {'text': 0, 'scan': 0, 'mixed': 0, 'updated': 0}

    if not pdf_text.exists():
        print('Нет pdf/text директории')
        return

    units = list(pdf_text.glob('UNIT_*'))
    print(f'Обрабатываю {len(units)} units...')

    for unit_path in units:
        manifest_path = unit_path / 'manifest.json'
        if not manifest_path.exists():
            continue

        with open(manifest_path, 'r') as f:
            manifest = json.load(f)

        needs_ocr_list = []
        for file_info in manifest.get('files', []):
            if file_info.get('detected_type') == 'pdf':
                needs_ocr = file_info.get('needs_ocr')
                if needs_ocr is not None:
                    needs_ocr_list.append(needs_ocr)

        if not needs_ocr_list:
            stats['text'] += 1
            continue

        all_need_ocr = all(needs_ocr_list)
        all_has_text = all(not x for x in needs_ocr_list)

        if all_need_ocr:
            new_route = 'pdf_scan'
            target_dir = pdf_scan
            stats['scan'] += 1
        elif all_has_text:
            stats['text'] += 1
            continue
        else:
            new_route = 'pdf_mixed'
            target_dir = pdf_mixed
            stats['mixed'] += 1

        current_route = manifest.get('processing', {}).get('route')
        if current_route != new_route:
            manifest['processing']['route'] = new_route
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2, ensure_ascii=False)
            stats['updated'] += 1

        if new_route != 'pdf_text':
            new_unit_path = target_dir / unit_path.name
            if new_unit_path.exists():
                shutil.rmtree(new_unit_path)
            shutil.move(str(unit_path), str(new_unit_path))

    print(f'text: {stats["text"]} | scan: {stats["scan"]} | mixed: {stats["mixed"]}')
    print(f'Обновлено manifest: {stats["updated"]}')

    print('\nФинальная структура:')
    for subdir in ['text', 'scan', 'mixed']:
        path = ready / 'pdf' / subdir
        count = len(list(path.glob('UNIT_*'))) if path.exists() else 0
        print(f'  pdf/{subdir}: {count}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Перераспределение PDF units')
    parser.add_argument('--date', type=str, required=True, help='Дата в формате YYYY-MM-DD')
    args = parser.parse_args()
    redistribute_pdf(args.date)

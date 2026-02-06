#!/usr/bin/env python3
"""
Скрипт для инициализации структуры директорий Data согласно PRD.

Использование:
    python scripts/init_data_structure.py [--date YYYY-MM-DD] [--data-dir ./Data]
"""
import sys
from pathlib import Path

# Добавляем путь к модулю
sys.path.insert(0, str(Path(__file__).parent.parent))

from docprep.core.config import init_directory_structure, DATA_BASE_DIR
import argparse


def main():
    parser = argparse.ArgumentParser(description="Инициализация структуры директорий Data")
    parser.add_argument(
        "--date",
        type=str,
        help="Дата в формате YYYY-MM-DD (опционально, создаст структуру для конкретной даты)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_BASE_DIR,
        help=f"Базовая директория Data (по умолчанию: {DATA_BASE_DIR})",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Подробный вывод"
    )

    args = parser.parse_args()

    if args.verbose:
        print(f"Инициализация структуры директорий")
        print(f"Базовая директория: {args.data_dir}")
        if args.date:
            print(f"Дата: {args.date}")

    try:
        init_directory_structure(args.data_dir, args.date)
        print("✓ Структура директорий успешно создана")
        
        if args.date:
            print(f"✓ Структура для даты {args.date} готова к использованию")
        else:
            print("✓ Базовая структура готова к использованию")
            print("\nДля создания структуры для конкретной даты используйте:")
            print(f"  python {sys.argv[0]} --date YYYY-MM-DD")
    except Exception as e:
        print(f"✗ Ошибка при создании структуры: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


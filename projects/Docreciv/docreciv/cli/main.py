#!/usr/bin/env python3
"""
Основной модуль CLI для препроцессинга документов.

Содержит класс PreprocessingTestCLI с меню и роутингом команд.
Импортирует все handlers из соответствующих модулей.
"""

import sys
import json
import time
import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

# Настройка PYTHONPATH для доступа к sync_db
project_root = Path(__file__).parent.parent  # /root/winners_preprocessor (теперь на уровень выше)
sys.path.insert(0, str(project_root))

# Загрузка переменных окружения из .env файла
def load_env_file():
    """Загружает переменные окружения из .env файла если он существует."""
    env_file = Path(__file__).parent.parent / ".env"  # /home/pak/projects/Docreciv/.env
    if env_file.exists():
        try:
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        # Удаляем кавычки если они есть
                        value = value.strip('"').strip("'")
                        os.environ[key] = value
        except Exception as e:
            print(f"⚠️  Не удалось загрузить .env файл: {e}")

# Загружаем переменные окружения
load_env_file()

# Импорт только необходимых handlers
try:
    # Попытка относительного импорта
    from .handlers.load_handlers import (
        handle_sync_protocols,
        handle_download_protocols,
        handle_check_input_files
    )
    from .handlers.utils_handlers import (
        handle_cleanup_test_data,
        handle_create_test_files,
        handle_check_infrastructure
    )
except ImportError:
    # Fallback для абсолютного импорта
    from docreciv.cli.handlers.load_handlers import (
        handle_sync_protocols,
        handle_download_protocols,
        handle_check_input_files
    )
    from docreciv.cli.handlers.utils_handlers import (
        handle_cleanup_test_data,
        handle_create_test_files,
        handle_check_infrastructure
    )

# Импорт конфигурации
try:
    from .config import MENU_CATEGORIES, MENU_MAPPING, MENU_TITLE, MENU_SEPARATOR, CLI_SETTINGS
except ImportError:
    from docreciv.cli.config import MENU_CATEGORIES, MENU_MAPPING, MENU_TITLE, MENU_SEPARATOR, CLI_SETTINGS


class PreprocessingTestCLI:
    """CLI для тестирования препроцессинга."""

    def __init__(self):
        self.metrics = None
        self.session_id = None

        # Импортируем time и json для использования в handlers
        self.time = time
        self.json = json
        
        # Инициализация директорий из конфигурации
        from docreciv.core.config import get_config
        config = get_config()
        self.INPUT_DIR = config.downloader.output_dir
        self.OUTPUT_DIR = config.downloader.output_dir  # Используем тот же путь для output

    def show_menu(self):
        """Показывает главное меню."""
        print(f"\n{MENU_SEPARATOR}")
        print(MENU_TITLE)
        print(MENU_SEPARATOR)

        for category_key, category_info in MENU_CATEGORIES.items():
            print(f"\n=== {category_info['title']} ===")
            for item in category_info['items']:
                print(item)

        print("\n0. Выход")
        print(f"\n{MENU_SEPARATOR}")

    def run(self):
        """Главный цикл CLI."""
        print("\n🚀 ЗАПУСК CLI ТЕСТИРОВАНИЯ ПРЕПРОЦЕССИНГА")
        print(MENU_SEPARATOR)

        while True:
            try:
                self.show_menu()
                choice = input(CLI_SETTINGS["prompt_template"]).strip()

                if choice == "0":
                    print("👋 Выход...")
                    break

                # Получаем информацию о выбранном пункте меню
                choice_num = int(choice) if choice.isdigit() else None
                if choice_num and choice_num in MENU_MAPPING:
                    category, function_name = MENU_MAPPING[choice_num]

                    # Получаем функцию из соответствующего модуля
                    handler_function = self._get_handler_function(category, function_name)

                    if handler_function:
                        # Вызываем функцию с экземпляром CLI
                        handler_function(self)
                    else:
                        print(f"❌ Функция {function_name} не найдена в категории {category}")
                else:
                    print("❌ Неверный выбор. Попробуйте снова.")

                input("\n⏎ Нажмите Enter для продолжения...")

            except KeyboardInterrupt:
                print("\n\n👋 Выход...")
                break
            except Exception as e:
                print(f"\n❌ Ошибка: {e}")
                import traceback
                traceback.print_exc()
                input("\n⏎ Нажмите Enter для продолжения...")

    def _get_handler_function(self, category: str, function_name: str):
        """Получает функцию handler по категории и имени."""
        # Маппинг категорий к модулям (только актуальные handlers)
        category_mapping = {
            "load": {
                "sync_protocols": handle_sync_protocols,
                "download_protocols": handle_download_protocols,
                "check_input_files": handle_check_input_files
            },
            "utils": {
                "cleanup_test_data": handle_cleanup_test_data,
                "create_test_files": handle_create_test_files,
                "check_infrastructure": handle_check_infrastructure
            }
        }

        if category in category_mapping and function_name in category_mapping[category]:
            return category_mapping[category][function_name]

        return None


def main():
    """Точка входа для CLI."""
    cli = PreprocessingTestCLI()
    cli.run()


if __name__ == "__main__":
    main()
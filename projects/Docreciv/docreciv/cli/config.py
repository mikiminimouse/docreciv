"""
Конфигурация CLI приложения.
"""

# Настройки меню
MENU_TITLE = "=== ПРЕПРОЦЕССИНГ ДОКУМЕНТОВ - CLI ТЕСТИРОВАНИЯ ==="
MENU_SEPARATOR = "=" * 60

# Упрощенные категории меню с их пунктами
MENU_CATEGORIES = {
    "load": {
        "title": "ЗАГРУЗКА И ПОДГОТОВКА ДАННЫХ",
        "items": [
            "1. Синхронизация протоколов из MongoDB",
            "2. Скачивание протоколов за дату",
            "3. Проверка доступности файлов в INPUT_DIR"
        ]
    },
    "utils": {
        "title": "СЛУЖЕБНЫЕ ФУНКЦИИ",
        "items": [
            "25. Очистка тестовых данных",
            "26. Создание тестовых файлов",
            "27. Проверка инфраструктуры"
        ]
    }
}

# Упрощенный маппинг номеров пунктов меню к категориям и функциям
MENU_MAPPING = {
    # Load handlers (1-3)
    1: ("load", "sync_protocols"),
    2: ("load", "download_protocols"),
    3: ("load", "check_input_files"),

    # Utils handlers (25-27)
    25: ("utils", "cleanup_test_data"),
    26: ("utils", "create_test_files"),
    27: ("utils", "check_infrastructure")
}

# Настройки CLI
CLI_SETTINGS = {
    "max_menu_choice": 27,
    "exit_choice": 0,
    "prompt_template": "\nВыберите действие [0-27]: "
}
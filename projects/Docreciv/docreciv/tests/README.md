# Тесты для компонентов Preprocessing

## Структура тестов

```
tests/
├── unit tests (файлы с префиксом test_)
│   ├── test_enhanced_components.py - Тесты для улучшенных компонентов sync_db и downloader
│   ├── test_protocol_sync.py - Тесты синхронизации протоколов
│   ├── test_sync_integration.py - Интеграционные тесты синхронизации
│   ├── test_cycles.py - Тесты циклов обработки
│   ├── test_manifest_v2.py - Тесты нового манифеста
│   ├── test_state_machine.py - Тесты машины состояний
│   └── test_cli_functions.py - Тесты функций CLI
│
├── integration/
│   ├── test_20_units.py - Интеграционный тест на 20 единиц
│   └── test_full_pipeline.py - Полный тест pipeline обработки
```

## Описание тестов

### Unit тесты
- **test_enhanced_components.py** - Тесты для улучшенных компонентов синхронизации и загрузки
- **test_protocol_sync.py** - Тесты функциональности синхронизации протоколов
- **test_sync_integration.py** - Интеграционные тесты синхронизации с MongoDB
- **test_cycles.py** - Тесты циклов обработки документов
- **test_manifest_v2.py** - Тесты нового формата манифеста
- **test_state_machine.py** - Тесты машины состояний обработки
- **test_cli_functions.py** - Тесты функций командной строки

### Интеграционные тесты
- **test_20_units.py** - Автоматический тест CLI на 20 единицах обработки
- **test_full_pipeline.py** - Тестирование полного pipeline обработки документов

## Запуск тестов

### Запуск всех тестов
```bash
cd /root/winners_preprocessor/preprocessing
python -m pytest tests/ -v
```

### Запуск unit тестов
```bash
python -m pytest tests/test_*.py -v
```

### Запуск интеграционных тестов
```bash
python -m pytest tests/integration/ -v
```

### Запуск конкретного теста
```bash
python -m pytest tests/test_enhanced_components.py::test_imports -v
```

## Архивные тесты

Устаревшие тесты и отчеты перемещены в:
`/root/winners_preprocessor/archive/preprocessing_test_reports/`

Включают:
- Отчеты о багфиксе и тестировании
- Устаревшие тестовые скрипты
- Промежуточные отчеты о работе

## Активные компоненты для тестирования

1. **sync_db** - компонент синхронизации протоколов
2. **downloader** - компонент загрузки документов
3. **scheduler** - компонент планирования задач
4. **cli** - интерфейс командной строки

## Переменные окружения для тестов

Тесты используют те же переменные окружения, что и основное приложение:
- `MONGO_SERVER` - адрес удаленной MongoDB
- `MONGO_USER` - пользователь для удаленной MongoDB
- `MONGO_PASSWORD` - пароль для удаленной MongoDB
- `MONGO_METADATA_SERVER` - адрес локальной MongoDB
- `MONGO_METADATA_USER` - пользователь для локальной MongoDB
- `MONGO_METADATA_PASSWORD` - пароль для локальной MongoDB

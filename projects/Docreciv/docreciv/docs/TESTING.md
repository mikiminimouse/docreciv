# Тестирование компонентов Preprocessing

## Обзор

Этот документ описывает структуру тестов и процедуры их запуска для компонентов receiver.

## Структура тестов

### Unit тесты
Unit тесты расположены в директории `tests/` и предназначены для тестирования отдельных компонентов:

- `test_enhanced_components.py` - Тесты улучшенных компонентов синхронизации и загрузки
- `test_protocol_sync.py` - Тесты синхронизации протоколов
- `test_sync_integration.py` - Интеграционные тесты синхронизации с MongoDB
- `test_cycles.py` - Тесты циклов обработки документов
- `test_manifest_v2.py` - Тесты нового формата манифеста
- `test_state_machine.py` - Тесты машины состояний
- `test_cli_functions.py` - Тесты функций командной строки

### Интеграционные тесты
Интеграционные тесты расположены в директории `tests/integration/` и тестируют взаимодействие компонентов:

- `test_20_units.py` - Автоматический тест CLI на 20 единицах обработки
- `test_full_pipeline.py` - Тестирование полного pipeline обработки документов

## Запуск тестов

### Требования
Для запуска тестов необходимо:
1. Активировать виртуальное окружение
2. Убедиться, что все зависимости установлены
3. Настроить переменные окружения

### Активация виртуального окружения
```bash
cd /root/winners_preprocessor/final_preprocessing/receiver
source activate_venv.sh
```

### Запуск всех тестов
```bash
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
python -m pytest tests/test_enhanced_components.py -v
python -m pytest tests/test_enhanced_components.py::test_imports -v
```

### Запуск тестов с покрытием
```bash
python -m pytest tests/ --cov=receiver --cov-report=html
```

## Активные компоненты для тестирования

### 1. sync_db
Компонент синхронизации протоколов из удаленной MongoDB.

**Ключевые файлы для тестирования:**
- `sync_db/enhanced_service.py`
- `sync_db/health_checks.py`
- `sync_db/analytics.py`

**Тесты:**
- `tests/test_enhanced_components.py`
- `tests/test_protocol_sync.py`
- `tests/test_sync_integration.py`

### 2. downloader
Компонент загрузки документов по URL.

**Ключевые файлы для тестирования:**
- `downloader/enhanced_service.py`
- `downloader/utils.py`

**Тесты:**
- `tests/test_enhanced_components.py`

### 3. scheduler
Компонент планирования задач.

**Ключевые файлы для тестирования:**
- `scheduler/jobs.py`
- `scheduler/main.py`

### 4. cli
Интерфейс командной строки.

**Ключевые файлы для тестирования:**
- `cli/main.py`
- `cli/handlers/*.py`

**Тесты:**
- `tests/test_cli_functions.py`
- `tests/integration/test_20_units.py`
- `tests/integration/test_full_pipeline.py`

## Переменные окружения для тестов

Тесты используют те же переменные окружения, что и основное приложение:

```bash
# Удаленная MongoDB (для протоколов)
MONGO_SERVER=192.168.0.46:8635
MONGO_USER=readProtocols223
MONGO_PASSWORD=your_password
MONGO_SSL_CERT=/path/to/certificate.crt

# Локальная MongoDB (для метаданных)
MONGO_METADATA_SERVER=localhost:27018
LOCAL_MONGO_SERVER=localhost:27018
MONGO_METADATA_USER=admin
MONGO_METADATA_PASSWORD=your_password
MONGO_METADATA_DB=docling_metadata
```

## Метрики и логирование

### Локальные метрики
При недоступности MongoDB метрики сохраняются локально в:
`receiver/local_metrics/`

### Логирование тестов
Тесты используют стандартное логирование Python:
- INFO - основная информация
- WARNING - предупреждения
- ERROR - ошибки
- DEBUG - отладочная информация

## Отладка тестов

### Просмотр логов
```bash
# Просмотр логов тестов
tail -f /var/log/receiver/test.log

# Просмотр локальных метрик
ls -la receiver/local_metrics/
```

### Отладка конкретного теста
```bash
# Запуск теста с подробным выводом
python -m pytest tests/test_enhanced_components.py -v -s

# Запуск теста с отладочным логированием
python -m pytest tests/test_enhanced_components.py -v --log-cli-level=DEBUG
```

## Создание новых тестов

### Структура тестового файла
```python
#!/usr/bin/env python3
"""
Краткое описание теста.
"""

import sys
from pathlib import Path

# Добавляем путь к проекту
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_functionality():
    """Тест функциональности."""
    # Подготовка данных
    # Выполнение теста
    # Проверка результатов
    assert condition, "Описание ошибки"

def main():
    """Запуск тестов."""
    # Код запуска тестов

if __name__ == "__main__":
    main()
```

### Рекомендации по созданию тестов
1. Использовать описательные имена функций тестов
2. Добавлять комментарии к сложным участкам кода
3. Проверять граничные условия
4. Использовать fixtures для подготовки данных
5. Писать независимые тесты

## Интеграция с CI/CD

### Автоматический запуск тестов
Тесты автоматически запускаются при:
1. Коммите в репозиторий
2. Создании pull request
3. Ручном запуске через CLI

### Отчеты о тестировании
После выполнения тестов генерируются отчеты:
- HTML отчет о покрытии кода
- XML отчет о результатах тестов
- JSON отчет о метриках

## Устранение неполадок

### Распространенные проблемы

1. **"ModuleNotFoundError"**
   - Проверьте, что виртуальное окружение активировано
   - Убедитесь, что все зависимости установлены
   - Проверьте PYTHONPATH

2. **"ConnectionError" при тестировании MongoDB**
   - Проверьте подключение к MongoDB
   - Убедитесь, что VPN активен (для удаленной MongoDB)
   - Проверьте учетные данные

3. **"ImportError" при импорте модулей**
   - Проверьте структуру импортов
   - Убедитесь, что пути к модулям корректны
   - Проверьте наличие __init__.py файлов

### Логи и диагностика
```bash
# Проверка логов MongoDB
tail -f /var/log/mongodb/mongod.log

# Проверка логов приложения
tail -f /var/log/receiver/app.log

# Проверка логов тестов
tail -f /var/log/receiver/test.log
```

## Дальнейшее развитие

### Планы по улучшению тестирования
1. Добавление тестов для scheduler компонента
2. Расширение интеграционных тестов
3. Добавление тестов производительности
4. Автоматизация генерации отчетов
5. Интеграция с системами мониторинга

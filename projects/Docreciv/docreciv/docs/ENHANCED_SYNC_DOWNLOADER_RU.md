# Улучшенные компоненты синхронизации и загрузки

## Обзор

Документ описывает улучшенные компоненты синхронизации и загрузки для Docreciv.
Компоненты обеспечивают улучшенную обработку ошибок, комплексные проверки работоспособности,
расширенную аналитику и асинхронную загрузку с высокой производительностью.

**Версия**: 1.1.0 (январь 2026)
**Статус**: Production Ready ✅

---

## Улучшенная служба SyncDB

### Особенности

1. **Использование `loadDate`**: Миграция с `publicationDateTime` на `loadDate` (40x больше данных)
2. **Улучшенная обработка ошибок**: Комплексная обработка с категоризацией
3. **Расширенная статистика**: Подробный сбор метрик
4. **Пакетная обработка**: Эффективная обработка с настраиваемыми размерами пакетов
5. **Управление подключениями**: Надёжное управление с тайм-аутами и retry

### Основные классы

#### EnhancedSyncService
Основной класс службы для синхронизации протоколов:
- `sync_protocols_for_date()`: Синхронизация за определённую дату
- `sync_protocols_for_date_range()`: Синхронизация за диапазон дат
- `sync_daily_updates()`: Ежедневная синхронизация последних протоколов
- `sync_full_collection()`: Полная синхронизация за указанный период

#### EnhancedSyncResult
Расширенный объект результата с подробными метриками:
- Базовые метрики (просмотрено, вставлено, пропущено, ошибки)
- Статистика распределения URL
- Анализ типов вложений
- Категоризация ошибок

### Использование

```bash
# Из командной строки
python3 -m docreciv.sync_db.enhanced_service sync-date --date 2026-01-24
python3 -m docreciv.sync_db.enhanced_service sync-range --start-date 2026-01-01 --end-date 2026-01-31
python3 -m docreciv.sync_db.enhanced_service sync-daily
```

---

## Async режим загрузки

### Особенности

1. **Высокая производительность**: До 4.25 files/s
2. **Параллельная обработка**: До 100 одновременных запросов
3. **Высокий success rate**: 99.1% в среднем
4. **Graceful error handling**: Обработка timeout и connection errors

### Конфигурация Async режима

```bash
# Async downloader settings
export ASYNC_MAX_CONCURRENT_REQUESTS=100
export ASYNC_MAX_CONCURRENT_PROTOCOLS=50
export ASYNC_LIMIT_PER_HOST=50
export ASYNC_CONNECTION_TIMEOUT=120
export ASYNC_READ_TIMEOUT=300
```

### Использование Async режима

```python
import asyncio
from docreciv.downloader.async_service import AsyncioProtocolDownloader
from docreciv.downloader.models import DownloadRequest
from datetime import datetime

async def download():
    async with AsyncioProtocolDownloader() as downloader:
        result = await downloader.process_download_request(
            DownloadRequest(
                from_date=datetime(2026, 1, 24),
                to_date=datetime(2026, 1, 24),
                max_units_per_run=2000
            )
        )
        return result

result = asyncio.run(download())
```

### Производительность Async режима

| Дата | Протоколов | Файлов | Success Rate | Throughput | Duration |
|------|-----------|--------|-------------|-----------|----------|
| 2026-01-22 | 1,703 | 1,841 | 99.4% | 3.31 f/s | 9.3 мин |
| 2026-01-24 | 1,665 | 1,823 | 98.2% | 4.25 f/s | 7.1 мин |
| 2026-01-27 | 1,502 | 1,663 | 99.8% | 4.38 f/s | 6.3 мин |

**Подробнее**: [ASYNC_TEST_RESULTS_SUMMARY.md](ASYNC_TEST_RESULTS_SUMMARY.md)

---

## Модуль проверки работоспособности

### Особенности

1. **Комплексный мониторинг**: Проверки VPN, MongoDB, среды
2. **Подробная отчётность**: Структурированные отчёты со значками состояния
3. **Индивидуальные проверки**: Возможность выполнения определённых проверок
4. **Агрегированное состояние**: Общее состояние работоспособности системы

### Основные функции

- `check_vpn_connectivity()`: Проверка VPN к zakupki.gov.ru
- `check_remote_mongodb_connectivity()`: Проверка подключения к удалённой MongoDB
- `check_local_mongodb_connectivity()`: Проверка подключения к локальной MongoDB
- `check_environment_variables()`: Проверка необходимых переменных среды
- `run_comprehensive_health_check()`: Выполнение всех проверок

### Использование

```bash
# Из командной строки
python3 -m docreciv.sync_db.health_checks --check all
python3 -m docreciv.sync_db.health_checks --check vpn
python3 -m docreciv.sync_db.health_checks --check remote-mongo
```

---

## Конфигурация

### Переменные среды

```bash
# Удалённая MongoDB (через VPN)
MONGO_SERVER=192.168.0.46:8635
MONGO_USER=readProtocols223
MONGO_PASSWORD=***
MONGO_SSL_CERT=/path/to/sber2.crt
MONGO_PROTOCOLS_DB=protocols223

# Локальная MongoDB (метаданные)
LOCAL_MONGO_SERVER=localhost:27018
MONGO_METADATA_SERVER=localhost:27018
MONGO_METADATA_USER=admin
MONGO_METADATA_PASSWORD=***
MONGO_METADATA_DB=docling_metadata

# Async Downloader
ASYNC_MAX_CONCURRENT_REQUESTS=100
ASYNC_MAX_CONCURRENT_PROTOCOLS=50
ASYNC_LIMIT_PER_HOST=50

# Processing data
PROCESSING_DATA_DIR=/home/pak/Processing data

# VPN
VPN_REQUIRED=true
VPN_ENABLED_REMOTE_MONGO=true
VPN_ENABLED_ZAKUPKI=true
```

---

## Исправления (Январь 2026)

| Проблема | Решение | Файл |
|----------|---------|------|
| Только 41 протокол | Миграция на `loadDate` | `enhanced_service.py` |
| "File name too long" | UTF-8 aware ограничение | `utils.py` |
| Дублирование пути | Проверка base_input_dir | `file_manager.py` |
| Ошибки `/app` | Graceful error handling | `config.py` |

---

## Установка

```bash
cd /home/pak/projects/Docreciv
pip install -r requirements.txt
```

---

## Тестирование

```bash
# Все тесты
python3 -m pytest docreciv/tests/ -v

# Unit тесты
python3 -m pytest docreciv/tests/test_*.py -v
```

**Подробнее**: [TESTING.md](TESTING.md)

---

## Устранение неполадок

### Распространённые проблемы

1. **Сбои подключения MongoDB**
   - Проверьте переменные среды
   - Проверьте путь к SSL-сертификату
   - Убедитесь, что VPN подключен

2. **Сбои загрузки**
   - Проверьте доступность zakupki.gov.ru
   - Проверьте дисковое пространство
   - Убедитесь, что VPN активен

3. **Проблемы с производительностью**
   - Настройте `ASYNC_MAX_CONCURRENT_REQUESTS`
   - Проверьте сетевое соединение
   - Отслеживайте системные ресурсы

### Журналы

Компоненты ведут журнал в консоли со структурированными сообщениями.
Включите debug с помощью флага `--verbose`.

---

## Дополнительная документация

| Файл | Описание |
|------|----------|
| [README.md](README.md) | Главная документация |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Архитектура системы |
| [DATA_FLOW.md](DATA_FLOW.md) | Поток данных |
| [SETUP_GUIDE.md](SETUP_GUIDE.md) | Руководство по настройке |
| [ASYNC_TEST_RESULTS_SUMMARY.md](ASYNC_TEST_RESULTS_SUMMARY.md) | Результаты тестов |

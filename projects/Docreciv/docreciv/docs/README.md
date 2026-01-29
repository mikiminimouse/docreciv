# Документация Docreciv

## Обзор

**Docreciv** — компонент приёма данных для pipeline обработки протоколов закупок.
Система синхронизирует протоколы из удалённой MongoDB и загружает документы с zakupki.gov.ru.

**Версия**: 1.1.0 (январь 2026)
**Статус**: Production Ready ✅

---

## Активные компоненты

### 1. Синхронизация (sync_db)
Компонент для синхронизации протоколов из удалённой MongoDB в локальную по полю `loadDate`.

**Документация**:
- [ARCHITECTURE.md](ARCHITECTURE.md) — Общая архитектура
- [DATA_FLOW.md](DATA_FLOW.md) — Поток данных
- [ENHANCED_SYNC_DOWNLOADER_RU.md](ENHANCED_SYNC_DOWNLOADER_RU.md) — Подробная документация (RU)
- [SETUP_GUIDE.md](SETUP_GUIDE.md) — Руководство по настройке

**Ключевые файлы**:
- `docreciv/sync_db/enhanced_service.py` — Сервис синхронизации
- `docreciv/sync_db/health_checks.py` — Проверки здоровья

### 2. Async загрузчик (downloader)
Асинхронный компонент для загрузки документов с высокой производительностью.

**Документация**:
- [ASYNC_TEST_RESULTS_SUMMARY.md](ASYNC_TEST_RESULTS_SUMMARY.md) — Результаты тестов

**Ключевые файлы**:
- `docreciv/downloader/async_service.py` — Async сервис загрузки
- `docreciv/downloader/utils.py` — Utilities (sanitize_filename исправлен)
- `docreciv/downloader/file_manager.py` — Управление файлами

### 3. Pipeline (pipeline)
Компонент управления pipeline метаданных.

**Ключевые файлы**:
- `docreciv/pipeline/manager.py` — Управление pipeline
- `docreciv/pipeline/models.py` — Модели данных

---

## Производительность (Январь 2026)

### Async режим

| Метрика | Значение |
|---------|----------|
| Throughput | **4.25 files/s** (средний) |
| Success Rate | **99.1%** (средний) |
| Concurrent requests | 100 |
| Concurrent protocols | 50 |

### Тестовые запуски

| Дата | Протоколов | Файлов | Success Rate | Throughput |
|------|-----------|--------|-------------|-----------|
| 2026-01-22 | 1,703 | 1,841 | 99.4% | 3.31 f/s |
| 2026-01-24 | 1,665 | 1,823 | 98.2% | 4.25 f/s |
| 2026-01-27 | 1,502 | 1,663 | 99.8% | 4.38 f/s |

**Подробнее**: [ASYNC_TEST_RESULTS_SUMMARY.md](ASYNC_TEST_RESULTS_SUMMARY.md)

---

## Конфигурация

### Переменные окружения

```bash
# Удалённая MongoDB (через VPN)
MONGO_SERVER=192.168.0.46:8635
MONGO_USER=readProtocols223
MONGO_PASSWORD=***
MONGO_SSL_CERT=/path/to/sber2.crt
MONGO_PROTOCOLS_DB=protocols223

# Локальная MongoDB (метаданные)
LOCAL_MONGO_SERVER=localhost:27018
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
VPN_ENABLED_ZAKUPKI=true
VPN_ENABLED_REMOTE_MONGO=true
```

---

## Использование

### Синхронизация протоколов за дату

```bash
cd /home/pak/projects/Docreciv
python3 -m docreciv.sync_db.enhanced_service sync-date --date 2026-01-24 --limit 2000
```

### Async загрузка документов

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

---

## MongoDB Collections (docling_metadata)

| Collection | Назначение |
|------------|-----------|
| **protocols** | Основная коллекция протоколов |
| **pipeline_runs** | Запуски pipeline |
| **qa_results** | Результаты QA |
| **docling_results** | Результаты Docling |

### Структура документа `protocols`

```json
{
  "_id": ObjectId("..."),
  "unit_id": "UNIT_a1b2c3d4e5f6g7h8",
  "loadDate": ISODate("2026-01-24T10:30:00Z"),
  "urls": [{"url": "https://...", "fileName": "protocol.pdf"}],
  "status": "downloaded"
}
```

---

## Meta файлы UNIT

### Структура UNIT директории

```
/home/pak/Processing data/YYYY-MM-DD/Input/
├── UNIT_xxx/
│   ├── *.pdf, *.docx
│   ├── unit.meta.json      # Метаданные
│   └── raw_url_map.json    # Карта URL
```

### unit.meta.json
```json
{
  "unit_id": "UNIT_01d705f30ce548b1",
  "purchase_notice_number": "32615603710",
  "source_date": "2026-01-24",
  "files_total": 2,
  "files_success": 2
}
```

---

## Исправления (Январь 2026)

| Проблема | Решение |
|----------|---------|
| Только 41 протокол | Миграция на `loadDate` (40x больше данных) |
| "File name too long" | `sanitize_filename()` с UTF-8 ограничением |
| Дублирование пути | Исправлен `file_manager.py` |
| Ошибки `/app` | Graceful error handling в `config.py` |

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

## Интеграция с pipeline

```
Docreciv (Downloader)
    ├── unit.meta.json ────▶ LLM_qaenrich
    ├── raw_url_map.json ──▶ Аудит
    ▼
Docprep (Preparation)
    ▼
Doclingproc (Processing)
    ▼
LLM_qaenrich (QA & Enrichment)
```

---

## Дополнительная документация

| Файл | Описание |
|------|----------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Архитектура системы |
| [DATA_FLOW.md](DATA_FLOW.md) | Поток данных |
| [SETUP_GUIDE.md](SETUP_GUIDE.md) | Руководство по настройке |
| [REFACTORING_REPORT.md](REFACTORING_REPORT.md) | Отчёт о рефакторинге |
| [TESTING.md](TESTING.md) | Руководство по тестированию |
| [SYSTEM_STATE_2026-01-28.md](SYSTEM_STATE_2026-01-28.md) | Состояние системы |
| [ASYNC_TEST_RESULTS_SUMMARY.md](ASYNC_TEST_RESULTS_SUMMARY.md) | Результаты тестов |

# Docreciv System State (2026-01-28)

## Обзор системы

**Docreciv** — компонент приёма данных для pipeline обработки протоколов закупок.

### Дата и версия
- **Дата обновления**: 2026-01-28
- **Версия**: post-async-migration, post-config-fix
- **Статус**: Production Ready ✅

---

## Архитектура компонентов

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DOCRECIV ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐     │
│  │ Remote MongoDB│────▶│ Local MongoDB │────▶│ Async        │     │
│  │ (zakupki.gov │      │ (localhost:  │      │ Downloader   │     │
│  │  .ru via VPN)│      │  27018)      │      │              │     │
│  └──────────────┘      └──────────────┘      └──────┬───────┘     │
│         │                      │                     │             │
│         │ protocols223/        │ docling_metadata/  │             │
│         │ purchaseProtocol     │ protocols          │             │
│         │                      │                    │             │
│         ▼                      ▼                    ▼             │
│   ┌─────────────────────────────────────────────────────────┐     │
│   │                    PROCESSING DATA                      │     │
│   │   /home/pak/Processing data/YYYY-MM-DD/Input/           │     │
│   │                                                          │     │
│   │   UNIT_xxx/                                            │     │
│   │   ├── *.pdf, *.docx                                    │     │
│   │   ├── unit.meta.json  ───┐                              │     │
│   │   └── raw_url_map.json ──┼──▶ Трассировка и аудит       │     │
│   │                          │                              │     │
│   └──────────────────────────┼──────────────────────────────┘     │
│                              │                                     │
│                              ▼                                     │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐     │
│  │   Docprep    │────▶│  Doclingproc │────▶│ LLM_qaenrich │     │
│  │              │      │              │      │              │     │
│  └──────────────┘      └──────────────┘      └──────────────┘     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Изменения от 2026-01-28

### 1. Миграция на `loadDate` (вместо `publicationDateTime`)

**Проблема**: `publicationDateTime` (дата публикации) даёт только 41 протокол за выходные, `loadDate` (дата загрузки) показывает 1665 протоколов.

**Изменённые файлы**:
| Файл | Изменение |
|------|-----------|
| `sync_db/enhanced_service.py` | Query по `loadDate` |
| `scripts/metrics_collector.py` | Агрегация по `loadDate` |
| `pipeline/models.py` | Обновлён documentation |
| `pipeline/manager.py` | Использует `loadDate` |

**Результат**: 1665 протоколов вместо 41 (40x больше данных!)

### 2. Исправление дублирования пути

**Проблема**: UNIT директории создавались в `/path/Input/YYYY-MM-DD/Input/` вместо `/path/Input/`.

**Решение**: `file_manager.py:40` — проверка `base_input_dir.name == "Input"`

### 3. Исправление `sanitize_filename()`

**Проблема**: "File name too long" ошибки при загрузке файлов с длинными русскими именами.

**Решение**: `utils.py:51-92` — добавлено ограничение длины с учётом UTF-8:
- Максимальная длина: 200 символов (запас до 255 байт)
- Сохраняет расширение файла при обрезке
- Дополнительная проверка по байтам

### 4. Исправление проблемы `/app` (Docker leftovers)

**Проблема**: Конфигурация пыталась создать директории в `/app/...` без прав.

**Решение**: `config.py` — два изменения:
1. `RouterConfig._get_default_dir()` — правильные дефолты для local dev
2. `ensure_directories()` — graceful error handling

---

## MongoDB Collections (docling_metadata)

| Collection | Назначение | Поля |
|------------|-----------|------|
| **protocols** | Основная коллекция протоколов | `unit_id`, `loadDate`, `status`, `urls`, `purchaseNoticeNumber` |
| **pipeline_runs** | Запуски pipeline | `run_id`, `batch_date`, `stages` |
| **qa_results** | Результаты QA | `unit_id`, `winner_found`, `winner_inn` |
| **docling_results** | Результаты Docling | `unit_id`, `docling_success` |

### Структура документа `protocols`:

```json
{
  "_id": ObjectId("..."),
  "unit_id": "UNIT_a1b2c3d4e5f6g7h8",
  "purchaseInfo": {
    "purchaseNoticeNumber": "0373200040224000001"
  },
  "loadDate": ISODate("2026-01-24T10:30:00Z"),
  "publicationDateTime": ISODate("2026-01-23T15:00:00Z"),
  "urls": [
    {
      "url": "https://zakupki.gov.ru/...",
      "fileName": "protocol.pdf",
      "guid": "...",
      "contentUid": "..."
    }
  ],
  "multi_url": true,
  "url_count": 2,
  "source": "remote_mongo_direct",
  "status": "downloaded",
  "created_at": ISODate("..."),
  "updated_at": ISODate("...")
}
```

---

## Meta файлы UNIT

### unit.meta.json
```json
{
  "unit_id": "UNIT_01d705f30ce548b1",
  "record_id": "697a7a1a1117075d6ac7b638",
  "source_date": "2026-01-24",
  "downloaded_at": "2026-01-28T21:13:18Z",
  "files_total": 2,
  "files_success": 2,
  "files_failed": 0,
  "purchase_notice_number": "32615603710",
  "source": "remote_mongo_direct",
  "url_count": 2,
  "multi_url": true
}
```

### raw_url_map.json
```json
[
  {
    "url": "https://zakupki.gov.ru/...?id=108146508",
    "filename": "ПРОТОКОЛ_ЗАКУП...",
    "status": "ok",
    "guid": "e7ad45fe-14a1-47d3-97ab-6c925b05ad3e",
    "contentUid": "79292DCDCB094B3988EDF7B4FC3060AE"
  }
]
```

---

## Performance Metrics (Test Results 2026-01-28)

### Async Downloader

| Метрика | Значение |
|---------|----------|
| Throughput | **4.25 files/s** |
| Success Rate | **98.2%** |
| Duration | ~7 минут для 1665 protocols (1823 files) |
| Concurrent Requests | 100 |
| Concurrent Protocols | 50 |
| Protocols synced | 1665 (по loadDate) |
| Files downloaded | 1823 |
| Errors | 34 (timeout/connection reset) |

### URL Distribution

| Тип | Количество | % |
|-----|-----------|---|
| Single URL | 1 516 | 91% |
| Multi URL | 149 | 9% |
| No URL | 0 | 0% |

### File Types

| Тип | Количество |
|-----|-----------|
| PDF | 1 006 |
| DOCX | 365 |
| Прочие | 473 |
| **Всего файлов** | **1 844** |
| Meta файлы | 3 330 |
| UNIT директорий | 1 665 |
| Disk usage | 1000 MB |

---

## Bottleneck Analysis

| Компонент | Статус | Оптимизация |
|-----------|--------|-------------|
| **Network (zakupki.gov.ru)** | PRIMARY BOTTLENECK | Внешний API limit (~2 files/s даже при высокой конкурентности) |
| MongoDB Connection Pool | Нужно улучшить | `maxPoolSize=50` вместо 10 |
| VPN Overhead | Минимален | Cache на 5 мин |
| Disk I/O | OK | Async writes работают |
| sanitize_filename | ✅ Исправлено | Нет "File name too long" ошибок |
| Path duplication | ✅ Исправлено | UNIT создаются в правильном месте |

---

## Конфигурация

### Environment Variables (.env)

```bash
# Remote MongoDB (через VPN)
MONGO_SERVER=192.168.0.46:8635
MONGO_USER=readProtocols223
MONGO_PASSWORD=***
MONGO_SSL_CERT=/path/to/sber2.crt
MONGO_PROTOCOLS_DB=protocols223

# Local MongoDB
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
python -m docreciv.sync_db.enhanced_service sync-date --date 2026-01-24
```

### Загрузка документов (Async)
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

asyncio.run(download())
```

---

## Ключевые файлы

| Файл | Строк кода | Назначение |
|------|-----------|-----------|
| `downloader/async_service.py` | 572 | Async downloader |
| `downloader/utils.py` | 166 | Utilities (sanitize_filename исправлен) |
| `downloader/file_manager.py` | 149 | File management (duplicate path исправлен) |
| `sync_db/enhanced_service.py` | 1079 | Sync service (loadDate) |
| `scripts/metrics_collector.py` | 337 | Metrics (loadDate) |
| `pipeline/manager.py` | 448 | Pipeline management |
| `pipeline/models.py` | 366 | Data models |
| `core/config.py` | ~300 | Configuration (/app исправлен) |

**Всего**: ~3,400 строк основного кода

---

## Связи между компонентами

```
Docreciv (Downloader)
    ├── unit.meta.json ────▶ LLM_qaenrich (purchase_notice_number)
    ├── raw_url_map.json ──▶ Аудит и трассировка
    │
    ▼
Docprep (Preparation)
    ├── manifest.json ──────▶ Контент UNIT
    ├── docprep.contract.json
    │
    ▼
Doclingproc (Processing)
    ├── docling_results ────▶ MongoDB
    │
    ▼
LLM_qaenrich (QA & Enrichment)
    ├── qa_results ──────────▶ MongoDB
    └── purchase_notice_number ──▶ protocols (для обогащения)
```

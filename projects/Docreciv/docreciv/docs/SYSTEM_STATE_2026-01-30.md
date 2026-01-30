# Docreciv System State (2026-01-30)

## Обзор системы

**Docreciv** — компонент приёма данных для pipeline обработки протоколов закупок.

### Дата и версия
- **Дата обновления**: 2026-01-30
- **Версия**: 2.1.0 (Trace System)
- **Статус**: Production Ready ✅

---

## 🎯 Ключевые изменения v2.1.0

### Унифицированная система трейсинга (registrationNumber)

**BREAKING CHANGE**: Вместо `remote_mongo_id` используется `registrationNumber` как PRIMARY TRACE ID.

**Причина**: `remote_mongo_id` (ObjectId MongoDB) может быть потерян при миграции баз данных. `registrationNumber` — это бизнес-ключ из zakupki.gov.ru, который сохраняется при любых операциях.

| Критерий | remote_mongo_id | registrationNumber |
|----------|------------------|-------------------|
| Уникален? | ✅ Да | ✅ Да |
| Стабилен при миграции БД? | ❌ Может потеряться | ✅ Сохраняется |
| Является бизнес-ключом? | ❌ Технический ObjectId | ✅ Номер протокола |
| Используется в external API? | ❌ Нет | ✅ zakupki.gov.ru |

### Изменённые файлы v2.1.0

| Файл | Изменение |
|------|-----------|
| `sync_db/enhanced_service.py` | `registrationNumber` вместо `remote_mongo_id` |
| `downloader/meta_generator.py` | `registrationNumber` в `unit.meta.json` |
| `scripts/verify_trace_system.py` | Проверки для `registrationNumber` |
| `docs/TRACE_SYSTEM.md` | v2.1.0 документация |

### Результаты миграции

| Метрика | Значение |
|---------|----------|
| Документы с `registrationNumber` | 3205 (100%) |
| Исторические дубликаты | 14 (до v2.1.0) |
| Уникальный индекс | ✅ Создаётся при синхронизации |

---

## Архитектура компонентов

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DOCRECIV ARCHITECTURE v2.1.0                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐     │
│  │ Remote MongoDB│────▶│ Local MongoDB │────▶│ Async        │     │
│  │ (zakupki.gov │      │ (localhost:  │      │ Downloader   │     │
│  │  .ru via VPN)│      │  27018)      │      │              │     │
│  │              │      │              │      │              │     │
│  │ purchaseProto│      │ protocols    │      │              │     │
│  │ registration │      │ registration │      │              │     │
│  │ Number ★     │      │ Number ★     │      │              │     │
│  └──────────────┘      └──────────────┘      └──────┬───────┘     │
│                                                      │             │
│                              ▼                        │             │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │                    PROCESSING DATA                              │ │
│  │   /home/pak/Processing data/YYYY-MM-DD/Input/                   │ │
│  │                                                                  │ │
│  │   UNIT_xxx/                                                     │ │
│  │   ├── *.pdf, *.docx                                             │ │
│  │   ├── unit.meta.json  ────▶ registrationNumber ★                │ │
│  │   └── raw_url_map.json                                          │ │
│  │                                                                  │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                              │                                       │
│                              ▼                                       │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      │
│  │   Docprep    │────▶│  Doclingproc │────▶│ LLM_qaenrich │      │
│  │              │      │              │      │              │      │
│  └──────────────┘      └──────────────┘      └──────────────┘      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

★ = registrationNumber (PRIMARY TRACE ID)
```

---

## MongoDB Collections (docling_metadata)

| Коллекция | Назначение | PRIMARY KEY | Компонент |
|-----------|-----------|-------------|-----------|
| **protocols** | Протоколы закупок | `registrationNumber` | Docreciv (write), все (read) |
| **pipeline_runs** | Запуски pipeline | `run_id` | Pipeline Manager |
| **docling_results** | Результаты Docling | `unit_id` | Doclingproc |
| **qa_results** | Результаты QA | `unit_id` | LLM_qaenrich |

### Структура документа `protocols` (v2.1.0):

```javascript
{
  "_id": ObjectId("697a8e32..."),

  // === PRIMARY TRACE ID ★ ===
  "registrationNumber": "0373200040224000001",

  // === Метаданные ===
  "purchaseNoticeNumber": "32615605974",  // Номер закупки (НЕ уникален)

  // === Бизнес-данные ===
  "purchaseInfo": {...},
  "loadDate": ISODate("2026-01-24T10:30:00Z"),
  "urls": [...],
  "multi_url": true,
  "url_count": 2,

  // === UNIT идентификатор ===
  "unit_id": "UNIT_a1b2c3d4e5f6a7b8",

  // === TRACE: Покомпонентное отслеживание ★ ===
  "trace": {
    "docreciv": {
      "unit_id": "UNIT_a1b2c3...",
      "synced_at": "2026-01-29T07:00:00Z",
      "registrationNumber": "0373200040224000001",
      "files_downloaded": 2,
    },
    // Docprep добавит: "docprep": {...}
    // Doclingproc добавит: "docling": {...}
    // LLM_qaenrich добавит: "llm_qaenrich": {...}
  },

  // === HISTORY: Хронологический лог ===
  "history": [
    {
      "component": "docreciv",
      "action": "synced",
      "timestamp": "2026-01-29T07:00:00Z",
      "registrationNumber": "0373200040224000001",
    },
    // Future events...
  ],
}
```

---

## Индексы для трейсинга

```python
# В коллекции protocols
[
    # PRIMARY TRACE INDEX (UNIQUE) ★
    IndexModel([("registrationNumber", 1)], unique=True),

    # Component trace indexes
    IndexModel([("trace.docreciv.unit_id", 1)]),
    IndexModel([("trace.docprep.manifest_path", 1)]),
    IndexModel([("trace.docling.results_id", 1)]),
    IndexModel([("trace.llm_qaenrich.qa_record_id", 1)]),

    # Business keys (для поиска, НЕ уникальные)
    IndexModel([("purchaseInfo.purchaseNoticeNumber", 1)]),
    IndexModel([("purchaseNoticeNumber", 1)]),

    # Date indexes
    IndexModel([("loadDate", -1)]),
    IndexModel([("history.timestamp", -1)]),
]
```

---

## Meta файлы UNIT (v2.1.0)

### unit.meta.json
```json
{
  "unit_id": "UNIT_a1b2c3d4e5f6a7b8",

  "registrationNumber": "0373200040224000001",  // ★ PRIMARY TRACE ID
  "local_mongo_id": "697a8e32f1a2b3c4d5e6f7",     // Локальный MongoDB _id (reference)
  "record_id": "697a8e32f1a2b3c4d5e6f7",          // Legacy (совместимость)

  "source_date": "2026-01-24",
  "downloaded_at": "2026-01-29T07:05:00Z",
  "files_total": 2,
  "files_success": 2,
  "files_failed": 0,

  "purchase_notice_number": "32615605974",     // Бизнес-метаданные
  "source": "remote_mongo_direct",
  "url_count": 2,
  "multi_url": true,

  "trace_id": "0373200040224000001"            // Для удобства = registrationNumber
}
```

---

## Performance Metrics (Январь 2026)

### Сводная таблица всех тестов

| Дата | Протоколов | Файлов | Success Rate | Throughput | Duration |
|------|-----------|--------|-------------|-----------|----------|
| 2026-01-22 | 1,703 | 1,841 | 99.4% | 3.31 f/s | 9.3 мин |
| 2026-01-24 | 1,665 | 1,823 | 98.2% | 4.25 f/s | 7.1 мин |
| 2026-01-27 | 1,502 | 1,663 | 99.8% | 4.38 f/s | 6.3 мин |
| **Среднее** | **1,623** | **1,776** | **99.1%** | **3.98 f/s** | **7.6 мин** |

### Async Downloader

| Метрика | Значение |
|---------|----------|
| Throughput (avg) | **3.98 files/s** |
| Success Rate (avg) | **99.1%** |
| Concurrent Requests | 100 |
| Concurrent Protocols | 50 |

---

## Выявленные проблемы (roadmap)

### 1. Разрыв в trace chain ⚠️
**Проблема**: Только Docreciv пишет в `trace.docreciv`. Другие компоненты не обновляют trace.

**Результат**: Невозможно отследить полный путь документа через pipeline.

### 2. Отсутствие блокировок ⚠️
**Проблема**: Нет механизма `processing_lock`. Возможны race conditions.

**Результат**: Два экземпляра могут обрабатывать один документ одновременно.

### 3. Разрозненные коллекции результатов ⚠️
**Проблема**: `docling_results` и `qa_results` используют `unit_id` вместо `registrationNumber`.

**Результат**: Сложно связать результаты с исходным протоколом после миграций.

### 4. Нет транзакций ⚠️
**Проблема**: Критичные операции не атомарны.

**Результат**: Потенциальная потеря данных при сбоях.

**Подробнее**: [`OPTIMIZATION_ROADMAP.md`](OPTIMIZATION_ROADMAP.md)

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

### Верификация trace системы
```bash
python -m docreciv.scripts.verify_trace_system
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

## Roadmap

Следующие шаги для оптимизации:

1. **Phase 1**: Unified Trace Update API — общий метод для обновления trace
2. **Phase 2**: Processing Locks — механизм блокировок
3. **Phase 3**: Normalize Result Collections — `registrationNumber` во всех коллекциях
4. **Phase 4**: Aggregation Views — MongoDB views для полных данных

**Подробнее**: [`OPTIMIZATION_ROADMAP.md`](OPTIMIZATION_ROADMAP.md)

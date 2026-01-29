# Унифицированная система трейсинга данных

## Обзор

Система трейсинга обеспечивает сквозную отслеживаемость документов от источника (удалённая MongoDB) до всех этапов обработки (Docreciv → Docprep → Doclingproc → LLM_qaenrich).

**Версия**: 2.0.0 (январь 2026)
**Статус**: Реализовано в Docreciv

---

## Ключевая концепция: remote_mongo_id

**`remote_mongo_id`** — единый идентификатор для всего pipeline. Это оригинальный `_id` документа из удалённой MongoDB (zakupki.gov.ru).

```
Удалённая MongoDB (protocols223)
┌─────────────────────────────┐
│ _id: 65a1b2c3d4e5f6g7h8i9j0 │ ← Исходный ObjectId
│ purchaseInfo: {...}          │
│ loadDate: 2026-01-24        │
└─────────────────────────────┘
              │
              │ sync (сохраняем _id как remote_mongo_id)
              ▼
Локальная MongoDB (docling_metadata)
┌─────────────────────────────┐
│ _id: 697a8e32f1a2b3c4d5e6f7 │ ← Локальный ObjectId (служебный)
│ remote_mongo_id: 65a1b2c3... │ ← PRIMARY TRACE ID
│ trace: {...}                 │
│ history: [...]               │
└─────────────────────────────┘
```

---

## Структура документа в MongoDB

### Полная схема документа protocols:

```javascript
{
  // === Локальные служебные поля ===
  "_id": ObjectId("697a8e32..."),        // Локальный MongoDB _id
  "created_at": ISODate("2026-01-28"),
  "updated_at": ISODate("2026-01-28"),
  "status": "downloaded",

  // === PRIMARY TRACE ID ===
  "remote_mongo_id": "65a1b2c3d4e5f6g7h8i9j0",  // Исходный _id из удалённой MongoDB

  // === Бизнес-данные (из источника) ===
  "purchaseInfo": {
    "purchaseNoticeNumber": "32615605974",  // Метаданные, НЕ уникальный ID!
    "purchaseName": "...",
    ...
  },
  "loadDate": ISODate("2026-01-24T10:30:00Z"),
  "publicationDateTime": ISODate("2026-01-23T15:00:00Z"),
  "urls": [...],
  "multi_url": true,
  "url_count": 2,

  // === UNIT идентификатор ===
  "unit_id": "UNIT_a1b2c3d4e5f6a7b8",

  // === TRACE: Покомпонентное отслеживание ===
  "trace": {
    "docreciv": {
      "unit_id": "UNIT_a1b2c3d4e5f6a7b8",
      "synced_at": "2026-01-28T21:00:00Z",
      "remote_mongo_id": "65a1b2c3d4e5f6g7h8i9j0",
      "files_downloaded": 2,
    },
    // Docprep добавит:
    // "docprep": {
    //   "processed_at": "...",
    //   "manifest_path": "/...",
    // },
    // Doclingproc добавит:
    // "docling": {...},
    // LLM_qaenrich добавит:
    // "llm_qaenrich": {...},
  },

  // === HISTORY: Хронологический лог ===
  "history": [
    {
      "component": "docreciv",
      "action": "synced",
      "timestamp": "2026-01-28T21:00:00Z",
      "remote_mongo_id": "65a1b2c3d4e5f6g7h8i9j0",
    },
    {
      "component": "docreciv",
      "action": "downloaded",
      "timestamp": "2026-01-28T21:05:00Z",
      "files_count": 2,
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
    # PRIMARY TRACE INDEX (UNIQUE)
    IndexModel([("remote_mongo_id", 1)], unique=True),

    # Component trace indexes
    IndexModel([("trace.docreciv.unit_id", 1)]),
    IndexModel([("trace.docprep.manifest_path", 1)]),
    IndexModel([("trace.docling.results_id", 1)]),
    IndexModel([("trace.llm_qaenrich.qa_record_id", 1)]),

    # Business keys (для поиска, НЕ уникальные)
    IndexModel([("purchaseInfo.purchaseNoticeNumber", 1)]),

    # Date indexes
    IndexModel([("loadDate", -1)]),
    IndexModel([("history.timestamp", -1)]),
]
```

---

## unit.meta.json структура

```json
{
  "unit_id": "UNIT_a1b2c3d4e5f6a7b8",

  "remote_mongo_id": "65a1b2c3d4e5f6g7h8i9j0",   // PRIMARY TRACE ID
  "local_mongo_id": "697a8e32f1a2b3c4d5e6f7",   // Локальный _id
  "record_id": "697a8e32f1a2b3c4d5e6f7",        // Legacy (совместимость)

  "source_date": "2026-01-24",
  "downloaded_at": "2026-01-28T21:05:00Z",
  "files_total": 2,
  "files_success": 2,
  "files_failed": 0,

  "purchase_notice_number": "32615605974",     // Бизнес-метаданные
  "source": "remote_mongo_direct",
  "url_count": 2,
  "multi_url": true,

  "trace_id": "65a1b2c3d4e5f6g7h8i9j0"          // Для удобства
}
```

---

## Использование

### Поиск по remote_mongo_id

```python
from pymongo import MongoClient

client = MongoClient('mongodb://admin:password@localhost:27018/?authSource=admin')
db = client['docling_metadata']
protocols = db['protocols']

# Найти по первичному trace ID
protocol = protocols.find_one({"remote_mongo_id": "65a1b2c3d4e5f6g7h8i9j0"})

# Получить полный трейс
trace = protocol.get('trace', {})
for component, data in trace.items():
    print(f"{component}: {data}")
```

### Агрегация с другими коллекциями

```python
# Объединить с qa_results по remote_mongo_id
pipeline = [
    {"$match": {"remote_mongo_id": "65a1b2c3..."}},
    {
        "$lookup": {
            "from": "qa_results",
            "let": {"remote_id": "$remote_mongo_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$remote_mongo_id", "$$remote_id"]}}}
            ],
            "as": "qa_data"
        }
    }
]
```

### Обновление trace из компонента

```python
# Компонент (например, LLM_qaenrich) обновляет свой trace
def update_trace(remote_mongo_id: str, component: str, data: dict):
    protocols.update_one(
        {"remote_mongo_id": remote_mongo_id},
        {
            "$set": {f"trace.{component}": data},
            "$push": {
                "history": {
                    "component": component,
                    "action": "processed",
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                }
            }
        }
    )
```

---

## Верификация системы

Запустите верификационный скрипт:

```bash
python3 docreciv/scripts/verify_trace_system.py
```

Ожидаемый вывод для новой системы:
```
============================================================
📊 ИТОГОВЫЙ СТАТУС
============================================================
  ✅ Система трейсинга настроена КОРРЕКТНО

Проверка 1: Индексы
  ✅ remote_mongo_id_idx: PRIMARY TRACE INDEX (UNIQUE)
  ✅ trace_docreciv_unit_idx: Component trace
  ✅ purchase_notice_idx: Business key

Проверка 2: Структура документов
  С remote_mongo_id: N (XX%)

Проверка 3: Пример документа
  remote_mongo_id: 65a1b2c3d4...
  Trace: {docreciv: {...}}

Проверка 4: Уникальность
  ✅ Все remote_mongo_id уникальны
```

---

## Изменённые файлы

| Файл | Изменения |
|------|-----------|
| `docreciv/sync_db/enhanced_service.py` | Добавлено `remote_mongo_id`, `trace`, `history` в `_create_protocol_document()` |
| `docreciv/sync_db/enhanced_service.py` | Обновлён `_ensure_indexes()` для новых индексов |
| `docreciv/downloader/meta_generator.py` | Добавлены `remote_mongo_id`, `local_mongo_id`, `trace_id` в `unit.meta.json` |
| `docreciv/scripts/verify_trace_system.py` | Новый скрипт верификации |

---

## Почему НЕ purchaseNoticeNumber?

| Проверка | Результат |
|----------|-----------|
| Уникален ли? | **НЕТ** — один закуп может иметь несколько протоколов |
| Зависит от времени? | **НЕТ** — константа для закупки |
| Отслеживает источник? | **НЕТ** — только метаданные закупки |

**Вывод**: `purchaseNoticeNumber` — важная бизнес-метадата для группировки, но **НЕ идентификатор документа**.

---

## Roadmap для других компонентов

### Phase 2: Docprep
- Добавить `remote_mongo_id` в `manifest.json`
- Обновлять `trace.docprep` при обработке

### Phase 3: Doclingproc
- Использовать `remote_mongo_id` для поиска метаданных
- Добавлять `trace.docling` в результаты

### Phase 4: LLM_qaenrich
- Добавить `remote_mongo_id` в QA записи
- Обновлять `trace.llm_qaenrich`

---

## Миграция существующих данных

Для существующих документов без `remote_mongo_id`:

```python
# Вариант A: Оставить как есть (backward compatibility)
# Старые документы продолжают работать с unit_id

# Вариант B: Миграция при следующей синхронизации
# Повторная синхронизация перезапишет документы с remote_mongo_id
```

**Рекомендация**: Оставить старые документы как есть. Новые документы будут иметь полную структуру трейсинга. При необходимости можно выполнить ресинхронизацию для конкретных дат.

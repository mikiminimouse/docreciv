# Унифицированная система трейсинга данных

## Обзор

Система трейсинга обеспечивает сквозную отслеживаемость документов от источника (удалённая MongoDB) до всех этапов обработки (Docreciv → Docprep → Doclingproc → LLM_qaenrich).

**Версия**: 2.1.0 (январь 2026)
**Статус**: Production Ready ✅

**Ключевое изменение**: Вместо `remote_mongo_id` используется `registrationNumber` как PRIMARY TRACE ID для устойчивости к миграциям баз данных.

---

## Ключевая концепция: registrationNumber

**`registrationNumber`** — уникальный номер протокола закупки из коллекции `purchaseProtocol` удалённой MongoDB. Используется как единый идентификатор для всего pipeline.

**Почему не `remote_mongo_id`**: ObjectId из MongoDB может быть потерян при миграции или перезаписи баз данных. `registrationNumber` — это бизнес-ключ, который сохраняется при любых операциях с БД.

```
Удалённая MongoDB (purchaseProtocol)
┌─────────────────────────────┐
│ registrationNumber: "0373...│ ← PRIMARY TRACE ID
│ purchaseInfo.purchaseNotice...│
│ loadDate: 2026-01-24        │
└─────────────────────────────┘
              │ sync (сохраняем registrationNumber)
              ▼
Локальная MongoDB (docling_metadata)
┌─────────────────────────────┐
│ registrationNumber: "0373..."│ ← PRIMARY TRACE ID
│ trace: {...}                 │
│ history: [...]               │
└─────────────────────────────┘
```

---

## Структура документа в MongoDB

```javascript
{
  "_id": ObjectId("697a8e32..."),        // Локальный MongoDB (служебный)

  // === PRIMARY TRACE ID ===
  "registrationNumber": "0373200040224000001",  // Из purchaseProtocol

  // === Метаданные ===
  "purchaseNoticeNumber": "32615605974",  // Номер закупки (НЕ уникален для протоколов)

  // === Бизнес-данные (из источника) ===
  "purchaseInfo": {
    "purchaseNoticeNumber": "32615605974",
    "purchaseName": "...",
    ...
  },
  "loadDate": ISODate("2026-01-24T10:30:00Z"),
  "urls": [...],
  "multi_url": true,
  "url_count": 2,

  // === UNIT идентификатор ===
  "unit_id": "UNIT_a1b2c3d4e5f6a7b8",

  // === TRACE: Покомпонентное отслеживание ===
  "trace": {
    "docreciv": {
      "unit_id": "UNIT_a1b2c3...",
      "synced_at": "2026-01-29T07:00:00Z",
      "registrationNumber": "0373200040224000001",
      "files_downloaded": 2,
    },
    // Docprep добавит:
    // "docprep": {
    //   "processed_at": "...",
    //   "manifest_path": "/.../Ready2Docling/.../manifest.json",
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
      "timestamp": "2026-01-29T07:00:00Z",
      "registrationNumber": "0373200040224000001",
    },
    {
      "component": "docreciv",
      "action": "downloaded",
      "timestamp": "2026-01-29T07:05:00Z",
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

## unit.meta.json структура

```json
{
  "unit_id": "UNIT_a1b2c3d4e5f6a7b8",

  "registrationNumber": "0373200040224000001",  // PRIMARY TRACE ID
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

## Использование

### Поиск по registrationNumber

```python
from pymongo import MongoClient

client = MongoClient('mongodb://admin:password@localhost:27018/?authSource=admin')
db = client['docling_metadata']
protocols = db['protocols']

# Найти по первичному trace ID
protocol = protocols.find_one({"registrationNumber": "0373200040224000001"})

# Получить полный трейс
trace = protocol.get('trace', {})
for component, data in trace.items():
    print(f"{component}: {data}")
```

### Агрегация с другими коллекциями

```python
# Объединить с qa_results по registrationNumber
pipeline = [
    {"$match": {"registrationNumber": "0373200040224000001"}},
    {
        "$lookup": {
            "from": "qa_results",
            "let": {"reg_number": "$registrationNumber"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$registrationNumber", "$$reg_number"]}}}
            ],
            "as": "qa_data"
        }
    }
]
```

### Обновление trace из компонента

```python
def update_trace(registrationNumber: str, component: str, data: dict):
    """Обновить trace протокола."""
    protocols.update_one(
        {"registrationNumber": registrationNumber},
        {
            "$set": {f"trace.{component}": data},
            "$push": {
                "history": {
                    "component": component,
                    "action": "processed",
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "registrationNumber": registrationNumber,
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
  ✅ registration_number_idx: PRIMARY TRACE INDEX (UNIQUE)
  ✅ trace_docreciv_unit_idx: Component trace
  ✅ purchase_notice_idx: Business key

Проверка 2: Структура документов
  С registrationNumber: N (XX%)

Проверка 3: Пример документа
  registrationNumber: 03732000402...
  Trace: {docreciv: {...}}

Проверка 4: Уникальность
  ✅ Все registrationNumber уникальны
```

---

## Изменённые файлы

| Файл | Изменения |
|------|-----------|
| `docreciv/sync_db/enhanced_service.py` | `registrationNumber` вместо `remote_mongo_id` |
| `docreciv/downloader/meta_generator.py` | `registrationNumber` в unit.meta.json |
| `docreciv/scripts/verify_trace_system.py` | Проверки для `registrationNumber` |

---

## Почему registrationNumber?

| Критерий | remote_mongo_id | registrationNumber |
|----------|------------------|-------------------|
| Уникален? | ✅ Да | ✅ Да |
| Стабилен при миграции БД? | ❌ Может потеряться | ✅ Сохраняется |
| Является бизнес-ключом? | ❌ Технический ObjectId | ✅ Номер протокола |
| Используется в external API? | ❌ Нет | ✅ zakupki.gov.ru |

---

## Roadmap для других компонентов

### Phase 2: Docprep
- Добавить `registrationNumber` в manifest.json
- Обновлять trace при переносе UNIT

### Phase 3: DoclingProc
- Использовать `registrationNumber` для поиска метаданных
- Добавлять trace в результаты

### Phase 4: LLM_qaenrich
- Добавить `registrationNumber` в QA записи
- Обновлять trace в protocols

---

## Миграция существующих данных

Для существующих документов без `registrationNumber`:

```python
# Вариант A: Оставить как есть (backward compatibility)
# Старые документы продолжают работать с unit_id

# Вариант B: Миграция при следующей синхронизации
# Повторная синхронизация перезапишет документы с registrationNumber
```

**Рекомендация**: Оставить старые документы как есть. Новые документы будут иметь `registrationNumber`. При необходимости можно выполнить ресинхронизацию для конкретных дат.

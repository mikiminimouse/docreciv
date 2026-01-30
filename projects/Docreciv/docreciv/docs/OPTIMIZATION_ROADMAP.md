# Roadmap: Оптимизация общей MongoDB для pipeline компонентов

## Версия
**Дата**: 2026-01-30
**Версия системы**: 2.1.0 (Trace System)
**Статус**: Planning

---

## Executive Summary

**Проблема**: Использование общей MongoDB (`docling_metadata`) всеми компонентами pipeline без надлежащих механизмов синхронизации приводит к:

1. **Разрыву в trace chain** — невозможно отследить полный путь документа
2. **Race conditions** — параллельная обработка одних данных
3. **Разрозненным коллекциям** — сложность связывания результатов

**Решение**: Многофазная оптимизация с внедрением Unified Trace API, Processing Locks и нормализации коллекций.

---

## Текущее состояние

### Коллекции в docling_metadata

| Коллекция | PRIMARY KEY | Trace linkage | Проблема |
|-----------|-------------|---------------|----------|
| **protocols** | `registrationNumber` ✅ | ✅ | — |
| **docling_results** | `unit_id` ⚠️ | ❌ | Нет `registrationNumber` |
| **qa_results** | `unit_id` ⚠️ | ❌ | Нет `registrationNumber` |
| **pipeline_runs** | `run_id` | — | — |

### Trace chain статус

```
Docreciv ──▶ protocols: ✅ trace.docreciv обновляется
Docprep ────▶ protocols: ❌ не обновляет trace
Doclingproc ─▶ protocols: ❌ не обновляет trace
LLM_qaenrich ▶ protocols: ❌ не обновляет trace
```

---

## Phase 1: Unified Trace Update API ⭐ (Priority 1)

### Цель
Создать общий модуль для обновления trace в protocols, который будут использовать все компоненты.

### Реализация

#### 1.1 Создать `docreciv/core/trace.py`

```python
"""
Unified Trace API для обновления trace в protocols.

Позволяет всем компонентам pipeline добавлять свою информацию
в trace и history документов protocols.
"""

from datetime import datetime
from typing import Dict, Any, Optional
from pymongo import MongoClient, UpdateOne
from docreciv.core.config import get_config


class TraceUpdater:
    """API для обновления trace в protocols."""

    def __init__(self):
        config = get_config()
        local_config = config.sync_db.local_mongo
        self.client = MongoClient(local_config.get_connection_url())
        self.db = self.client[local_config.db]
        self.collection = self.db[local_config.collection]

    def update_trace(
        self,
        registrationNumber: str,
        component: str,
        action: str,
        data: Dict[str, Any],
        timestamp: Optional[str] = None
    ) -> bool:
        """
        Атомарное обновление trace и history.

        Args:
            registrationNumber: Primary trace ID
            component: Имя компонента (docreciv, docprep, docling, llm_qaenrich)
            action: Действие (synced, processed, failed, etc.)
            data: Данные для записи в trace.{component}
            timestamp: Опциональная timestamp (default: now)

        Returns:
            True если обновление успешно
        """
        if timestamp is None:
            timestamp = datetime.utcnow().isoformat() + "Z"

        # Добавляем metadata
        trace_data = {
            **data,
            "updated_at": timestamp,
            "registrationNumber": registrationNumber
        }

        history_event = {
            "component": component,
            "action": action,
            "timestamp": timestamp,
            "registrationNumber": registrationNumber
        }

        result = self.collection.update_one(
            {"registrationNumber": registrationNumber},
            {
                "$set": {f"trace.{component}": trace_data},
                "$push": {"history": history_event}
            }
        )

        return result.modified_count > 0

    def bulk_update_traces(self, updates: list) -> Dict[str, int]:
        """
        Bulk обновление trace для нескольких документов.

        Args:
            updates: Список кортежей (registrationNumber, component, action, data)

        Returns:
            Словарь со статистикой {matched, modified, failed}
        """
        operations = []
        timestamp = datetime.utcnow().isoformat() + "Z"

        for reg_num, component, action, data in updates:
            trace_data = {
                **data,
                "updated_at": timestamp,
                "registrationNumber": reg_num
            }
            history_event = {
                "component": component,
                "action": action,
                "timestamp": timestamp,
                "registrationNumber": reg_num
            }

            operations.append(UpdateOne(
                {"registrationNumber": reg_num},
                {
                    "$set": {f"trace.{component}": trace_data},
                    "$push": {"history": history_event}
                }
            ))

        if operations:
            result = self.collection.bulk_write(operations, ordered=False)
            return {
                "matched": result.matched_count,
                "modified": result.modified_count,
                "failed": len(operations) - result.modified_count
            }
        return {"matched": 0, "modified": 0, "failed": 0}

    def get_trace(self, registrationNumber: str) -> Optional[Dict[str, Any]]:
        """Получить полный trace для документа."""
        doc = self.collection.find_one(
            {"registrationNumber": registrationNumber},
            projection={"trace": 1, "history": 1, "_id": 0}
        )
        return doc

    def verify_trace_chain(self, registrationNumber: str) -> Dict[str, bool]:
        """
        Проверить полноту trace chain.

        Returns:
            {component: present} для каждого компонента
        """
        doc = self.collection.find_one(
            {"registrationNumber": registrationNumber},
            projection={"trace": 1, "_id": 0}
        )

        if not doc:
            return {}

        trace = doc.get("trace", {})
        return {
            "docreciv": "docreciv" in trace,
            "docprep": "docprep" in trace,
            "docling": "docling" in trace,
            "llm_qaenrich": "llm_qaenrich" in trace
        }
```

#### 1.2 Обновить `enhanced_service.py`

```python
# Вместо прямого обновления trace:
from docreciv.core.trace import TraceUpdater

class EnhancedSyncService:
    def _create_protocol_document(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        # ... существующий код ...

        # Использовать TraceUpdater для начального trace
        trace_updater = TraceUpdater()
        trace_updater.update_trace(
            registrationNumber=reg_number,
            component="docreciv",
            action="synced",
            data={
                "unit_id": unit_id,
                "synced_at": now_ts.isoformat() + "Z",
                "files_downloaded": 0,  # Обновится после download
            }
        )
```

### Файлы для изменения

| Файл | Действие |
|------|----------|
| `docreciv/core/trace.py` | Создать новый модуль |
| `docreciv/sync_db/enhanced_service.py` | Использовать TraceUpdater |
| `docreciv/downloader/async_service.py` | Использовать TraceUpdater |
| Обновить Docprep | Использовать TraceUpdater |
| Обновить Doclingproc | Использовать TraceUpdater |
| Обновить LLM_qaenrich | Использовать TraceUpdater |

### Тестирование

```python
# test_trace_api.py
def test_trace_update():
    updater = TraceUpdater()

    # Тест 1: Одиночное обновление
    result = updater.update_trace(
        registrationNumber="0373200040224000001",
        component="docreciv",
        action="synced",
        data={"unit_id": "UNIT_test", "files": 2}
    )
    assert result is True

    # Тест 2: Проверка trace
    trace = updater.get_trace("0373200040224000001")
    assert "docreciv" in trace["trace"]

    # Тест 3: Проверка chain
    chain = updater.verify_trace_chain("0373200040224000001")
    assert chain["docreciv"] is True
```

---

## Phase 2: Processing Locks ⭐ (Priority 2)

### Цель
Предотвратить race conditions при параллельной обработке документов.

### Реализация

#### 2.1 Создать `docreciv/core/locks.py`

```python
"""
Processing Locks для предотвращения race conditions.

Использует MongoDB для реализации распределённых блокировок.
"""

from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from pymongo import MongoClient
from docreciv.core.config import get_config


class ProcessingLock:
    """Распределённые блокировки для обработки документов."""

    def __init__(self):
        config = get_config()
        local_config = config.sync_db.local_mongo
        self.client = MongoClient(local_config.get_connection_url())
        self.db = self.client[local_config.db]
        self.collection = self.db[local_config.collection]

    def acquire(
        self,
        registrationNumber: str,
        component: str,
        ttl_seconds: int = 3600
    ) -> bool:
        """
        Захватить блокировку на документ.

        Args:
            registrationNumber: Primary trace ID
            component: Имя компонента
            ttl_seconds: Время жизни блокировки (default: 1 час)

        Returns:
            True если блокировка захвачена, False если уже занята
        """
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=ttl_seconds)

        # Пытаемся захватить только если нет активной блокировки
        result = self.collection.update_one(
            {
                "registrationNumber": registrationNumber,
                f"processing_lock.{component}": {"$exists": False}
            },
            {
                "$set": {
                    f"processing_lock.{component}": {
                        "locked_at": now.isoformat() + "Z",
                        "locked_by": component,
                        "expires_at": expires_at.isoformat() + "Z"
                    }
                }
            }
        )

        return result.modified_count > 0

    def release(self, registrationNumber: str, component: str) -> bool:
        """Освободить блокировку."""
        result = self.collection.update_one(
            {"registrationNumber": registrationNumber},
            {"$unset": {f"processing_lock.{component}": ""}}
        )
        return result.modified_count > 0

    def is_locked(
        self,
        registrationNumber: str,
        component: Optional[str] = None
    ) -> bool:
        """
        Проверить заблокирован ли документ.

        Args:
            registrationNumber: Primary trace ID
            component: Опционально - проверить конкретный компонент

        Returns:
            True если заблокирован
        """
        query = {"registrationNumber": registrationNumber}

        if component:
            query[f"processing_lock.{component}"] = {"$exists": True}
        else:
            query["processing_lock"] = {"$exists": True, "$ne": {}}

        doc = self.collection.find_one(query, projection=["processing_lock"])
        return doc is not None

    def cleanup_expired(self) -> int:
        """Удалить истёкшие блокировки. Возвращает количество удалённых."""
        now = datetime.utcnow().isoformat() + "Z"

        # Найти документы с истекшими блокировками
        docs_with_expired = list(self.collection.find({
            "processing_lock": {"$exists": True}
        }))

        cleaned = 0
        for doc in docs_with_expired:
            locks_to_remove = []
            for component, lock_info in doc.get("processing_lock", {}).items():
                if lock_info.get("expires_at", "") < now:
                    locks_to_remove.append(f"processing_lock.{component}")

            if locks_to_remove:
                update = {"$unset": {field: "" for field in locks_to_remove}}
                self.collection.update_one(
                    {"_id": doc["_id"]},
                    update
                )
                cleaned += 1

        return cleaned

    def get_lock_info(self, registrationNumber: str) -> Optional[Dict[str, Any]]:
        """Получить информацию о блокировках документа."""
        doc = self.collection.find_one(
            {"registrationNumber": registrationNumber},
            projection=["processing_lock"]
        )
        return doc.get("processing_lock") if doc else None


class LockContextManager:
    """Context manager для автоматического освобождения блокировок."""

    def __init__(self, registrationNumber: str, component: str, ttl_seconds: int = 3600):
        self.registrationNumber = registrationNumber
        self.component = component
        self.ttl_seconds = ttl_seconds
        self.lock = None

    def __enter__(self):
        self.lock = ProcessingLock()
        acquired = self.lock.acquire(
            self.registrationNumber,
            self.component,
            self.ttl_seconds
        )
        if not acquired:
            raise RuntimeError(f"Failed to acquire lock for {self.registrationNumber}")
        return self.lock

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock:
            self.lock.release(self.registrationNumber, self.component)
        return False
```

#### 2.2 Использование в компонентах

```python
# В async_service.py
from docreciv.core.locks import LockContextManager

async def process_protocol(self, protocol: Dict[str, Any]) -> Dict[str, Any]:
    registrationNumber = protocol.get("registrationNumber")

    # Использовать context manager для автоматического освобождения
    with LockContextManager(registrationNumber, "docreciv", ttl_seconds=1800):
        # Обработка документа
        result = await self._download_files(protocol)
        return result
```

### Файлы для изменения

| Файл | Действие |
|------|----------|
| `docreciv/core/locks.py` | Создать новый модуль |
| `docreciv/downloader/async_service.py` | Добавить LockContextManager |
| Обновить Doclingproc | Добавить LockContextManager |
| Обновить LLM_qaenrich | Добавить LockContextManager |

### Тестирование

```python
# test_locks.py
import asyncio
from docreciv.core.locks import ProcessingLock, LockContextManager

def test_lock_acquire():
    lock = ProcessingLock()

    # Тест 1: Захват блокировки
    assert lock.acquire("test_reg", "component1") is True

    # Тест 2: Повторный захват должен failed
    assert lock.acquire("test_reg", "component1") is False

    # Тест 3: Освобождение
    assert lock.release("test_reg", "component1") is True

    # Тест 4: Повторный захват после освобождения
    assert lock.acquire("test_reg", "component1") is True

async def test_concurrent_locking():
    """Тест race conditions."""
    results = []

    async def worker(worker_id: int):
        lock = ProcessingLock()
        acquired = lock.acquire("test_reg_concurrent", f"worker{worker_id}")
        results.append((worker_id, acquired))
        if acquired:
            await asyncio.sleep(0.1)
            lock.release("test_reg_concurrent", f"worker{worker_id}")

    # Запустить 5 воркеров одновременно
    await asyncio.gather(*[worker(i) for i in range(5)])

    # Только один должен захватить
    acquired_count = sum(1 for _, acq in results if acq)
    assert acquired_count == 1
```

---

## Phase 3: Normalize Result Collections (Priority 3)

### Цель
Добавить `registrationNumber` во все коллекции результатов для надежной связи.

### Изменения в схемах

#### 3.1 docling_results

```javascript
// ДО (текущее состояние)
{
  "_id": ObjectId("..."),
  "unit_id": "UNIT_a1b2...",  // PRIMARY KEY
  "docling_document": {...},
  "markdown_content": "...",
  "processed_at": "..."
}

// ПОСЛЕ (с registrationNumber)
{
  "_id": ObjectId("..."),
  "registrationNumber": "0373200040224000001",  // NEW ★
  "unit_id": "UNIT_a1b2...",
  "docling_document": {...},
  "markdown_content": "...",
  "processed_at": "...",
  "trace_updated": true  // NEW
}

// Индексы
[
  { "registrationNumber": 1 },  // NEW ★
  { "unit_id": 1 },
  { "registrationNumber": 1, "unit_id": 1 }  // Composite
]
```

#### 3.2 qa_results

```javascript
// ДО (текущее состояние)
{
  "_id": ObjectId("..."),
  "unit_id": "UNIT_a1b2...",  // PRIMARY KEY
  "winner_found": true,
  "winner_inn": "...",
  "qa_processed_at": "..."
}

// ПОСЛЕ (с registrationNumber)
{
  "_id": ObjectId("..."),
  "registrationNumber": "0373200040224000001",  // NEW ★
  "unit_id": "UNIT_a1b2...",
  "winner_found": true,
  "winner_inn": "...",
  "qa_processed_at": "...",
  "trace_updated": true  // NEW
}

// Индексы
[
  { "registrationNumber": 1 },  // NEW ★
  { "unit_id": 1 },
  { "winner_found": 1 },
  { "registrationNumber": 1, "winner_found": 1 }  // Composite
]
```

### Скрипт миграции

```python
# scripts/migrate_results_to_registration.py
"""
Миграция существующих результатов для добавления registrationNumber.
"""

from pymongo import MongoClient
from docreciv.core.config import get_config


def migrate_docling_results():
    """Добавить registrationNumber в docling_results."""
    config = get_config()
    local_config = config.sync_db.local_mongo
    client = MongoClient(local_config.get_connection_url())
    db = client[local_config.db]

    protocols = db[local_config.collection]
    docling_results = db["docling_results"]

    # Создать индекс
    docling_results.create_index("registrationNumber")

    # Миграция
    migrated = 0
    for result in docling_results.find({"registrationNumber": {"$exists": False}}):
        unit_id = result.get("unit_id")
        if not unit_id:
            continue

        # Найти registrationNumber в protocols
        protocol = protocols.find_one({"unit_id": unit_id})
        if protocol:
            reg_number = protocol.get("registrationNumber")
            if reg_number:
                docling_results.update_one(
                    {"_id": result["_id"]},
                    {"$set": {"registrationNumber": reg_number}}
                )
                migrated += 1

    print(f"Migrated {migrated} docling_results")
    return migrated


def migrate_qa_results():
    """Добавить registrationNumber в qa_results."""
    # Аналогично для qa_results
    pass


if __name__ == "__main__":
    migrate_docling_results()
    migrate_qa_results()
```

### Файлы для изменения

| Файл | Действие |
|------|----------|
| `docreciv/scripts/migrate_results_to_registration.py` | Создать скрипт миграции |
| Обновить Doclingproc | Добавлять registrationNumber при записи |
| Обновить LLM_qaenrich | Добавлять registrationNumber при записи |

---

## Phase 4: Aggregation Views (Priority 4)

### Цель
Создать MongoDB Views для удобной агрегации данных из всех коллекций.

### Реализация

```javascript
// View: full_protocol_trace
db.createView("full_protocol_trace", "protocols", [
  {
    $lookup: {
      from: "docling_results",
      localField: "registrationNumber",
      foreignField: "registrationNumber",
      as: "docling"
    }
  },
  {
    $lookup: {
      from: "qa_results",
      localField: "registrationNumber",
      foreignField: "registrationNumber",
      as: "qa"
    }
  },
  {
    $addFields: {
      "docling": { $arrayElemAt: ["$docling", 0] },
      "qa": { $arrayElemAt: ["$qa", 0] }
    }
  }
])

// Использование
db.full_protocol_trace.findOne({"registrationNumber": "0373200040224000001"})
```

### Python API для работы с view

```python
# docreciv/core/aggregation.py
from pymongo import MongoClient
from docreciv.core.config import get_config


class ProtocolView:
    """API для работы с агрегированным view."""

    def __init__(self):
        config = get_config()
        local_config = config.sync_db.local_mongo
        self.client = MongoClient(local_config.get_connection_url())
        self.db = self.client[local_config.db]
        self.view = self.db["full_protocol_trace"]

    def get_full_trace(self, registrationNumber: str) -> Optional[Dict[str, Any]]:
        """Получить полную информацию о протоколе с результатами."""
        return self.view.find_one({"registrationNumber": registrationNumber})

    def get_pipeline_summary(self, from_date: str, to_date: str) -> list:
        """Получить сводку по pipeline за период."""
        return list(self.view.find({
            "loadDate": {
                "$gte": f"{from_date}T00:00:00Z",
                "$lte": f"{to_date}T23:59:59Z"
            }
        }))
```

---

## Приоритеты и сроки

| Phase | Приоритет | Сложность | Срок |
|-------|-----------|-----------|------|
| Phase 1: Trace API | ⭐⭐⭐ | Средняя | 2-3 дня |
| Phase 2: Locks | ⭐⭐ | Средняя | 2-3 дня |
| Phase 3: Normalize | ⭐ | Низкая | 1-2 дня |
| Phase 4: Views | ⭐ | Низкая | 1 день |

**Общий срок**: 6-9 дней при последовательной реализации.

---

## Критерии завершения

### Phase 1
- [ ] `TraceUpdater` класс создан
- [ ] Все компоненты используют `TraceUpdater`
- [ ] Verify script показывает 100% trace chain

### Phase 2
- [ ] `ProcessingLock` класс создан
- [ ] Все компоненты используют блокировки
- [ ] Тесты concurrent обработки проходят

### Phase 3
- [ ] `registrationNumber` добавлен в docling_results
- [ ] `registrationNumber` добавлен в qa_results
- [ ] Скрипт миграции выполнен

### Phase 4
- [ ] MongoDB view создан
- [ ] `ProtocolView` API создан
- [ ] Документация обновлена

---

## Риски

| Риск | Вероятность | Влияние | Митигация |
|------|-------------|---------|-----------|
| Потеря данных при миграции | Низкая | Критично | Backup перед миграцией |
| Увеличение latency | Средняя | Среднее | Оптимизация индексов |
| Race conditions при внедрении | Средняя | Среднее | Плавный rollout |

---

## Дальнейшие шаги

1. **Создать backup** локальной MongoDB перед началом работ
2. **Реализовать Phase 1** (Trace API) — протестировать на dev среде
3. **Реализовать Phase 2** (Locks) — нагрузочное тестирование
4. **Выполнить миграцию Phase 3** — на production с минимальным downtime
5. **Создать View Phase 4** — после успешной миграции

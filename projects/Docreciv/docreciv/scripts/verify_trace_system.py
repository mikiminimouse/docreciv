#!/usr/bin/env python3
"""
Верификационный скрипт для унифицированной системы трейсинга.

Проверяет:
1. remote_mongo_id присутствует в документах protocols
2. Структура trace корректно инициализирована
3. Индексы созданы правильно
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Добавляем project root в path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pymongo import MongoClient
from docreciv.core.config import get_config


def verify_trace_system() -> Dict[str, Any]:
    """Проверяет состояние системы трейсинга."""
    config = get_config()
    results = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "checks": {}
    }

    # Подключение к локальной MongoDB
    local_config = config.sync_db.local_mongo
    connection_url = local_config.get_connection_url()

    print(f"📡 Подключение к локальной MongoDB: {local_config.server}")
    client = MongoClient(connection_url)
    db = client[local_config.db]
    collection = db[local_config.collection]

    # Проверка 1: Индексы
    print("\n" + "=" * 60)
    print("🔍 ПРОВЕРКА 1: Индексы")
    print("=" * 60)

    existing_indexes = collection.index_information()
    required_indexes = {
        "remote_mongo_id_idx": "PRIMARY TRACE INDEX",
        "trace_docreciv_unit_idx": "Component trace",
        "purchase_notice_idx": "Business key",
    }

    indexes_ok = True
    for idx_name, description in required_indexes.items():
        if idx_name in existing_indexes:
            idx_info = existing_indexes[idx_name]
            is_unique = idx_info.get("unique", False)
            print(f"  ✅ {idx_name}: {description} {'(UNIQUE)' if is_unique else ''}")
        else:
            print(f"  ❌ {idx_name}: {description} — ОТСУТСТВУЕТ")
            indexes_ok = False

    results["checks"]["indexes"] = {
        "status": "ok" if indexes_ok else "missing",
        "existing": list(existing_indexes.keys())
    }

    # Проверка 2: Структура документов
    print("\n" + "=" * 60)
    print("🔍 ПРОВЕРКА 2: Структура документов")
    print("=" * 60)

    total_docs = collection.count_documents({})
    with_remote_id = collection.count_documents({"remote_mongo_id": {"$exists": True, "$ne": ""}})
    with_trace = collection.count_documents({"trace.docreciv": {"$exists": True}})
    with_history = collection.count_documents({"history": {"$exists": True}})

    print(f"  Всего документов: {total_docs}")
    print(f"  С remote_mongo_id: {with_remote_id} ({with_remote_id/total_docs*100:.1f}%)" if total_docs > 0 else "  С remote_mongo_id: 0")
    print(f"  С trace.docreciv: {with_trace} ({with_trace/total_docs*100:.1f}%)" if total_docs > 0 else "  С trace.docreciv: 0")
    print(f"  С history: {with_history} ({with_history/total_docs*100:.1f}%)" if total_docs > 0 else "  С history: 0")

    results["checks"]["document_structure"] = {
        "total": total_docs,
        "with_remote_mongo_id": with_remote_id,
        "with_trace": with_trace,
        "with_history": with_history,
        "coverage_pct": round(with_remote_id / total_docs * 100, 1) if total_docs > 0 else 0
    }

    # Проверка 3: Пример документа
    print("\n" + "=" * 60)
    print("🔍 ПРОВЕРКА 3: Пример документа")
    print("=" * 60)

    sample = collection.find_one({"remote_mongo_id": {"$exists": True, "$ne": ""}})
    if sample:
        print(f"  _id: {str(sample.get('_id', ''))[:16]}...")
        print(f"  remote_mongo_id: {sample.get('remote_mongo_id', '')[:16]}...")
        print(f"  unit_id: {sample.get('unit_id', 'N/A')}")
        print(f"  status: {sample.get('status', 'N/A')}")

        trace = sample.get('trace', {})
        if trace:
            print(f"\n  Trace:")
            for component, data in trace.items():
                print(f"    {component}:")
                for k, v in data.items():
                    print(f"      {k}: {v}")

        history = sample.get('history', [])
        if history:
            print(f"\n  History ({len(history)} events):")
            for event in history[:3]:  # Первые 3 события
                print(f"    - {event.get('component')}.{event.get('action')} @ {event.get('timestamp', '')[:19]}")
            if len(history) > 3:
                print(f"    ... и еще {len(history) - 3}")

        results["checks"]["sample_document"] = {
            "has_remote_mongo_id": bool(sample.get('remote_mongo_id')),
            "has_trace": bool(trace),
            "has_history": bool(history),
            "trace_components": list(trace.keys()) if trace else [],
            "history_events": len(history)
        }
    else:
        print("  ⚠️  Документ с remote_mongo_id не найден")
        results["checks"]["sample_document"] = {"status": "not_found"}

    # Проверка 4: Уникальность remote_mongo_id
    print("\n" + "=" * 60)
    print("🔍 ПРОВЕРКА 4: Уникальность remote_mongo_id")
    print("=" * 60)

    pipeline = [
        {"$match": {"remote_mongo_id": {"$exists": True, "$ne": ""}}},
        {"$group": {"_id": "$remote_mongo_id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    duplicates = list(collection.aggregate(pipeline))

    if duplicates:
        print(f"  ❌ Найдено {len(duplicates)} дубликатов remote_mongo_id:")
        for dup in duplicates[:5]:
            print(f"    - {dup['_id'][:16]}... ({dup['count']} документов)")
        results["checks"]["uniqueness"] = {"status": "duplicates", "count": len(duplicates)}
    else:
        print(f"  ✅ Все remote_mongo_id уникальны")
        results["checks"]["uniqueness"] = {"status": "ok"}

    # Итоговый статус
    print("\n" + "=" * 60)
    print("📊 ИТОГОВЫЙ СТАТУС")
    print("=" * 60)

    all_ok = (
        indexes_ok and
        with_remote_id > 0 and
        sample is not None and
        len(duplicates) == 0
    )

    if all_ok:
        print("  ✅ Система трейсинга настроена КОРРЕКТНО")
    else:
        print("  ⚠️  Обнаружены проблемы — см. детали выше")

    results["overall_status"] = "ok" if all_ok else "issues_found"

    return results


if __name__ == "__main__":
    verify_trace_system()

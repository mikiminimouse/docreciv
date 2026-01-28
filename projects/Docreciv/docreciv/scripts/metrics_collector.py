"""
Скрипт для сбора метрик по протоколам за последние N дней.

Собирает статистику из MongoDB protocols collection:
- Общее количество протоколов по дням
- Количество протоколов с URL
- Распределение по статусам
- Мульти-URL протоколы

Использование:
    python -m docreciv.scripts.metrics_collector --days 7
    python -m docreciv.scripts.metrics_collector --days 7 --output metrics.json
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

from pymongo import MongoClient
from pymongo.command_cursor import CommandCursor

from docreciv.core.config import get_config
from docreciv.pipeline.models import ProtocolMetrics


def get_mongo_client() -> MongoClient:
    """Получает клиент MongoDB из конфигурации."""
    config = get_config()
    local_config = config.sync_db.local_mongo

    connection_url = (
        f"mongodb://{local_config.user}:{local_config.password}"
        f"@{local_config.server}"
    )

    return MongoClient(
        connection_url,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=5000
    )


def collect_daily_metrics(
    client: MongoClient,
    db_name: str,
    collection_name: str,
    days: int = 7
) -> List[ProtocolMetrics]:
    """
    Собирает метрики за последние N дней.

    Args:
        client: MongoDB клиент
        db_name: Имя базы данных
        collection_name: Имя коллекции
        days: Количество дней для анализа

    Returns:
        Список метрик по дням
    """
    db = client[db_name]
    collection = db[collection_name]

    # Вычисляем даты
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    print(f"Collecting metrics from {start_date.date()} to {end_date.date()}")

    # Агрегация по дням
    pipeline = [
        {
            "$match": {
                "loadDate": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$loadDate"},
                    "month": {"$month": "$loadDate"},
                    "day": {"$dayOfMonth": "$loadDate"}
                },
                "total": {"$sum": 1},
                "pending": {
                    "$sum": {
                        "$cond": [{"$eq": ["$status", "pending"]}, 1, 0]
                    }
                },
                "downloaded": {
                    "$sum": {
                        "$cond": [{"$eq": ["$status", "downloaded"]}, 1, 0]
                    }
                },
                "with_urls": {
                    "$sum": {
                        "$cond": [{"$gt": [{"$size": {"$ifNull": ["$urls", []]}}, 0]}, 1, 0]
                    }
                },
                "multi_url": {
                    "$sum": {
                        "$cond": [
                            {"$and": [
                                {"$gt": [{"$size": {"$ifNull": ["$urls", []]}}, 1]},
                                {"$eq": ["$multi_url", True]}
                            ]},
                            1,
                            0
                        ]
                    }
                }
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]

    results: List[ProtocolMetrics] = []

    try:
        cursor: CommandCursor = collection.aggregate(pipeline)

        for doc in cursor:
            date_key = doc["_id"]
            date_str = f"{date_key['year']}-{date_key['month']:02d}-{date_key['day']:02d}"

            metrics = ProtocolMetrics(
                date=date_str,
                total_protocols=doc.get("total", 0),
                pending_protocols=doc.get("pending", 0),
                downloaded_protocols=doc.get("downloaded", 0),
                with_urls=doc.get("with_urls", 0),
                multi_url_protocols=doc.get("multi_url", 0),
                without_urls=doc.get("total", 0) - doc.get("with_urls", 0)
            )
            results.append(metrics)

    except Exception as e:
        print(f"Error during aggregation: {e}")

    return results


def collect_total_metrics(
    client: MongoClient,
    db_name: str,
    collection_name: str
) -> Dict[str, Any]:
    """
    Собирает общую метрики по всей коллекции.

    Args:
        client: MongoDB клиент
        db_name: Имя базы данных
        collection_name: Имя коллекции

    Returns:
        Словарь с общими метриками
    """
    db = client[db_name]
    collection = db[collection_name]

    total = collection.count_documents({})
    pending = collection.count_documents({"status": "pending"})
    downloaded = collection.count_documents({"status": "downloaded"})

    with_urls = collection.count_documents({"urls.0": {"$exists": True}})
    without_urls = total - with_urls
    multi_url = collection.count_documents({"multi_url": True})

    return {
        "total_protocols": total,
        "pending_protocols": pending,
        "downloaded_protocols": downloaded,
        "with_urls": with_urls,
        "without_urls": without_urls,
        "multi_url_protocols": multi_url
    }


def print_metrics_table(metrics: List[ProtocolMetrics]) -> None:
    """Выводит метрики в виде таблицы."""
    if not metrics:
        print("No metrics collected.")
        return

    print("\n" + "=" * 90)
    print("PROTOCOL METRICS BY DAY")
    print("=" * 90)
    print(f"{'Date':<12} {'Total':<8} {'Pending':<10} {'Downloaded':<12} {'With URL':<10} {'No URL':<8} {'Multi-URL':<10}")
    print("-" * 90)

    total_sum = 0
    pending_sum = 0
    downloaded_sum = 0
    with_urls_sum = 0
    multi_url_sum = 0

    for m in metrics:
        print(
            f"{m.date:<12} "
            f"{m.total_protocols:<8} "
            f"{m.pending_protocols:<10} "
            f"{m.downloaded_protocols:<12} "
            f"{m.with_urls:<10} "
            f"{m.without_urls:<8} "
            f"{m.multi_url_protocols:<10}"
        )
        total_sum += m.total_protocols
        pending_sum += m.pending_protocols
        downloaded_sum += m.downloaded_protocols
        with_urls_sum += m.with_urls
        multi_url_sum += m.multi_url_protocols

    print("-" * 90)
    print(
        f"{'TOTAL':<12} "
        f"{total_sum:<8} "
        f"{pending_sum:<10} "
        f"{downloaded_sum:<12} "
        f"{with_urls_sum:<10} "
        f"{total_sum - with_urls_sum:<8} "
        f"{multi_url_sum:<10}"
    )
    print("=" * 90)


def export_metrics_json(
    metrics: List[ProtocolMetrics],
    total_metrics: Dict[str, Any],
    output_path: Path
) -> None:
    """
    Экспортирует метрики в JSON файл.

    Args:
        metrics: Список метрик по дням
        total_metrics: Общие метрики
        output_path: Путь к выходному файлу
    """
    output_data = {
        "generated_at": datetime.utcnow().isoformat(),
        "total_metrics": total_metrics,
        "daily_metrics": [m.to_dict() for m in metrics]
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"\nMetrics exported to: {output_path}")


def main():
    """Главная функция для запуска из CLI."""
    parser = argparse.ArgumentParser(
        description="Collect protocol metrics from MongoDB"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to collect metrics for (default: 7)"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output JSON file path"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="docling_metadata",
        help="Database name (default: docling_metadata)"
    )
    parser.add_argument(
        "--collection",
        type=str,
        default="protocols",
        help="Collection name (default: protocols)"
    )

    args = parser.parse_args()

    # Подключаемся к MongoDB
    try:
        client = get_mongo_client()
        # Проверяем соединение
        client.admin.command("ping")
        print("Connected to MongoDB")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        sys.exit(1)

    # Собираем метрики
    daily_metrics = collect_daily_metrics(
        client,
        args.db,
        args.collection,
        args.days
    )

    total_metrics = collect_total_metrics(
        client,
        args.db,
        args.collection
    )

    # Выводим результаты
    print("\nTOTAL COLLECTION METRICS:")
    print(f"  Total protocols: {total_metrics['total_protocols']}")
    print(f"  Pending: {total_metrics['pending_protocols']}")
    print(f"  Downloaded: {total_metrics['downloaded_protocols']}")
    print(f"  With URLs: {total_metrics['with_urls']}")
    print(f"  Without URLs: {total_metrics['without_urls']}")
    print(f"  Multi-URL: {total_metrics['multi_url_protocols']}")

    print_metrics_table(daily_metrics)

    # Экспорт в JSON если указан
    if args.output:
        output_path = Path(args.output)
        export_metrics_json(daily_metrics, total_metrics, output_path)

    client.close()


if __name__ == "__main__":
    main()

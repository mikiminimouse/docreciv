#!/usr/bin/env python3
"""
Интеграционный тест синхронизации протоколов.
Проверяет подключение к MongoDB и запуск синхронизации с ограниченным лимитом.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# Добавляем корневую директорию в PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent))

from router.protocol_sync import (
    get_remote_mongo_mcp_client,
    get_local_mongo_client,
    sync_protocols_for_date
)


def test_mongodb_connections():
    """Тест подключений к MongoDB."""
    print("=== ТЕСТ ПОДКЛЮЧЕНИЙ К MONGODB ===")

    # Тест удаленной MongoDB
    print("\n1. Тест подключения к удаленной MongoDB...")
    try:
        remote_client = get_remote_mongo_mcp_client()
        if remote_client:
            print("✅ Подключение к удаленной MongoDB успешно")
            remote_client.close()
            remote_ok = True
        else:
            print("❌ Не удалось подключиться к удаленной MongoDB")
            print("   Проверьте переменные окружения: mongoServer, readAllUser, readAllPassword, sslCertPath")
            remote_ok = False
    except Exception as e:
        print(f"❌ Ошибка подключения к удаленной MongoDB: {e}")
        remote_ok = False

    # Тест локальной MongoDB
    print("\n2. Тест подключения к локальной MongoDB...")
    try:
        local_client = get_local_mongo_client()
        if local_client:
            print("✅ Подключение к локальной MongoDB успешно")
            local_client.close()
            local_ok = True
        else:
            print("❌ Не удалось подключиться к локальной MongoDB")
            print("   Проверьте переменные окружения: MONGO_METADATA_*")
            local_ok = False
    except Exception as e:
        print(f"❌ Ошибка подключения к локальной MongoDB: {e}")
        local_ok = False

    return remote_ok, local_ok


def test_sync_small_batch():
    """Тест синхронизации с небольшим лимитом."""
    print("\n=== ТЕСТ СИНХРОНИЗАЦИИ (МАЛЕНЬКАЯ ПОРЦИЯ) ===")

    # Проверяем подключения
    remote_ok, local_ok = test_mongodb_connections()

    if not remote_ok or not local_ok:
        print("\n❌ Пропускаем тест синхронизации из-за проблем с подключением")
        return False

    # Выбираем дату для тестирования (вчера)
    target_date = datetime.utcnow() - timedelta(days=1)
    limit = 5  # Очень маленький лимит для теста

    print(f"\n3. Запуск тестовой синхронизации...")
    print(f"   Дата: {target_date.date()}")
    print(f"   Лимит: {limit} протоколов")

    try:
        result = sync_protocols_for_date(target_date, limit=limit)

        if result.get("status") == "success":
            print("✅ Синхронизация завершена успешно!")
            print(f"   Просмотрено документов: {result.get('scanned', 0)}")
            print(f"   Вставлено протоколов: {result.get('inserted', 0)}")
            print(f"   Пропущено дубликатов: {result.get('skipped_existing', 0)}")

            if result.get("errors_count", 0) > 0:
                print(f"   Ошибок: {result.get('errors_count', 0)}")
                print("   Первые ошибки:")
                for error in result.get("errors", [])[:3]:
                    print(f"     - {error}")

            # Проверяем разумность результатов
            if result.get("scanned", 0) >= result.get("inserted", 0) + result.get("skipped_existing", 0):
                print("✅ Логика результатов корректна")
                return True
            else:
                print("⚠️  Несоответствие в логике результатов")
                return False

        else:
            print(f"❌ Синхронизация завершилась с ошибкой: {result.get('message', 'Unknown error')}")
            return False

    except Exception as e:
        print(f"❌ Исключение во время синхронизации: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_sync_results():
    """Валидация результатов синхронизации в локальной БД."""
    print("\n=== ВАЛИДАЦИЯ РЕЗУЛЬТАТОВ СИНХРОНИЗАЦИИ ===")

    try:
        local_client = get_local_mongo_client()
        if not local_client:
            print("❌ Не удалось подключиться к локальной MongoDB для валидации")
            return False

        db = local_client["docling_metadata"]
        collection = db["protocols"]

        # Считаем общее количество документов
        total_count = collection.count_documents({})
        print(f"   Всего документов в коллекции: {total_count}")

        # Считаем документы с source: "remote_mongo"
        remote_count = collection.count_documents({"source": "remote_mongo"})
        print(f"   Документов из удаленной синхронизации: {remote_count}")

        # Считаем документы со статусом "pending"
        pending_count = collection.count_documents({"status": "pending"})
        print(f"   Документов со статусом 'pending': {pending_count}")

        # Проверяем структуру последнего документа
        latest_doc = collection.find_one(
            {"source": "remote_mongo"},
            sort=[("created_at", -1)]
        )

        if latest_doc:
            print("\n✅ Структура последнего синхронизированного документа:")
            required_fields = ["unit_id", "purchaseNoticeNumber", "urls", "status", "source", "created_at"]
            missing_fields = []

            for field in required_fields:
                if field not in latest_doc:
                    missing_fields.append(field)
                else:
                    if field == "unit_id":
                        print(f"   ✓ unit_id: {latest_doc[field]}")
                    elif field == "purchaseNoticeNumber":
                        print(f"   ✓ purchaseNoticeNumber: {latest_doc[field]}")
                    elif field == "urls":
                        urls = latest_doc[field]
                        print(f"   ✓ urls: {len(urls)} URL(s)")
                        if urls and len(urls) > 0:
                            print(f"     Первый URL: {urls[0].get('url', 'N/A')[:50]}...")
                    elif field == "status":
                        print(f"   ✓ status: {latest_doc[field]}")
                    elif field == "source":
                        print(f"   ✓ source: {latest_doc[field]}")

            if missing_fields:
                print(f"   ❌ Отсутствующие поля: {', '.join(missing_fields)}")
                return False
            else:
                print("   ✅ Структура документа корректна")
                return True
        else:
            print("   ⚠️  Нет документов из удаленной синхронизации")
            return True  # Это нормально, если тестовая синхронизация ничего не нашла

    except Exception as e:
        print(f"❌ Ошибка валидации: {e}")
        return False
    finally:
        if 'local_client' in locals():
            local_client.close()


def main():
    """Основная функция интеграционного теста."""
    print("🚀 ИНТЕГРАЦИОННЫЙ ТЕСТ СИНХРОНИЗАЦИИ ПРОТОКОЛОВ")
    print("=" * 60)

    # Проверяем наличие .env файла
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        print(f"❌ Файл .env не найден: {env_path}")
        print("💡 Создайте .env файл на основе .env.example")
        return 1

    print(f"✅ Найден файл конфигурации: {env_path}")

    # Запускаем тесты
    results = []

    # Тест подключений
    remote_ok, local_ok = test_mongodb_connections()
    results.append(("Подключения к MongoDB", remote_ok and local_ok))

    # Тест синхронизации (только если подключения работают)
    if remote_ok and local_ok:
        sync_ok = test_sync_small_batch()
        results.append(("Синхронизация протоколов", sync_ok))

        # Валидация результатов
        validation_ok = validate_sync_results()
        results.append(("Валидация результатов", validation_ok))
    else:
        results.append(("Синхронизация протоколов", "Пропущено"))
        results.append(("Валидация результатов", "Пропущено"))

    # Вывод результатов
    print("\n" + "=" * 60)
    print("📊 РЕЗУЛЬТАТЫ ИНТЕГРАЦИОННОГО ТЕСТИРОВАНИЯ")
    print("=" * 60)

    passed = 0
    total = 0

    for test_name, result in results:
        total += 1
        if result == "Пропущено":
            print(f"⏭️  {test_name}: ПРОПУЩЕН")
        elif result:
            print(f"✅ {test_name}: ПРОЙДЕН")
            passed += 1
        else:
            print(f"❌ {test_name}: ПРОВАЛЕН")

    print(f"\n📈 Итого: {passed}/{total} тестов пройдено")

    if passed == total:
        print("🎉 Все тесты пройдены успешно!")
        return 0
    else:
        print("⚠️  Некоторые тесты провалены. Проверьте конфигурацию.")
        return 1


if __name__ == "__main__":
    exit(main())

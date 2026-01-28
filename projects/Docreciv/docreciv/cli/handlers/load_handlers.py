"""
Handlers для загрузки и подготовки данных (пункты меню 1-3).

Включает функции:
- handle_sync_protocols: синхронизация протоколов из MongoDB
- handle_download_protocols: скачивание протоколов через VPN
- handle_check_input_files: проверка файлов в INPUT_DIR
"""

from datetime import datetime, timedelta
try:
    from ..utils import sanitize_filename
except ImportError:
    # Fallback для случаев когда импорт не работает
    def sanitize_filename(name: str) -> str:
        return name.replace('/', '_').replace('\\', '_')


def handle_sync_protocols(cli_instance):
    """Синхронизация протоколов из удалённой MongoDB в локальную."""
    print("\n=== СИНХРОНИЗАЦИЯ ПРОТОКОЛОВ ИЗ УДАЛЁННОЙ MONGODB ===")
    print("Микросервис sync_db - первый компонент препроцессинга")

    # Импорт нового микросервиса синхронизации
    try:
        from docreciv.sync_db.enhanced_service import EnhancedSyncService
        from docreciv.sync_db.health_checks import run_comprehensive_health_check, print_health_check_report
    except ImportError as e:
        print(f"❌ Модуль sync_db не найден: {e}")
        print("Убедитесь, что PYTHONPATH настроен корректно")
        return

    # Выбор типа синхронизации
    print("\n🔧 ВЫБОР ТИПА ОПЕРАЦИИ:")
    print("  1. Синхронизация протоколов")
    print("  2. Проверка здоровья системы")
    operation = input("  Выберите [1-2] или Enter для синхронизации: ").strip() or "1"

    if operation == "2":
        # Health check operation
        print("\n🏥 ЗАПУСК ПРОВЕРКИ ЗДОРОВЬЯ СИСТЕМЫ...")
        try:
            results = run_comprehensive_health_check()
            print_health_check_report(results)
        except Exception as e:
            print(f"❌ Ошибка при проверке здоровья: {e}")
        return

    # Create service
    try:
        sync_service = EnhancedSyncService()
    except Exception as e:
        print(f"❌ Ошибка создания сервиса синхронизации: {e}")
        return

    # Выбор типа синхронизации
    print("\n📅 ВЫБОР ТИПА СИНХРОНИЗАЦИИ:")
    print("  1. Одна дата (по умолчанию)")
    print("  2. Диапазон дат (начало - конец)")
    print("  3. Начальная дата + количество дней")
    print("  4. Ежедневное обновление (вчерашний день)")
    print("  5. Полная синхронизация (последние 14 дней)")
    sync_type = input("  Выберите [1-5] или Enter для одной даты: ").strip() or "1"

    # Переменные для хранения дат
    target_date = None
    start_date = None
    end_date = None

    if sync_type == "1":
        # Одна дата
        print("\n📅 ВЫБОР ДАТЫ ДЛЯ СИНХРОНИЗАЦИИ:")
        print("  1. Вчерашний день (по умолчанию)")
        print("  2. Указать дату вручную (YYYY-MM-DD)")
        choice = input("  Выберите [1-2] или Enter для вчерашнего дня: ").strip()

        if choice == "2":
            date_str = input("  Введите дату (YYYY-MM-DD): ").strip()
            try:
                target_date = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                print(f"✗ Неверный формат даты: {date_str}")
                return
        else:
            target_date = datetime.utcnow() - timedelta(days=1)
    elif sync_type == "2":
        # Диапазон дат
        print("\n📅 ВВОД ДИАПАЗОНА ДАТ:")
        start_str = input("  Введите начальную дату (YYYY-MM-DD): ").strip()
        end_str = input("  Введите конечную дату (YYYY-MM-DD): ").strip()
        
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_str, "%Y-%m-%d")
            
            if start_date > end_date:
                print("✗ Начальная дата должна быть меньше или равна конечной")
                return
        except ValueError as e:
            print(f"✗ Неверный формат даты: {e}")
            return
    elif sync_type == "3":
        # Начальная дата + количество дней
        print("\n📅 ВВОД НАЧАЛЬНОЙ ДАТЫ И КОЛИЧЕСТВА ДНЕЙ:")
        start_str = input("  Введите начальную дату (YYYY-MM-DD): ").strip()
        days_str = input("  Введите количество дней: ").strip()
        
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d")
            days = int(days_str)
            if days <= 0:
                print("✗ Количество дней должно быть больше 0")
                return
            end_date = start_date + timedelta(days=days-1)
        except ValueError as e:
            print(f"✗ Ошибка ввода: {e}")
            return
    elif sync_type == "4":
        # Ежедневное обновление
        target_date = datetime.utcnow() - timedelta(days=1)
        print(f"  Будет выполнена синхронизация за: {target_date.date()}")
    elif sync_type == "5":
        # Полная синхронизация
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=14)
        print(f"  Будет выполнена полная синхронизация за период: {start_date.date()} - {end_date.date()}")
    else:
        print("✗ Неверный выбор типа синхронизации")
        return

    # Лимит протоколов
    limit_str = input(f"\n🔢 ЛИМИТ ПРОТОКОЛОВ (по умолчанию: 0 - без ограничения): ").strip()
    limit = int(limit_str) if limit_str else 0

    # Запуск полной синхронизации (5 этапов)
    print(f"\n🚀 ЗАПУСК ПОЛНОЙ СИНХРОНИЗАЦИИ:")
    if target_date:
        print(f"   Дата: {target_date.date()}")
    else:
        print(f"   Период: {start_date.date()} - {end_date.date()}")
    if limit > 0:
        print(f"   Лимит: {limit}")
    else:
        print("   Лимит: без ограничения")
    print(f"   Микросервис: enhanced_sync_db")

    # Выполняем синхронизацию
    if target_date:
        result = sync_service.sync_protocols_for_date(target_date, limit if limit > 0 else None)
    elif start_date and end_date:
        result = sync_service.sync_protocols_for_date_range(start_date, end_date, limit if limit > 0 else None)
    else:
        print("✗ Неверные параметры синхронизации")
        return

    # Вывод результатов
    if result.success:
        print("\n✅ СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")
        print("📊 РЕЗУЛЬТАТЫ:")
        print(f"   📅 Дата/Период: {result.date}")
        print(f"   🔍 Просмотрено документов: {result.scanned}")
        print(f"   💾 Новых протоколов: {result.inserted}")
        print(f"   ⏭️  Пропущено дубликатов: {result.skipped_existing}")
        print(f"   ⚠️  Ошибок обработки: {result.errors_count}")
        print(f"   ⏱️  Длительность: {result.duration:.2f} секунд")
        
        # Детальная статистика
        if result.statistics:
            stats = result.statistics
            print("\n📈 ДЕТАЛЬНАЯ СТАТИСТИКА:")
            
            if "url_distribution" in stats:
                url_dist = stats["url_distribution"]
                print(f"   Распределение URL:")
                print(f"     Одиночные URL: {url_dist['single_url']}")
                print(f"     Множественные URL: {url_dist['multi_url']}")
                print(f"     Без URL: {url_dist['no_url']}")
            
            if "attachment_types" in stats and stats["attachment_types"]:
                print(f"   Типы вложений:")
                for att_type, count in stats["attachment_types"].items():
                    print(f"     {att_type}: {count}")
            
            if "average_processing_time" in stats:
                print(f"   Среднее время обработки: {stats['average_processing_time']:.4f} сек")
                print(f"   Максимальное время: {stats['max_processing_time']:.4f} сек")
                print(f"   Минимальное время: {stats['min_processing_time']:.4f} сек")
        
        if result.errors_count > 0:
            print("\n📝 Подробности ошибок:")
            for i, error in enumerate(result.errors[:5], 1):
                print(f"     {i}. {error[:100]}{'...' if len(error) > 100 else ''}")
            if len(result.errors) > 5:
                print(f"     ... и еще {len(result.errors) - 5} ошибок")
                
        if result.warnings:
            print("\n⚠️  Предупреждения:")
            for i, warning in enumerate(result.warnings[:3], 1):
                print(f"     {i}. {warning}")
    else:
        print(f"\n❌ ОШИБКА СИНХРОНИЗАЦИИ: {result.message}")
        if hasattr(result, 'errors') and result.errors:
            print("   Подробности:")
            for error in result.errors[:5]:
                print(f"   • {error}")


def handle_download_protocols(cli_instance):
    """Скачивание протоколов из локальной MongoDB через VPN."""
    print("\n=== СКАЧИВАНИЕ ПРОТОКОЛОВ ИЗ MONGODB (С VPN) ===")

    # Проверяем доступность модулей скачивания
    try:
        # Import moved to where it's used
        from downloader.utils import check_zakupki_health
    except ImportError:
        print("❌ Модули скачивания не доступны")
        return

    # Проверка VPN подключения
    print("\n1. Проверка доступности zakupki.gov.ru через VPN...")
    if not check_zakupki_health():
        print("✗ zakupki.gov.ru недоступен (нет VPN / блокировка)")
        print("  Убедитесь, что VPN настроен через route-up-zakupki.sh")
        print("  Проверьте, что OpenVPN туннель активен")
        return

    print("✓ zakupki.gov.ru доступен через VPN")

    # Выбор периода для скачивания
    print("\n📅 ВЫБОР ПЕРИОДА ДЛЯ СКАЧИВАНИЯ:")
    print("  1. Все ожидающие протоколы (по умолчанию)")
    print("  2. Протоколы за конкретную дату")
    print("  3. Протоколы за диапазон дат")
    period_choice = input("  Выберите [1-3] или Enter для всех протоколов: ").strip() or "1"

    # Лимит протоколов
    limit_str = input(f"\n2. Лимит протоколов/units для скачивания (по умолчанию: 0 - без ограничения): ").strip()
    limit = int(limit_str) if limit_str else 0

    if limit < 0:
        print("✗ Лимит должен быть больше или равен 0")
        return

    # Запуск скачивания
    print(f"\n3. Запуск скачивания протоколов...")
    if limit > 0:
        print(f"   Лимит: {limit} протоколов")
    else:
        print("   Лимит: без ограничения")
    print(f"   Директория: {cli_instance.INPUT_DIR.absolute()}")

    try:
        from docreciv.downloader.enhanced_service import EnhancedProtocolDownloader
        
        # Use EnhancedProtocolDownloader with proper output directory
        downloader = EnhancedProtocolDownloader(output_dir=cli_instance.INPUT_DIR)
        start_time = cli_instance.time.time()
        result = downloader.process_pending_protocols(limit=limit if limit > 0 else 0)
        duration = cli_instance.time.time() - start_time

        print("\n✓ Скачивание завершено!")
        print(f"   Время: {duration:.1f} сек")
        print(f"   Статус: {result.status}")
        print(f"   Протоколы обработано: {result.processed}")
        print(f"   Документы скачано: {result.downloaded}")
        print(f"   Ошибок: {result.failed}")
        
        if result.errors:
            print(f"\n   Ошибки:")
            for error in result.errors[:5]:  # Показываем первые 5 ошибок
                print(f"     - {error}")
            if len(result.errors) > 5:
                print(f"     ... и еще {len(result.errors) - 5} ошибок")

    except Exception as e:
        print(f"❌ Ошибка скачивания: {e}")
        import traceback
        traceback.print_exc()


def handle_check_input_files(cli_instance):
    """Проверка файлов в INPUT_DIR."""
    print("\n=== ПРОВЕРКА INPUT_DIR ===")

    if not cli_instance.INPUT_DIR.exists():
        print(f"❌ Директория {cli_instance.INPUT_DIR} не существует")
        return

    files = [f for f in cli_instance.INPUT_DIR.rglob("*") if f.is_file()]
    files = [f for f in files if f.is_file() and not f.name.startswith('.')]

    print(f"📁 Найдено файлов: {len(files)}")

    if files:
        print("\nФайлы:")
        for i, file_path in enumerate(files[:10], 1):  # Показываем первые 10
            size_mb = file_path.stat().st_size / (1024 * 1024)
            print(f"   {i:2d}. {file_path.relative_to(cli_instance.INPUT_DIR)} ({size_mb:.1f} MB)")

        if len(files) > 10:
            print(f"... и еще {len(files) - 10} файлов")
    else:
        print("📭 INPUT_DIR пуст")
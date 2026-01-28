#!/usr/bin/env python3
"""
Автоматический тест CLI на 20 units.
Выполняет полную обработку и собирает детальные метрики.
"""
import sys
from pathlib import Path
import time
from datetime import datetime
import json

# Добавляем путь к preprocessing
sys.path.insert(0, str(Path(__file__).parent))

# Импортируем из cli.py (не из cli/ директории)
import importlib.util
spec = importlib.util.spec_from_file_location("cli_module", Path(__file__).parent / "cli.py")
cli_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli_module)
PreprocessingTestCLI = cli_module.PreprocessingTestCLI
from router.config import INPUT_DIR, PENDING_DIR, READY_DOCLING_DIR
from router.metrics import init_processing_metrics, save_processing_metrics, get_current_metrics
from router.unit_distribution_new import get_unit_statistics
from router.merge import get_ready_docling_statistics

def main():
    """Запуск автоматического тестирования на 20 units."""
    print("=" * 70)
    print("🧪 АВТОМАТИЧЕСКОЕ ТЕСТИРОВАНИЕ CLI НА 20 UNITS")
    print("=" * 70)
    print()
    
    # Инициализация
    cli = PreprocessingTestCLI()
    
    # Инициализируем метрики
    init_processing_metrics()
    metrics = get_current_metrics()
    session_id = metrics.get("session_id", "unknown")
    print(f"📊 Session ID: {session_id}")
    print()
    
    start_time = time.time()
    
    # ШАГ 1: Проверка входных файлов
    print("=" * 70)
    print("ШАГ 1: ПРОВЕРКА ВХОДНЫХ ФАЙЛОВ")
    print("=" * 70)
    
    # Ищем файлы в корне INPUT_DIR
    files = list(INPUT_DIR.glob("*"))
    files = [f for f in files if f.is_file() and not f.name.startswith('.')]
    
    # Если файлов нет в корне, ищем внутри UNIT_* директорий
    if len(files) == 0:
        print("📁 Файлов в корне INPUT_DIR не найдено, ищем внутри UNIT_* директорий...")
        unit_dirs = [d for d in INPUT_DIR.iterdir() if d.is_dir() and d.name.startswith("UNIT_")]
        for unit_dir in unit_dirs:
            unit_files = list(unit_dir.glob("*"))
            unit_files = [f for f in unit_files if f.is_file() and not f.name.startswith('.')]
            files.extend(unit_files)
    
    print(f"📁 Найдено файлов для обработки: {len(files)}")
    if len(files) == 0:
        print("❌ Нет файлов для обработки!")
        print("💡 Подсказка: Запустите пункт 2 (Скачивание протоколов) для загрузки файлов")
        return
    
    # Показываем первые 5 файлов
    print(f"📄 Примеры файлов (первые 5):")
    for f in files[:5]:
        print(f"   - {f.name} ({f.stat().st_size / 1024:.1f} KB)")
    if len(files) > 5:
        print(f"   ... и еще {len(files) - 5} файлов")
    print()
    
    # ШАГ 2: Полная обработка (все 5 шагов)
    print("=" * 70)
    print("ШАГ 2: ПОЛНАЯ ОБРАБОТКА (ШАГИ 1-5)")
    print("=" * 70)
    print()
    
    try:
        # Ограничиваем до 20 файлов для теста
        limit = 20
        print(f"🔢 Лимит обработки: {limit} файлов")
        print()
        
        # Выполняем полную обработку
        cli.handle_full_processing(limit=limit)
        
    except Exception as e:
        print(f"❌ Ошибка при полной обработке: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ШАГ 3: Сбор статистики
    print()
    print("=" * 70)
    print("ШАГ 3: СБОР СТАТИСТИКИ")
    print("=" * 70)
    
    # Статистика по PENDING
    print("\n📊 Статистика PENDING директорий:")
    pending_stats = get_unit_statistics(PENDING_DIR)
    total_units = 0
    total_files = 0
    for category, stats in pending_stats.items():
        units = stats.get("units", 0)
        files = stats.get("files", 0)
        total_units += units
        total_files += files
        if units > 0 or files > 0:
            print(f"   {category:12s}: {units:3d} units, {files:3d} файлов")
    print(f"   {'ИТОГО':12s}: {total_units:3d} units, {total_files:3d} файлов")
    
    # Статистика по READY_DOCLING
    print("\n📊 Статистика READY_DOCLING:")
    try:
        ready_stats = get_ready_docling_statistics()  # Исправлено: убрал аргумент
        print(f"   Units готовых к Docling: {ready_stats.get('total_units', 0)}")
        print(f"   Файлов готовых к Docling: {ready_stats.get('total_files', 0)}")
        
        # Детальная статистика по типам
        by_type = ready_stats.get('by_type', {})
        if by_type:
            print("   По типам файлов:")
            for file_type, type_stats in sorted(by_type.items()):
                print(f"      {file_type}: {type_stats.get('units', 0)} units, {type_stats.get('files', 0)} файлов")
    except Exception as e:
        print(f"   ⚠️  Ошибка получения статистики: {e}")
        import traceback
        traceback.print_exc()
    
    # Метрики сессии
    print("\n📊 Метрики сессии обработки:")
    final_metrics = get_current_metrics()
    if final_metrics:
        summary = final_metrics.get("summary", {})
        print(f"   Всего входных файлов: {summary.get('total_input_files', 0)}")
        print(f"   Всего архивов: {summary.get('total_archives', 0)}")
        print(f"   Всего извлечено: {summary.get('total_extracted', 0)}")
        print(f"   Всего units: {summary.get('total_units', 0)}")
        print(f"   Всего ошибок: {summary.get('total_errors', 0)}")
        
        # Статистика по типам
        by_type = summary.get("by_detected_type", {})
        if by_type:
            print("\n   По типам файлов:")
            for file_type, count in sorted(by_type.items(), key=lambda x: x[1], reverse=True):
                print(f"      {file_type}: {count}")
        
        # Статистика по расширениям
        by_ext = summary.get("by_extension", {})
        if by_ext:
            print("\n   По расширениям файлов:")
            for ext, count in sorted(by_ext.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"      {ext}: {count}")
        
        # PDF статистика
        pdf_stats = summary.get("pdf_statistics", {})
        if pdf_stats.get("total_pdf", 0) > 0:
            print("\n   PDF статистика:")
            print(f"      Всего PDF: {pdf_stats.get('total_pdf', 0)}")
            print(f"      С текстовым слоем: {pdf_stats.get('pdf_with_text_layer', 0)}")
            print(f"      Требуют OCR: {pdf_stats.get('pdf_requires_ocr', 0)}")
        
        # Статистика дубликатов
        dup_stats = summary.get("duplicate_statistics", {})
        if dup_stats.get("total_duplicate_files", 0) > 0:
            print("\n   Дубликаты:")
            print(f"      Всего дубликатов: {dup_stats.get('total_duplicate_files', 0)}")
            print(f"      Групп дубликатов: {dup_stats.get('duplicate_groups_count', 0)}")
        
        # Статистика конвертаций
        conv_stats = summary.get("doc_conversion_statistics", {})
        if conv_stats.get("total_attempted", 0) > 0:
            print("\n   Конвертации DOC->DOCX:")
            print(f"      Попыток: {conv_stats.get('total_attempted', 0)}")
            print(f"      Успешно: {conv_stats.get('successful', 0)}")
            print(f"      Ошибок: {conv_stats.get('failed', 0)}")
            if conv_stats.get("avg_conversion_time", 0) > 0:
                print(f"      Среднее время: {conv_stats.get('avg_conversion_time', 0):.2f} сек")
        
        # Статистика pending
        pending_stats = summary.get("pending_statistics", {})
        if any(pending_stats.values()):
            print("\n   Pending статистика:")
            print(f"      В normalize: {pending_stats.get('files_in_pending_normalize', 0)}")
            print(f"      В convert: {pending_stats.get('files_in_pending_convert', 0)}")
            print(f"      В extract: {pending_stats.get('files_in_pending_extract', 0)}")
            print(f"      Обработано из pending: {pending_stats.get('files_processed_from_pending', 0)}")
        
        # Ошибки (первые 5)
        errors = final_metrics.get("errors", [])
        if errors:
            print(f"\n   Ошибки (показано первых {min(5, len(errors))}):")
            for error in errors[:5]:
                error_file = error.get("file", "unknown")
                error_msg = error.get("error", "unknown")
                print(f"      {error_file}: {error_msg[:80]}...")
    
    # Сохраняем метрики
    try:
        save_processing_metrics()
        print("\n💾 Метрики сохранены")
    except Exception as e:
        print(f"\n⚠️  Ошибка сохранения метрик: {e}")
    
    # Итоговое время
    elapsed_time = time.time() - start_time
    print()
    print("=" * 70)
    print("✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("=" * 70)
    print(f"⏱️  Время выполнения: {elapsed_time:.2f} секунд ({elapsed_time/60:.2f} минут)")
    print(f"📊 Обработано units: {total_units}")
    print(f"📄 Обработано файлов: {total_files}")
    print()

if __name__ == "__main__":
    main()


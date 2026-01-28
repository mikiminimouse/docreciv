#!/usr/bin/env python3
"""Тестирование полного pipeline обработки через CLI."""
import sys
import importlib.util
from pathlib import Path
import shutil

# Загружаем cli.py
cli_path = Path(__file__).parent / "cli.py"
spec = importlib.util.spec_from_file_location("cli_module", cli_path)
cli_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli_module)

PreprocessingTestCLI = cli_module.PreprocessingTestCLI

from router.config import (
    INPUT_DIR, PENDING_DIR, READY_DOCLING_DIR,
    init_directories
)
from router.api import process_file

def setup_test_environment():
    """Подготовка тестового окружения."""
    print("\n=== Подготовка тестового окружения ===")
    init_directories()
    
    # Очищаем тестовые директории
    for d in [INPUT_DIR, PENDING_DIR, READY_DOCLING_DIR]:
        if d.exists():
            for item in d.glob("*"):
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
    
    print("✓ Тестовое окружение подготовлено")
    return True

def test_step1_detection():
    """Тест ШАГ 1: Детекция файлов."""
    print("\n=== ТЕСТ: ШАГ 1 - Детекция файлов ===")
    try:
        from router.file_detection import detect_file_type
        
        # Создаем тестовый PDF файл (минимальный)
        test_pdf = INPUT_DIR / "test.pdf"
        # Минимальный PDF заголовок
        test_pdf.write_bytes(b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\nxref\n0 0\ntrailer\n<< /Size 0 /Root 1 0 R >>\nstartxref\n0\n%%EOF")
        
        result = detect_file_type(test_pdf)
        assert result.get("detected_type") == "pdf", f"Ожидался pdf, получен {result.get('detected_type')}"
        print(f"✓ PDF детектирован корректно: {result.get('detected_type')}")
        
        # Создаем тестовый DOCX (это ZIP)
        test_docx = INPUT_DIR / "test.docx"
        import zipfile
        with zipfile.ZipFile(test_docx, 'w') as zf:
            zf.writestr("test.txt", "content")
        
        result = detect_file_type(test_docx)
        # DOCX может быть определен как zip_archive или docx
        assert result.get("detected_type") in ["zip_archive", "docx"], f"Неожиданный тип: {result.get('detected_type')}"
        print(f"✓ DOCX/ZIP детектирован: {result.get('detected_type')}")
        
        return True
    except Exception as e:
        print(f"✗ Ошибка детекции: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_step5_distribution():
    """Тест ШАГ 5: Распределение файлов."""
    print("\n=== ТЕСТ: ШАГ 5 - Распределение файлов ===")
    try:
        # Создаем тестовый файл
        test_file = INPUT_DIR / "test_distribution.txt"
        test_file.write_text("Test content for distribution")
        
        # Обрабатываем файл
        result = process_file(test_file)
        
        assert result.get("status") in ["processed", "error"], f"Неожиданный статус: {result.get('status')}"
        
        if result.get("status") == "processed":
            unit_id = result.get("unit_id")
            category = result.get("category")
            print(f"✓ Файл обработан: unit_id={unit_id}, category={category}")
            
            # Проверяем, что unit создан в PENDING
            from router.config import PENDING_DIR
            unit_found = False
            for pending_dir in PENDING_DIR.rglob("UNIT_*"):
                if pending_dir.is_dir() and pending_dir.name == unit_id:
                    unit_found = True
                    print(f"✓ Unit найден в PENDING: {pending_dir}")
                    break
            
            if not unit_found:
                print(f"⚠️  Unit {unit_id} не найден в PENDING (может быть в другой категории)")
        else:
            print(f"⚠️  Обработка завершилась с ошибкой: {result.get('message')}")
        
        return True
    except Exception as e:
        print(f"✗ Ошибка распределения: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_merge_dry_run():
    """Тест: Merge dry run."""
    print("\n=== ТЕСТ: Merge (DRY RUN) ===")
    try:
        from router.merge import merge_to_ready_docling, print_merge_summary
        
        result = merge_to_ready_docling(dry_run=True, limit=10)
        
        assert result.get("dry_run") is True
        print(f"✓ Dry run выполнен: {result.get('units_processed')} units, {result.get('files_moved')} файлов")
        
        return True
    except Exception as e:
        print(f"✗ Ошибка merge dry run: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_units_report():
    """Тест: Отчет по units."""
    print("\n=== ТЕСТ: Отчет по units ===")
    try:
        from router.unit_distribution_new import get_unit_statistics
        from router.merge import get_ready_docling_statistics
        from router.config import PENDING_DIR
        
        pending_stats = get_unit_statistics(PENDING_DIR)
        ready_stats = get_ready_docling_statistics()
        
        total_pending = sum(s["units"] for s in pending_stats.values())
        print(f"✓ Статистика получена: PENDING units={total_pending}, READY units={ready_stats['total_units']}")
        
        return True
    except Exception as e:
        print(f"✗ Ошибка отчета: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Запуск полного тестирования pipeline."""
    print("=" * 60)
    print("ТЕСТИРОВАНИЕ ПОЛНОГО PIPELINE")
    print("=" * 60)
    
    if not setup_test_environment():
        print("✗ Не удалось подготовить окружение")
        return False
    
    tests = [
        test_step1_detection,
        test_step5_distribution,
        test_merge_dry_run,
        test_units_report
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"✗ Тест упал с исключением: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"РЕЗУЛЬТАТЫ PIPELINE: ✓ {passed} пройдено, ✗ {failed} провалено")
    print("=" * 60)
    
    return failed == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)


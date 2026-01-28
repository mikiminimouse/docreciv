#!/usr/bin/env python3
"""Тестирование основных функций CLI после рефакторинга."""
import sys
import importlib.util
from pathlib import Path

# Загружаем cli.py
cli_path = Path(__file__).parent / "cli.py"
spec = importlib.util.spec_from_file_location("cli_module", cli_path)
cli_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli_module)

PreprocessingTestCLI = cli_module.PreprocessingTestCLI

def test_cli_initialization():
    """Тест 1: Инициализация CLI."""
    print("\n=== ТЕСТ 1: Инициализация CLI ===")
    try:
        cli = PreprocessingTestCLI()
        assert cli.metrics is None
        assert cli.session_id is None
        assert cli.local_metrics_dir.exists()
        print("✓ CLI инициализируется корректно")
        return cli
    except Exception as e:
        print(f"✗ Ошибка инициализации: {e}")
        raise

def test_imports():
    """Тест 2: Проверка импортов."""
    print("\n=== ТЕСТ 2: Проверка импортов ===")
    try:
        from router.file_detection import detect_file_type
        from router.archive import safe_extract_archive
        from router.utils import calculate_sha256
        from router.mongo import get_mongo_client
        from router.config import (
            PENDING_DIR, PENDING_DIRECT_DIR, PENDING_NORMALIZE_DIR,
            PENDING_CONVERT_DIR, PENDING_EXTRACT_DIR, PENDING_SPECIAL_DIR,
            READY_DOCLING_DIR
        )
        from router.merge import merge_to_ready_docling, get_ready_docling_statistics
        from router.file_classifier import classify_file
        print("✓ Все импорты работают")
        return True
    except Exception as e:
        print(f"✗ Ошибка импортов: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_file_detection():
    """Тест 3: Детекция файлов."""
    print("\n=== ТЕСТ 3: Детекция файлов ===")
    try:
        from router.file_detection import detect_file_type
        from router.config import INPUT_DIR
        
        # Создаем тестовый файл
        test_file = INPUT_DIR / "test_detection.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text("Test content for detection")
        
        result = detect_file_type(test_file)
        assert "detected_type" in result
        print(f"✓ Детекция работает: {result.get('detected_type')}")
        
        test_file.unlink()
        return True
    except Exception as e:
        print(f"✗ Ошибка детекции: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_metrics_local_save():
    """Тест 4: Локальное сохранение метрик."""
    print("\n=== ТЕСТ 4: Локальное сохранение метрик ===")
    try:
        cli = PreprocessingTestCLI()
        from router.metrics import init_processing_metrics
        
        # Инициализируем метрики
        metrics = init_processing_metrics()
        cli.metrics = metrics
        
        # Сохраняем локально
        saved = cli.save_metrics_local()
        assert saved, "Метрики должны быть сохранены"
        
        # Проверяем, что файл создан
        metrics_file = cli.local_metrics_dir / f"metrics_{metrics['session_id']}.json"
        assert metrics_file.exists(), "Файл метрик должен существовать"
        
        # Загружаем обратно
        loaded = cli.load_metrics_local(metrics['session_id'])
        assert loaded is not None, "Метрики должны загружаться"
        assert loaded['session_id'] == metrics['session_id']
        
        print(f"✓ Локальное сохранение/загрузка работает")
        return True
    except Exception as e:
        print(f"✗ Ошибка сохранения метрик: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_merge_functions():
    """Тест 5: Функции merge."""
    print("\n=== ТЕСТ 5: Функции merge ===")
    try:
        from router.merge import merge_to_ready_docling, get_ready_docling_statistics
        
        # Тест dry_run
        result = merge_to_ready_docling(dry_run=True, limit=0)
        assert "dry_run" in result
        assert result["dry_run"] is True
        print("✓ merge_to_ready_docling (dry_run) работает")
        
        # Тест статистики
        stats = get_ready_docling_statistics()
        assert "total_units" in stats
        assert "total_files" in stats
        print("✓ get_ready_docling_statistics работает")
        
        return True
    except Exception as e:
        print(f"✗ Ошибка merge функций: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_unit_statistics():
    """Тест 6: Статистика units."""
    print("\n=== ТЕСТ 6: Статистика units ===")
    try:
        from router.unit_distribution_new import get_unit_statistics
        from router.config import PENDING_DIR
        
        stats = get_unit_statistics(PENDING_DIR)
        assert isinstance(stats, dict)
        assert "direct" in stats
        print("✓ get_unit_statistics работает")
        return True
    except Exception as e:
        print(f"✗ Ошибка статистики units: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_cli_handlers():
    """Тест 7: Проверка наличия обработчиков."""
    print("\n=== ТЕСТ 7: Проверка обработчиков ===")
    try:
        cli = PreprocessingTestCLI()
        
        handlers = [
            "handle_merge_dry_run",
            "handle_merge_real",
            "handle_units_report",
            "handle_step4_check_mixed",
            "handle_step5_distribute",
            "handle_view_pending_structure",
            "handle_category_statistics"
        ]
        
        for handler_name in handlers:
            assert hasattr(cli, handler_name), f"Обработчик {handler_name} отсутствует"
            handler = getattr(cli, handler_name)
            assert callable(handler), f"Обработчик {handler_name} не вызываемый"
        
        print(f"✓ Все {len(handlers)} обработчиков присутствуют и вызываемы")
        return True
    except Exception as e:
        print(f"✗ Ошибка проверки обработчиков: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Запуск всех тестов."""
    print("=" * 60)
    print("ТЕСТИРОВАНИЕ CLI ПОСЛЕ РЕФАКТОРИНГА")
    print("=" * 60)
    
    tests = [
        test_cli_initialization,
        test_imports,
        test_file_detection,
        test_metrics_local_save,
        test_merge_functions,
        test_unit_statistics,
        test_cli_handlers
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
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"РЕЗУЛЬТАТЫ: ✓ {passed} пройдено, ✗ {failed} провалено")
    print("=" * 60)
    
    return failed == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)


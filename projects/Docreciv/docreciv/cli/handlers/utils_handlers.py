"""
Handlers для служебных функций (пункты меню 25-27).

Включает функции:
- handle_cleanup_test_data: очистка данных
- handle_create_test_files: создание тестовых файлов
- handle_check_infrastructure: проверка инфраструктуры
"""

from datetime import datetime
from pathlib import Path
import os
import subprocess
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def handle_cleanup_test_data(cli_instance):
    """Очистка тестовых данных."""
    print("\n=== ОЧИСТКА ТЕСТОВЫХ ДАННЫХ ===")

    dirs_to_clean = [
        cli_instance.TEMP_DIR, 
        cli_instance.EXTRACTED_DIR, 
        cli_instance.NORMALIZED_DIR, 
        cli_instance.ARCHIVE_DIR
    ]

    for directory in dirs_to_clean:
        if directory.exists():
            print(f"🧹 Очистка {directory}...")
            for item in directory.glob("*"):
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    import shutil
                    shutil.rmtree(item)
            print(f"   ✅ Очищено")

    print("🎉 Очистка завершена")


def handle_create_test_files(cli_instance):
    """Создание тестовых файлов."""
    print("\n=== СОЗДАНИЕ ТЕСТОВЫХ ФАЙЛОВ ===")

    # Создаем простой текстовый файл
    test_file = cli_instance.INPUT_DIR / "test_document.txt"
    test_file.write_text("Это тестовый документ для проверки препроцессинга.")

    # Создаем простой PDF (если возможно)
    print("📄 Создан тестовый файл: test_document.txt")

    print("✅ Тестовые файлы созданы")


def _check_docker_status():
    """Check Docker and Docker Compose status."""
    try:
        # Check Docker daemon
        result = subprocess.run(
            ["docker", "info"], 
            capture_output=True, 
            text=True, 
            timeout=10
        )
        if result.returncode == 0:
            print("✅ Docker daemon: запущен")
        else:
            print("❌ Docker daemon: не запущен")
            return False
            
        # Check Docker Compose
        result = subprocess.run(
            ["docker-compose", "version"], 
            capture_output=True, 
            text=True, 
            timeout=10
        )
        if result.returncode == 0:
            print("✅ Docker Compose: доступен")
        else:
            print("❌ Docker Compose: не доступен")
            
        return True
    except subprocess.TimeoutExpired:
        print("❌ Docker check: таймаут")
        return False
    except FileNotFoundError:
        print("❌ Docker: не установлен")
        return False
    except Exception as e:
        print(f"❌ Docker check error: {e}")
        return False


def _check_running_containers():
    """Check running containers."""
    try:
        result = subprocess.run(
            ["docker-compose", "ps", "--services", "--filter", "status=running"], 
            capture_output=True, 
            text=True, 
            timeout=10,
            cwd=project_root
        )
        if result.returncode == 0:
            services = result.stdout.strip().split('\n') if result.stdout.strip() else []
            if services and services[0]:  # Check if not empty
                print(f"✅ Запущенные сервисы ({len(services)}): {', '.join(services[:5])}")
                if len(services) > 5:
                    print(f"   ... и еще {len(services) - 5} сервисов")
            else:
                print("⚠️  Нет запущенных сервисов")
        else:
            print("❌ Не удалось получить список сервисов")
    except Exception as e:
        print(f"❌ Ошибка проверки контейнеров: {e}")


def _check_vpn_status():
    """Check VPN status."""
    try:
        # Try to ping the remote MongoDB server
        import socket
        server = os.environ.get("MONGO_SERVER", "192.168.0.46:8635")
        host, port = server.split(":") if ":" in server else (server, "27017")
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, int(port)))
        sock.close()
        
        if result == 0:
            print("✅ VPN: подключен (удаленный сервер доступен)")
        else:
            print("⚠️  VPN: возможны проблемы (удаленный сервер недоступен)")
            
    except Exception as e:
        print(f"⚠️  VPN check: {e}")


def _check_disk_space():
    """Check available disk space."""
    try:
        # Get disk usage for the root directory
        import shutil
        total, used, free = shutil.disk_usage("/")
        
        # Convert to GB
        total_gb = total / (1024**3)
        used_gb = used / (1024**3)
        free_gb = free / (1024**3)
        usage_percent = (used / total) * 100
        
        print(f"💾 Диск: {free_gb:.1f} GB свободно из {total_gb:.1f} GB ({usage_percent:.1f}% занято)")
        
        # Warn if less than 10% free
        if free_gb < 10 or usage_percent > 90:
            print("⚠️  Мало свободного места на диске!")
            
    except Exception as e:
        print(f"❌ Ошибка проверки диска: {e}")


def _check_python_packages():
    """Check required Python packages."""
    required_packages = [
        "pymongo",
        "requests",
        "apscheduler",
        "urllib3"
    ]
    
    print("🐍 Python packages check:")
    for package in required_packages:
        try:
            __import__(package)
            print(f"   ✅ {package}")
        except ImportError:
            print(f"   ❌ {package} (не установлен)")


def _check_environment_variables():
    """Check critical environment variables."""
    critical_vars = [
        "MONGO_SERVER",
        "MONGO_USER", 
        "MONGO_PASSWORD",
        "MONGO_SSL_CERT",
        "MONGO_METADATA_SERVER",
        "MONGO_METADATA_USER",
        "MONGO_METADATA_PASSWORD"
    ]
    
    print("🔐 Environment variables check:")
    missing_vars = []
    
    for var in critical_vars:
        value = os.environ.get(var)
        if value:
            # Show masked value for sensitive variables
            if "PASSWORD" in var or "SECRET" in var:
                print(f"   ✅ {var}: {'*' * len(value)}")
            else:
                print(f"   ✅ {var}: {value[:30]}{'...' if len(value) > 30 else ''}")
        else:
            print(f"   ❌ {var}: не установлена")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"   ⚠️  Отсутствуют критические переменные: {', '.join(missing_vars)}")


def _check_mongodb_connections():
    """Check MongoDB connections using health checks."""
    try:
        from docreciv.sync_db.health_checks import (
            check_remote_mongodb_connectivity,
            check_local_mongodb_connectivity,
            print_health_check_report
        )
        
        # Check remote MongoDB
        print("\n🔗 Проверка удаленной MongoDB для протоколов...")
        remote_result = check_remote_mongodb_connectivity()
        if remote_result.is_healthy():
            print(f"✅ {remote_result.name}: {remote_result.message}")
            if remote_result.details:
                print(f"   Сервер: {remote_result.details.get('server', 'N/A')}")
                print(f"   База данных: {remote_result.details.get('database', 'N/A')}")
        else:
            print(f"❌ {remote_result.name}: {remote_result.message}")
            if remote_result.details:
                print(f"   Детали: {remote_result.details}")

        # Check local MongoDB
        print("\n🔗 Проверка локальной MongoDB для метаданных...")
        local_result = check_local_mongodb_connectivity()
        if local_result.is_healthy():
            print(f"✅ {local_result.name}: {local_result.message}")
            if local_result.details:
                print(f"   Сервер: {local_result.details.get('server', 'N/A')}")
                print(f"   База данных: {local_result.details.get('database', 'N/A')}")
        else:
            print(f"❌ {local_result.name}: {local_result.message}")
            if local_result.details:
                print(f"   Детали: {local_result.details}")
            
    except ImportError:
        print("❌ MongoDB check: pymongo не установлен")
    except Exception as e:
        print(f"❌ MongoDB check error: {e}")


def handle_check_infrastructure(cli_instance):
    """Проверка инфраструктуры."""
    print("\n=== ПРОВЕРКА ИНФРАСТРУКТУРЫ ===")
    
    # Проверка директорий
    dirs_to_check = [
        ("INPUT_DIR", cli_instance.INPUT_DIR),
        ("TEMP_DIR", cli_instance.TEMP_DIR),
        ("OUTPUT_DIR", cli_instance.OUTPUT_DIR),
        ("EXTRACTED_DIR", cli_instance.EXTRACTED_DIR),
        ("NORMALIZED_DIR", cli_instance.NORMALIZED_DIR),
        ("ARCHIVE_DIR", cli_instance.ARCHIVE_DIR),
    ]

    print("📁 Проверка директорий:")
    for name, directory in dirs_to_check:
        if directory.exists():
            # Check if directory is writable
            try:
                test_file = directory / ".write_test"
                test_file.touch()
                test_file.unlink()
                print(f"  ✅ {name}: {directory} (доступна для записи)")
            except Exception:
                print(f"  ⚠️  {name}: {directory} (только чтение)")
        else:
            print(f"  ❌ {name}: не существует")
            try:
                directory.mkdir(parents=True, exist_ok=True)
                print(f"     📁 Создана: {directory}")
            except Exception as e:
                print(f"     ❌ Ошибка создания: {e}")

    # Проверка Docker
    print("\n🐳 Проверка Docker:")
    _check_docker_status()
    _check_running_containers()
    
    # Проверка VPN
    print("\n🔒 Проверка VPN:")
    _check_vpn_status()
    
    # Проверка диска
    print("\n💾 Проверка диска:")
    _check_disk_space()
    
    # Проверка Python пакетов
    print("\n🐍 Проверка Python пакетов:")
    _check_python_packages()
    
    # Проверка переменных окружения
    print("\n🔐 Проверка переменных окружения:")
    _check_environment_variables()
    
    # Проверка MongoDB
    print("\n" + "="*40)
    _check_mongodb_connections()

    print("\n🎯 Инфраструктура проверена")
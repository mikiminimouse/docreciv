# Руководство по CLI интерфейсу Preprocessing

## Обзор

Этот документ описывает интерфейс командной строки для компонентов receiver, включая синхронизацию, загрузку и проверку инфраструктуры.

## Основной CLI

### Запуск интерактивного режима
```bash
cd /root/winners_preprocessor/final_preprocessing/receiver
python run_cli.py
```

### Меню интерактивного режима
```
=== ПРЕПРОЦЕССИНГ ДОКУМЕНТОВ - CLI ТЕСТИРОВАНИЯ ===

=== ЗАГРУЗКА И ПОДГОТОВКА ДАННЫХ ===
1. Синхронизация протоколов из MongoDB
2. Скачивание протоколов за дату
3. Проверка доступности файлов в INPUT_DIR

=== СЛУЖЕБНЫЕ ФУНКЦИИ ===
25. Очистка тестовых данных
26. Создание тестовых файлов
27. Проверка инфраструктуры

0. Выход
```

## Компонент 1: Синхронизация (sync_db)

### Интерактивный режим
Пункт меню 1: "Синхронизация протоколов из MongoDB"

Опции синхронизации:
1. Одна дата (по умолчанию - вчерашний день)
2. Диапазон дат (начало - конец)
3. Начальная дата + количество дней
4. Ежедневное обновление (вчерашний день)
5. Полная синхронизация (последние 14 дней)
6. Проверка здоровья системы

### Командная строка
```bash
# Синхронизация за одну дату
python -m receiver.sync_db.enhanced_service sync-date --date 2025-03-19

# Синхронизация за диапазон дат
python -m receiver.sync_db.enhanced_service sync-range \
  --start-date 2025-03-01 --end-date 2025-03-19

# Ежедневное обновление
python -m receiver.sync_db.enhanced_service sync-daily

# Полная синхронизация за последние 30 дней
python -m receiver.sync_db.enhanced_service sync-full --days 30

# С лимитом
python -m receiver.sync_db.enhanced_service sync-date \
  --date 2025-03-19 --limit 100
```

### Результаты синхронизации
```
📊 SYNCHRONIZATION RESULTS
==============================

✅ SUCCESS!
📅 Period: 2025-03-19
🔍 Scanned: 1250
💾 Inserted: 1250
⏭️  Skipped (duplicates): 0
❌ Errors: 0
⏱️  Duration: 45.23 seconds

📈 Statistics:
   URL Distribution:
     Single URL: 890
     Multi URL: 360
     No URL: 0
   
   Attachment Types:
     url: 1250
     downloadUrl: 0
     fileUrl: 0
   
   Average Processing Time: 0.0341s
```

## Компонент 2: Загрузка (downloader)

### Интерактивный режим
Пункт меню 2: "Скачивание протоколов за дату"

Опции загрузки:
1. Все ожидающие протоколы
2. Протоколы за конкретную дату
3. Протоколы за диапазон дат

### Командная строка
```bash
# Загрузка всех ожидающих протоколов
python -m receiver.downloader.enhanced_service

# Загрузка с лимитом
python -m receiver.downloader.enhanced_service --limit 50

# Загрузка в пользовательскую директорию
python -m receiver.downloader.enhanced_service \
  --output-dir /custom/path

# С подробным логированием
python -m receiver.downloader.enhanced_service --verbose
```

### Результаты загрузки
```
📥 DOWNLOAD RESULTS
==============================

✅ SUCCESS!
   Processed protocols: 50
   Downloaded documents: 127
   Failed downloads: 3
   Duration: 127.45 seconds

📝 Errors:
   1. Timeout downloading https://zakupki.gov.ru/file1.pdf
   2. HTTP error 404 downloading https://zakupki.gov.ru/file2.doc
   3. Connection error for https://zakupki.gov.ru/file3.xls

📈 Statistics:
   File Sizes:
     Total: 256,789,123 bytes
     Average: 2,012,345 bytes
     Max: 15,678,901 bytes
   
   Download Times:
     Total: 125.34 seconds
     Average: 0.98 seconds
   
   File Types:
     .pdf: 89
     .doc: 23
     .docx: 15
```

## Компонент 3: Проверка инфраструктуры

### Интерактивный режим
Пункт меню 27: "Проверка инфраструктуры"

Проверки:
1. Директории (существование, права доступа)
2. Docker и контейнеры (статус)
3. VPN (подключение к zakupki.gov.ru)
4. Диск (свободное место)
5. Python пакеты (установленные)
6. Переменные среды (настройки)
7. MongoDB (подключения)

### Командная строка
```bash
# Проверка здоровья всей системы
python -m receiver.sync_db.health_checks --check all

# Проверка VPN
python -m receiver.sync_db.health_checks --check vpn

# Проверка удаленной MongoDB
python -m receiver.sync_db.health_checks --check remote-mongo

# Проверка локальной MongoDB
python -m receiver.sync_db.health_checks --check local-mongo

# С подробным выводом
python -m receiver.sync_db.health_checks --check all --verbose
```

### Результаты проверки
```
🏥 HEALTH CHECK REPORT
==============================

✅ VPN Connectivity
   Status: HEALTHY
   Message: Successfully connected to zakupki.gov.ru
   Details:
     response_time_ms: 156.23
     status_code: 200
     final_url: https://www.zakupki.gov.ru/

✅ Remote MongoDB
   Status: HEALTHY
   Message: Successfully connected to remote MongoDB
   Details:
     server: 192.168.0.46:8635
     database: protocols223
     collections: 45
     connection_time_ms: 234.56

✅ Local MongoDB
   Status: HEALTHY
   Message: Successfully connected to local MongoDB
   Details:
     server: localhost:27018
     database: docling_metadata
     collections: 12
     connection_time_ms: 12.34

✅ OVERALL SYSTEM HEALTH: HEALTHY
   All 4 checks passed
```

## Служебные функции

### Очистка тестовых данных (пункт 25)
```bash
# Очистка временных директорий
rm -rf /root/winners_preprocessor/data/temp/*
rm -rf /root/winners_preprocessor/data/extracted/*
rm -rf /root/winners_preprocessor/data/normalized/*
```

### Создание тестовых файлов (пункт 26)
Создает простой текстовый файл для тестирования pipeline.

### Проверка инфраструктуры (пункт 27)
Комплексная проверка всех компонентов системы.

## Переменные среды

### Обязательные переменные
```bash
# Удаленная MongoDB (для протоколов)
MONGO_SERVER=192.168.0.46:8635
MONGO_USER=readProtocols223
MONGO_PASSWORD=your_password
MONGO_SSL_CERT=/root/winners_preprocessor/final_preprocessing/receiver/certs/sber2.crt

# Локальная MongoDB (для метаданных)
MONGO_METADATA_SERVER=localhost:27018
LOCAL_MONGO_SERVER=localhost:27018
MONGO_METADATA_USER=admin
MONGO_METADATA_PASSWORD=your_password
MONGO_METADATA_DB=docling_metadata
```

### Опциональные переменные
```bash
# Директории
INPUT_DIR=/root/winners_preprocessor/final_preprocessing/Data
TEMP_DIR=/root/winners_preprocessor/data/temp
OUTPUT_DIR=/root/winners_preprocessor/data/output

# Лимиты
MAX_URLS_PER_PROTOCOL=15
DOWNLOAD_HTTP_TIMEOUT=120
DOWNLOAD_CONCURRENCY=20
PROTOCOLS_CONCURRENCY=20
```

## Устранение неполадок

### Проблемы с синхронизацией
1. **"Failed to connect to remote MongoDB"**
   - Проверьте VPN подключение
   - Проверьте SSL сертификат
   - Проверьте учетные данные

2. **"SSL certificate not found"**
   - Убедитесь, что файл `/root/winners_preprocessor/final_preprocessing/receiver/certs/sber2.crt` существует
   - Проверьте права доступа к файлу

### Проблемы с загрузкой
1. **"zakupki.gov.ru unavailable"**
   - Проверьте VPN подключение
   - Проверьте доступ к интернету

2. **"Failed to connect to MongoDB"**
   - Проверьте, что MongoDB запущена
   - Проверьте учетные данные

### Проблемы с инфраструктурой
1. **"Docker daemon not running"**
   - Запустите Docker: `systemctl start docker`
   - Проверьте права пользователя: `usermod -aG docker $USER`

2. **"Insufficient disk space"**
   - Очистите временные файлы
   - Проверьте использование диска: `df -h`

## Примеры использования

### Ежедневная обработка
```bash
# 1. Синхронизация вчерашних протоколов
python -m receiver.sync_db.enhanced_service sync-daily

# 2. Загрузка файлов
python -m receiver.downloader.enhanced_service

# 3. Проверка результатов
python -m receiver.sync_db.health_checks --check all
```

### Обработка за период
```bash
# 1. Синхронизация за неделю
python -m receiver.sync_db.enhanced_service sync-range \
  --start-date 2025-03-13 --end-date 2025-03-19

# 2. Загрузка файлов
python -m receiver.downloader.enhanced_service --limit 500

# 3. Проверка инфраструктуры
python run_cli.py  # Выбрать пункт 27
```

### Мониторинг системы
```bash
# Регулярная проверка здоровья
python -m receiver.sync_db.health_checks --check all

# Анализ трендов синхронизации
python -m receiver.sync_db.analytics trends --days 30

# Экспорт отчета
python -m receiver.sync_db.analytics export --days 30 --output monthly_report.json
```

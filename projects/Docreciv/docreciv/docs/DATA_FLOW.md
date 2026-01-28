# Поток данных в системе Receiver

## Обзор

Этот документ описывает поток данных в системе receiver, начиная от синхронизации протоколов из удаленной MongoDB до загрузки файлов в директории для дальнейшей обработки.

## Структура директорий данных

### Целевая структура в final_preprocessing
```
final_preprocessing/Data/
└── YYYY-MM-DD/              # Дата протокола
    ├── Input/               # Входные UNIT с файлами
    ├── Processing/          # Обработка по циклам (создается docprep)
    ├── Merge/               # Объединение файлов (создается docprep)
    ├── Exceptions/          # Исключения (создается docprep)
    └── Ready2Docling/      # Готовые к обработке в Docling (создается docprep)
```

## Этап 1: Синхронизация протоколов (sync_db)

### Источник данных
- **Удаленная MongoDB**: `protocols223.purchaseProtocol` через VPN
- **Адрес**: 192.168.0.46:8635
- **Аутентификация**: readProtocols223 / пароль

### Целевое хранилище
- **Локальная MongoDB**: `docling_metadata.protocols`
- **Структура документа**:
  ```javascript
  {
    // Служебные поля препроцессинга
    "unit_id": "UNIT_a1b2c3d4e5f6789a",
    "urls": [...],           // Извлеченные URLs из attachments
    "multi_url": false,      // true если >1 URL
    "url_count": 1,          // Количество URLs
    "source": "remote_mongo_direct",
    "status": "pending",     // Статус обработки
    "created_at": ISODate("..."),
    "updated_at": ISODate("..."),
    
    // ПОЛНЫЕ ДАННЫЕ ПРОТОКОЛА ИЗ MONGODB
    "guid": "96044532-5438-4fa7-97bd-16de4b30699e",
    "purchaseInfo": {
      "purchaseNoticeNumber": "32515525370",
      "name": "Название закупки",
      // ... все поля purchaseInfo
    },
    // ... ВСЕ остальные поля из оригинального документа
  }
  ```

### Процесс синхронизации
1. Подключение к удаленной MongoDB через VPN
2. Запрос протоколов за указанную дату
3. Извлечение URL из поля `attachments`
4. Генерация уникального `unit_id`
5. Проверка на дубликаты
6. Вставка в локальную MongoDB

## Этап 2: Загрузка файлов (downloader)

### Источник данных
- **Локальная MongoDB**: `docling_metadata.protocols`
- **Фильтр**: `status: "pending"` и `source: "remote_mongo_direct"`

### Целевая директория
- **Путь**: `final_preprocessing/Data/YYYY-MM-DD/Input/`
- **Структура**:
  ```
  final_preprocessing/Data/2025-03-19/Input/
  ├── UNIT_a1b2c3d4e5f6789a/
  │   ├── document_1.pdf
  │   └── document_2.docx
  └── UNIT_b2c3d4e5f6789a1b/
      └── protocol.pdf
  ```

### Процесс загрузки
1. Поиск протоколов со статусом "pending"
2. Создание директории UNIT в формате `final_preprocessing/Data/YYYY-MM-DD/Input/UNIT_xxx/`
3. Скачивание файлов по URL в директорию UNIT
4. Обновление статуса протокола на "downloaded"

## Интеграция с docprep

### Входные данные для docprep
- **Директория**: `final_preprocessing/Data/YYYY-MM-DD/Input/`
- **Структура**: UNIT директории с файлами

### Выходные данные от docprep
- **Директория**: `final_preprocessing/Data/YYYY-MM-DD/Ready2Docling/`
- **Структура**: Обработанные файлы готовые к Docling

## Конфигурация путей

### Переменные среды для директорий
```bash
# Базовые директории
INPUT_DIR=/root/winners_preprocessor/final_preprocessing/Data
OUTPUT_DIR=/root/winners_preprocessor/final_preprocessing/Data

# Конкретные пути формируются динамически:
# INPUT_DIR/YYYY-MM-DD/Input/ - для загрузки
# INPUT_DIR/YYYY-MM-DD/Ready2Docling/ - для результатов
```

## Пример полного цикла обработки

### День 1: Синхронизация
```bash
# Синхронизация протоколов за 2025-03-19
python -m receiver.sync_db.enhanced_service sync-date --date 2025-03-19

# Результат: Протоколы в локальной MongoDB с status="pending"
```

### День 2: Загрузка
```bash
# Загрузка файлов для протоколов 2025-03-19
python -m receiver.downloader.enhanced_service --limit 100

# Результат: 
# - Файлы в /root/winners_preprocessor/final_preprocessing/Data/2025-03-19/Input/
# - Протоколы в MongoDB со status="downloaded"
```

### День 3: Обработка docprep
```bash
# Обработка файлов docprep
cd /root/winners_preprocessor/final_preprocessing/docprep
python -m cli.main

# Результат:
# - Файлы в /root/winners_preprocessor/final_preprocessing/Data/2025-03-19/Ready2Docling/
```

## Мониторинг и отладка

### Проверка статусов
```bash
# Проверка протоколов в MongoDB
mongo docling_metadata
> db.protocols.count({"status": "pending"})    # Ожидают загрузки
> db.protocols.count({"status": "downloaded"}) # Загружены
> db.protocols.count({"status": "processed"})   # Обработаны docprep
```

### Логи
```bash
# Логи синхронизации
tail -f /var/log/receiver/sync.log

# Логи загрузки
tail -f /var/log/receiver/download.log
```

## Обработка ошибок

### Типичные ошибки синхронизации
1. **VPN недоступен**: Проверить подключение к 192.168.0.46:8635
2. **SSL сертификат**: Проверить путь к sber2.crt
3. **Аутентификация**: Проверить MONGO_USER/MONGO_PASSWORD

### Типичные ошибки загрузки
1. **zakupki.gov.ru недоступен**: Проверить VPN
2. **Ошибки URL**: Недоступные или некорректные ссылки
3. **Диск переполнен**: Проверить свободное место

## Резервное копирование

### Данные для резервирования
1. **Локальная MongoDB**: `docling_metadata` база данных
2. **Загруженные файлы**: `final_preprocessing/Data/YYYY-MM-DD/Input/`
3. **Конфигурационные файлы**: `.env`, `docker-compose.yml`

### Процедура резервирования
```bash
# Резервное копирование MongoDB
mongodump --db docling_metadata --out /backup/mongodb/

# Резервное копирование файлов
tar -czf /backup/files_$(date +%Y%m%d).tar.gz /root/winners_preprocessor/final_preprocessing/Data/
```

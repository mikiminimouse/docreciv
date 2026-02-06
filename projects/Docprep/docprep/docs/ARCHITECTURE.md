# Архитектура системы DocPrep

## Обзор

DocPrep - это CLI система для preprocessing документов перед обработкой в Docling pipeline. Система поддерживает 3 цикла итеративной обработки, state machine для управления состояниями UNIT, manifest v2 для хранения метаданных и полный audit log всех операций.

## Структура директорий

### Базовая структура

```
Data/
└── YYYY-MM-DD/              # Дата протокола
    ├── Input/               # Входные UNIT
    ├── Processing/          # Обработка по циклам
    │   ├── Processing_1/
    │   │   ├── Convert/     # Файлы для конвертации (doc, xls, ppt)
    │   │   │   ├── doc/
    │   │   │   ├── xls/
    │   │   │   └── ppt/
    │   │   ├── Extract/     # Архивы для распаковки
    │   │   │   ├── zip/
    │   │   │   ├── rar/
    │   │   │   └── 7z/
    │   │   └── Normalize/   # Файлы для нормализации
    │   │       ├── pdf/
    │   │       ├── docx/
    │   │       └── ...
    │   ├── Processing_2/    # Второй цикл
    │   └── Processing_3/     # Третий цикл
    ├── Merge/               # Объединение обработанных файлов
    │   ├── Merge_0/
    │   │   └── Direct/      # Прямые файлы (без обработки)
    │   ├── Merge_1/
    │   │   ├── Converted/   # Конвертированные файлы
    │   │   ├── Extracted/   # Извлеченные из архивов
    │   │   └── Normalized/  # Нормализованные файлы
    │   ├── Merge_2/
    │   └── Merge_3/
    ├── Exceptions/          # Исключения
    │   ├── Exceptions_1/
    │   │   ├── Empty/       # Только пустые UNIT
    │   │   ├── Special/     # Специальные файлы/подписи/приложения
    │   │   ├── Ambiguous/   # UNIT с нераспознанными форматами
    │   │   ├── ErConvert/   # UNIT с ошибками конвертации
    │   │   ├── ErNormalaze/ # UNIT с ошибками нормализации
    │   │   └── ErExtact/    # UNIT с ошибками извлечения
    │   ├── Exceptions_2/
    │   │   ├── Empty/
    │   │   ├── Special/
    │   │   ├── Ambiguous/
    │   │   ├── ErConvert/
    │   │   ├── ErNormalaze/
    │   │   └── ErExtact/
    │   └── Exceptions_3/
    │       ├── Empty/
    │       ├── Special/
    │       ├── Ambiguous/
    │       ├── ErConvert/
    │       ├── ErNormalaze/
    │       └── ErExtact/
    ├── ErMerge/             # Ошибки при финальном merge
    │   ├── Cycle_1/
    │   ├── Cycle_2/
    │   └── Cycle_3/
    └── Ready2Docling/       # Готовые к обработке в Docling
```

## Компоненты системы

### Core (Ядро системы)

#### 1. State Machine (`docprep/core/state_machine.py`)
- **Назначение**: Управление состояниями UNIT
- **Состояния**: RAW → CLASSIFIED_1 → PENDING_* → CLASSIFIED_2 → ... → READY_FOR_DOCLING
- **Особенности**: Детерминированные переходы, валидация, интеграция с manifest

#### 2. Manifest v2 (`docprep/core/manifest.py`)
- **Назначение**: Хранение метаданных UNIT
- **Структура**:
  - Schema version
  - Unit ID, Protocol ID, Protocol Date
  - File list с трансформациями
  - State machine history
  - Processing information
- **Формат**: JSON

#### 3. Audit Log (`docprep/core/audit.py`)
- **Назначение**: Полное логирование всех операций
- **Формат**: JSONL (JSON Lines)
- **Особенности**: Correlation ID для трекинга, сохранение в директории UNIT

#### 4. Config (`docprep/core/config.py`)
- **Назначение**: Конфигурация путей и параметров
- **Функции**: Инициализация структуры директорий, получение путей по циклам

#### 5. Unit Processor (`docprep/core/unit_processor.py`)
- **Назначение**: Обработка UNIT как атомарных единиц
- **Функции**:
  - Поиск UNIT директорий
  - Перемещение UNIT между директориями
  - Создание и обновление manifest
  - Определение расширений для сортировки

### Engine (Движки обработки)

#### 1. Classifier (`docprep/engine/classifier.py`)
- **Назначение**: Классификация файлов и определение категорий обработки
- **Категории**:
  - `direct`: Готовые к обработке (PDF, DOCX, XLSX, PPTX)
  - `convert`: Требуют конвертации (DOC → DOCX, XLS → XLSX, PPT → PPTX)
  - `extract`: Архивы для распаковки (ZIP, RAR, 7Z)
  - `normalize`: Требуют нормализации (исправление расширений, имен)
  - `special`: Специальные файлы (подписи, неподдерживаемые)
  - `mixed`: Смешанные типы файлов
  - `unknown`: Неопределенные/пустые UNIT
- **Особенности**:
  - Использует Decision Engine для определения типа файла
  - Автоматическое создание manifest
  - Перемещение UNIT в соответствующие директории
  - Сортировка по расширениям

#### 2. Converter (`docprep/engine/converter.py`)
- **Назначение**: Конвертация файлов через LibreOffice
- **Поддерживаемые конвертации**:
  - DOC → DOCX (исправлено: ранее ошибочно конвертировалось в XLSX)
  - XLS → XLSX
  - PPT → PPTX
  - RTF → DOCX
- **Особенности**: 
  - Перемещение в Merge_N/Converted/ после конвертации
  - Использование LIBREOFFICE_FORMAT_MAP для корректного маппинга форматов
  - Обновление manifest с информацией о трансформациях

#### 3. Extractor (`docprep/engine/extractor.py`)
- **Назначение**: Безопасная распаковка архивов
- **Поддерживаемые форматы**: ZIP, RAR, 7Z
- **Защита**: Лимиты на размер, количество файлов, глубину вложенности
- **Особенности**: Перемещение в Merge_N/Extracted/ после извлечения

#### 4. Normalizers (`docprep/engine/normalizers/`)
- **NameNormalizer**: Нормализация имен файлов
- **ExtensionNormalizer**: Нормализация расширений файлов
- **Особенности**: Перемещение в Merge_N/Normalized/ после нормализации

#### 5. Merger (`docprep/engine/merger.py`)
- **Назначение**: Объединение UNIT из разных циклов
- **Функции**: 
  - Сортировка по расширениям
  - Разделение PDF на scan/text/mixed (на основе needs_ocr из manifest)
  - Рекурсивный поиск файлов во вложенных директориях (после извлечения архивов)
  - Проверка валидности файлов (не пустые, прошли обработку)
  - Исключение mixed/special/ambiguous UNIT из Ready2Docling

#### 6. Validator (`docprep/engine/validator.py`)
- **Назначение**: Валидация UNIT структуры
- **Проверки**: Checksum (SHA256), структура manifest, state transitions

### Utils (Утилиты)

#### 1. File Operations (`docprep/utils/file_ops.py`)
- **Назначение**: Определение типов файлов
- **Подход**: Каскадная детекция
  - Magic bytes (python-magic)
  - Структурный парсинг (ZIP, Office)
  - Детальная проверка PDF
  - Decision Engine для разрешения конфликтов

#### 2. Paths (`docprep/utils/paths.py`)
- **Назначение**: Работа с путями и директориями
- **Функции**: 
  - Поиск UNIT
  - Рекурсивное получение всех файлов UNIT (включая вложенные директории после извлечения архивов)
  - Создание структуры директорий

#### 3. Statistics (`docprep/utils/statistics.py`)
- **Назначение**: Сбор и анализ статистики
- **Функции**:
  - Сбор статистики входных данных
  - Анализ результатов классификации
  - Вычисление процентных метрик
  - Генерация отчетов

### CLI (Командная строка)

#### Структура команд

```
docprep
├── pipeline          # Полный pipeline
├── cycle             # Управление циклами
├── stage             # Этапы обработки
├── substage          # Атомарные операции
├── classifier        # Классификация
├── merge             # Объединение
├── inspect           # Инспекция и отладка
├── utils             # Утилиты
└── stats             # Статистика и метрики
    ├── show          # Показать статистику
    ├── compare       # Сравнить циклы
    └── export        # Экспорт статистики
```

## Поток обработки

### Цикл 1

1. **Input** → **Classifier**
   - Классификация всех UNIT
   - Распределение по категориям
   - Direct → Merge_0/Direct/
   - Convert → Processing_1/Convert/
   - Extract → Processing_1/Extract/
   - Normalize → Processing_1/Normalize/
   - Exceptions → Exceptions/Exceptions_1/

2. **Processing_1** → **Converter/Extractor/Normalizer**
   - Конвертация DOC → DOCX
   - Распаковка архивов
   - Нормализация файлов
   - Результаты → Merge_1/

3. **Merge_1** → **Classifier (Cycle 2)**
   - Повторная классификация обработанных UNIT
   - Распределение по Processing_2/

### Цикл 2

1. **Processing_2** → **Converter/Extractor/Normalizer**
   - Обработка файлов из второго цикла
   - Результаты → Merge_2/

2. **Merge_2** → **Classifier (Cycle 3)**
   - Повторная классификация
   - Распределение по Processing_3/

### Цикл 3

1. **Processing_3** → **Converter/Extractor/Normalizer**
   - Финальная обработка
   - Результаты → Merge_3/

2. **Merge_3** → **Merger**
   - Объединение всех UNIT
   - Сортировка по расширениям
   - Готовые UNIT → Ready2Docling/

## Decision Engine

### Источники правды

1. **MIME Type** (python-magic)
2. **File Signature** (magic bytes)
3. **File Extension**

### Сценарии разрешения

- **2.1**: Все три совпали → `direct`
- **2.2**: MIME+Signature ≠ Extension → `normalize`
- **2.3**: Signature+Extension ≠ MIME → `normalize`
- **2.4**: MIME+Extension ≠ Signature → `ambiguous`
- **2.5**: Все три разные → `ambiguous`
- **2.6**: MIME = octet-stream → `ambiguous`
- **2.7**: Signature отсутствует → `ambiguous`

## State Machine

### Состояния

- `RAW`: Начальное состояние
- `CLASSIFIED_1/2/3`: Классифицирован в цикле N
- `PENDING_CONVERT/EXTRACT/NORMALIZE`: Ожидает обработки
- `MERGED_DIRECT`: Объединен как direct
- `MERGED_PROCESSED`: Объединен после обработки
- `READY_FOR_DOCLING`: Готов к обработке в Docling

### Переходы

Детерминированные переходы определены в `ALLOWED_TRANSITIONS`. Все переходы валидируются перед выполнением.

## Статистика и метрики

### Сбор статистики

Система собирает детальную статистику:
- По входным данным (Input)
- По выходным данным (Output)
- По категориям обработки
- По расширениям файлов
- По route (PDF scan/text, mixed, etc.)
- По циклам обработки

### Процентные метрики

- Процент файлов по расширениям
- Процент UNIT по расширениям
- Процент по категориям (с учетом и без пустых UNIT)
- Процент PDF по route (scan/text)
- Процент по циклам обработки

### CLI команды статистики

```bash
# Показать статистику
docprep stats show 2025-12-20

# Детальная статистика
docprep stats show 2025-12-20 --detailed

# Сравнить циклы
docprep stats compare 2025-12-20 --cycle1 1 --cycle2 2

# Экспорт в файл
docprep stats export 2025-12-20 --format markdown --output report.md
```

## Безопасность

### Защита от zip bomb

- Максимальный размер распаковки: 500 MB
- Максимальное количество файлов: 1000
- Максимальная глубина вложенности: 10

### Валидация

- Проверка целостности архивов
- Валидация state transitions
- Проверка checksum (SHA256)

## Расширяемость

Система спроектирована для легкого расширения:
- Добавление новых форматов конвертации
- Добавление новых типов архивов
- Добавление новых нормализаторов
- Кастомные обработчики ошибок


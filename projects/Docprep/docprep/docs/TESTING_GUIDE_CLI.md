# Руководство по тестированию через CLI

## Обзор

Это руководство описывает, как тестировать функциональность DocPrep через CLI команды.

## Подготовка

### 1. Инициализация структуры директорий

Перед началом тестирования необходимо инициализировать структуру директорий для конкретной даты:

```bash
# Инициализация структуры для даты 2025-12-20
docprep utils init-date 2025-12-20
```

Эта команда создаст структуру:
```
Data/2025-12-20/
├── Input/
├── Processing/
│   ├── Processing_1/
│   ├── Processing_2/
│   └── Processing_3/
├── Merge/
│   ├── Merge_0/
│   ├── Merge_1/
│   ├── Merge_2/
│   └── Merge_3/
├── Exceptions/
│   ├── Exceptions_1/
│   ├── Exceptions_2/
│   └── Exceptions_3/
└── Ready2Docling/
```

### 2. Загрузка UNIT в Input

Поместите UNIT директории в `Data/2025-12-20/Input/`:

```bash
# Пример структуры
Data/2025-12-20/Input/
├── UNIT_001/
│   └── document.pdf
├── UNIT_002/
│   └── document.docx
└── UNIT_003/
    └── archive.zip
```

## Тестирование первой итерации (Цикл 1)

### 1. Классификация UNIT

Запустите классификацию всех UNIT из Input:

```bash
# Классификация UNIT из Input
docprep classifier run \
  --input Data/2025-12-20/Input \
  --cycle 1 \
  --date 2025-12-20 \
  --verbose
```

**Что происходит:**
- Все UNIT из Input классифицируются
- UNIT распределяются по категориям:
  - `direct` → `Merge/Merge_0/Direct/`
  - `convert` → `Processing/Processing_1/Convert/`
  - `extract` → `Processing/Processing_1/Extract/`
  - `normalize` → `Processing/Processing_1/Normalize/`
  - `mixed` → `Exceptions/Exceptions_1/Mixed/`
  - `unknown` → `Exceptions/Exceptions_1/Ambiguous/`

**Проверка результатов:**
```bash
# Просмотр структуры после классификации
docprep inspect tree Data/2025-12-20

# Просмотр UNIT в конкретной категории
docprep inspect units Data/2025-12-20/Processing/Processing_1/Convert
```

### 2. Просмотр статистики

После классификации можно посмотреть статистику:

```bash
# Базовая статистика
docprep stats show 2025-12-20

# Детальная статистика
docprep stats show 2025-12-20 --detailed

# Сохранить в файл
docprep stats show 2025-12-20 --output stats_report.txt
```

## Тестирование второй итерации (Цикл 2)

### 1. Обработка через Converter

Обработайте UNIT из `Processing/Processing_1/Convert/`:

```bash
# Конвертация всех UNIT в Convert
docprep substage convert run \
  --input Data/2025-12-20/Processing/Processing_1/Convert \
  --cycle 1 \
  --date 2025-12-20 \
  --verbose
```

**Что происходит:**
- Все `.doc` файлы конвертируются в `.docx`
- Все `.xls` файлы конвертируются в `.xlsx`
- Все `.ppt` файлы конвертируются в `.pptx`
- Все `.rtf` файлы конвертируются в `.docx`
- UNIT перемещаются в `Merge/Merge_1/Converted/`

**Требования:**
- Должен быть установлен LibreOffice:
  ```bash
  sudo apt-get update
  sudo apt-get install libreoffice
  ```

**Проверка результатов:**
```bash
# Просмотр конвертированных UNIT
docprep inspect units Data/2025-12-20/Merge/Merge_1/Converted

# Просмотр manifest конкретного UNIT
docprep inspect manifest UNIT_XXX --directory Data/2025-12-20/Merge/Merge_1/Converted
```

### 2. Обработка через Extractor

Обработайте UNIT из `Processing/Processing_1/Extract/`:

```bash
# Извлечение всех архивов
docprep substage extract run \
  --input Data/2025-12-20/Processing/Processing_1/Extract \
  --cycle 1 \
  --date 2025-12-20 \
  --verbose
```

**Что происходит:**
- ZIP архивы распаковываются
- RAR архивы распаковываются (требуется `rarfile` библиотека)
- 7Z архивы распаковываются (требуется `py7zr` библиотека)
- UNIT перемещаются в `Merge/Merge_1/Extracted/`

**Требования:**
- Для RAR: `pip install rarfile`
- Для 7Z: `pip install py7zr`

**Проверка результатов:**
```bash
# Просмотр извлеченных UNIT
docprep inspect units Data/2025-12-20/Merge/Merge_1/Extracted
```

### 3. Обработка через Normalizers

Обработайте UNIT из `Processing/Processing_1/Normalize/`:

```bash
# Нормализация имен файлов
docprep substage normalize name \
  --input Data/2025-12-20/Processing/Processing_1/Normalize \
  --cycle 1 \
  --date 2025-12-20 \
  --verbose

# Нормализация расширений файлов
docprep substage normalize extension \
  --input Data/2025-12-20/Processing/Processing_1/Normalize \
  --cycle 1 \
  --date 2025-12-20 \
  --verbose
```

**Что происходит:**
- Имена файлов нормализуются (исправляются смещенные точки, двойные расширения)
- Расширения файлов исправляются на основе magic bytes
- UNIT перемещаются в `Merge/Merge_1/Normalized/`

**Проверка результатов:**
```bash
# Просмотр нормализованных UNIT
docprep inspect units Data/2025-12-20/Merge/Merge_1/Normalized
```

### 4. Повторная классификация (Цикл 2)

После обработки запустите повторную классификацию UNIT из Merge_1:

```bash
# Классификация UNIT из Merge_1
docprep classifier run \
  --input Data/2025-12-20/Merge/Merge_1 \
  --cycle 2 \
  --date 2025-12-20 \
  --verbose
```

**Что происходит:**
- UNIT из `Merge/Merge_1/Converted/` классифицируются
- UNIT из `Merge/Merge_1/Extracted/` классифицируются
- UNIT из `Merge/Merge_1/Normalized/` классифицируются
- UNIT распределяются по:
  - `direct` → `Merge/Merge_2/Direct/` (если готовы)
  - `convert` → `Processing/Processing_2/Convert/` (если требуют дальнейшей обработки)
  - `extract` → `Processing/Processing_2/Extract/`
  - `normalize` → `Processing/Processing_2/Normalize/`
  - `exceptions` → `Exceptions/Exceptions_2/`

**Проверка результатов:**
```bash
# Просмотр структуры после второй классификации
docprep inspect tree Data/2025-12-20

# Статистика после второй итерации
docprep stats show 2025-12-20 --detailed
```

## Тестирование третьей итерации (Цикл 3)

### 1. Обработка через Converter/Extractor/Normalizers (Цикл 2)

Повторите шаги из второй итерации для `Processing/Processing_2/`:

```bash
# Конвертация
docprep substage convert run \
  --input Data/2025-12-20/Processing/Processing_2/Convert \
  --cycle 2 \
  --date 2025-12-20

# Извлечение
docprep substage extract run \
  --input Data/2025-12-20/Processing/Processing_2/Extract \
  --cycle 2 \
  --date 2025-12-20

# Нормализация
docprep substage normalize name \
  --input Data/2025-12-20/Processing/Processing_2/Normalize \
  --cycle 2 \
  --date 2025-12-20

docprep substage normalize extension \
  --input Data/2025-12-20/Processing/Processing_2/Normalize \
  --cycle 2 \
  --date 2025-12-20
```

### 2. Повторная классификация (Цикл 3)

```bash
# Классификация UNIT из Merge_2
docprep classifier run \
  --input Data/2025-12-20/Merge/Merge_2 \
  --cycle 3 \
  --date 2025-12-20 \
  --verbose
```

## Полный pipeline

Для автоматического выполнения всех итераций можно использовать полный pipeline:

```bash
# Полный pipeline (3 цикла)
docprep pipeline run \
  Data/2025-12-20/Input \
  Data/2025-12-20/Ready2Docling \
  --date 2025-12-20 \
  --max-cycles 3 \
  --verbose
```

**Что происходит:**
1. Цикл 1: Классификация → Обработка → Повторная классификация
2. Цикл 2: Обработка → Повторная классификация
3. Цикл 3: Обработка → Повторная классификация → Merge → Ready2Docling

## Просмотр статистики и метрик

### Базовая статистика

```bash
# Показать статистику
docprep stats show 2025-12-20
```

**Вывод включает:**
- Общее количество UNIT
- Количество пустых UNIT
- Количество UNIT с файлами
- Распределение по категориям (без пустых UNIT)
- Статистика по расширениям с процентами
- Детальная статистика PDF с разбивкой по route

### Детальная статистика

```bash
# Детальная статистика
docprep stats show 2025-12-20 --detailed
```

**Дополнительно показывает:**
- Расширения по категориям (вход)
- Распределение по локациям
- Детальная информация по каждому UNIT

### Сравнение циклов

```bash
# Сравнить циклы 1 и 2
docprep stats compare 2025-12-20 --cycle1 1 --cycle2 2
```

**Показывает:**
- Количество UNIT в каждом цикле
- Изменения в распределении
- Разницу между циклами

### Экспорт статистики

```bash
# Экспорт в Markdown
docprep stats export 2025-12-20 --format markdown --output report.md

# Экспорт в JSON
docprep stats export 2025-12-20 --format json --output report.json
```

## Инспекция и отладка

### Просмотр структуры директорий

```bash
# Дерево структуры
docprep inspect tree Data/2025-12-20

# Дерево конкретной директории
docprep inspect tree Data/2025-12-20/Processing/Processing_1
```

### Просмотр UNIT

```bash
# Список UNIT в директории
docprep inspect units Data/2025-12-20/Processing/Processing_1/Convert

# Список UNIT с деталями
docprep inspect units Data/2025-12-20/Processing/Processing_1/Convert --detailed
```

### Просмотр manifest

```bash
# Manifest конкретного UNIT
docprep inspect manifest UNIT_XXX --directory Data/2025-12-20/Processing/Processing_1/Convert

# Manifest с полной информацией
docprep inspect manifest UNIT_XXX \
  --directory Data/2025-12-20/Processing/Processing_1/Convert \
  --full
```

## Dry-run режим

Для проверки без выполнения операций используйте `--dry-run`:

```bash
# Проверка классификации без выполнения
docprep classifier run \
  --input Data/2025-12-20/Input \
  --cycle 1 \
  --date 2025-12-20 \
  --dry-run \
  --verbose

# Проверка конвертации без выполнения
docprep substage convert run \
  --input Data/2025-12-20/Processing/Processing_1/Convert \
  --cycle 1 \
  --date 2025-12-20 \
  --dry-run \
  --verbose
```

## Типичные проблемы и решения

### 1. LibreOffice не найден

**Проблема:** `[Errno 2] No such file or directory: 'libreoffice'`

**Решение:**
```bash
sudo apt-get update
sudo apt-get install libreoffice
```

### 2. RAR extraction requires rarfile library

**Проблема:** Ошибка при извлечении RAR архивов

**Решение:**
```bash
pip install rarfile
```

### 3. 7z extraction requires py7zr library

**Проблема:** Ошибка при извлечении 7Z архивов

**Решение:**
```bash
pip install py7zr
```

### 4. Invalid state transition

**Проблема:** `Invalid transition from UnitState.CLASSIFIED_1 to UnitState.MERGED_PROCESSED`

**Решение:** Это исправлено в последней версии. Убедитесь, что используете актуальную версию кода.

### 5. Manifest not found

**Проблема:** `Manifest not found for unit UNIT_XXX`

**Решение:** Manifest создается автоматически при классификации. Убедитесь, что UNIT был классифицирован.

## Пример полного тестирования

```bash
# 1. Инициализация
docprep utils init-date 2025-12-20

# 2. Классификация (Цикл 1)
docprep classifier run \
  --input Data/2025-12-20/Input \
  --cycle 1 \
  --date 2025-12-20 \
  --verbose

# 3. Статистика после классификации
docprep stats show 2025-12-20 --output stats_cycle1.md

# 4. Конвертация
docprep substage convert run \
  --input Data/2025-12-20/Processing/Processing_1/Convert \
  --cycle 1 \
  --date 2025-12-20

# 5. Извлечение
docprep substage extract run \
  --input Data/2025-12-20/Processing/Processing_1/Extract \
  --cycle 1 \
  --date 2025-12-20

# 6. Нормализация
docprep substage normalize name \
  --input Data/2025-12-20/Processing/Processing_1/Normalize \
  --cycle 1 \
  --date 2025-12-20

docprep substage normalize extension \
  --input Data/2025-12-20/Processing/Processing_1/Normalize \
  --cycle 1 \
  --date 2025-12-20

# 7. Повторная классификация (Цикл 2)
docprep classifier run \
  --input Data/2025-12-20/Merge/Merge_1 \
  --cycle 2 \
  --date 2025-12-20 \
  --verbose

# 8. Статистика после второй итерации
docprep stats show 2025-12-20 --detailed --output stats_cycle2.md

# 9. Сравнение циклов
docprep stats compare 2025-12-20 --cycle1 1 --cycle2 2

# 10. Экспорт финальной статистики
docprep stats export 2025-12-20 --format markdown --output final_report.md
```

## Заключение

Это руководство описывает основные команды для тестирования функциональности DocPrep. Для получения дополнительной информации используйте:

```bash
# Справка по команде
docprep --help

# Справка по конкретной команде
docprep stats --help
docprep classifier --help
docprep substage --help
```


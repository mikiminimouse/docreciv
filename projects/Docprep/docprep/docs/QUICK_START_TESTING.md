# Быстрый старт тестирования

## Подготовка

### 1. Установка зависимостей

```bash
# LibreOffice для конвертации
sudo apt-get update
sudo apt-get install libreoffice

# Python библиотеки для архивов
pip install rarfile py7zr
```

### 2. Инициализация структуры

```bash
cd /root/winners_preprocessor/final_preprocessing

# Инициализация структуры для даты
docprep utils init-date 2025-12-20
```

## Тестирование первой итерации

### 1. Классификация UNIT

```bash
# Классификация всех UNIT из Input
docprep classifier run \
  --input Data/2025-12-20/Input \
  --cycle 1 \
  --date 2025-12-20 \
  --verbose
```

**Результат:**
- UNIT распределены по категориям
- Direct → `Merge/Merge_0/Direct/`
- Convert → `Processing/Processing_1/Convert/`
- Extract → `Processing/Processing_1/Extract/`
- Normalize → `Processing/Processing_1/Normalize/`
- Exceptions → `Exceptions/Exceptions_1/`

### 2. Просмотр статистики

```bash
# Статистика после классификации
docprep stats show 2025-12-20
```

## Тестирование второй итерации

### 1. Конвертация

```bash
# Конвертация UNIT из Convert
docprep substage convert run \
  --input Data/2025-12-20/Processing/Processing_1/Convert \
  --cycle 1 \
  --date 2025-12-20 \
  --verbose
```

**Результат:**
- DOC → DOCX
- XLS → XLSX
- PPT → PPTX
- RTF → DOCX
- UNIT → `Merge/Merge_1/Converted/`

### 2. Извлечение архивов

```bash
# Извлечение UNIT из Extract
docprep substage extract run \
  --input Data/2025-12-20/Processing/Processing_1/Extract \
  --cycle 1 \
  --date 2025-12-20 \
  --verbose
```

**Результат:**
- ZIP архивы распакованы
- RAR архивы распакованы
- 7Z архивы распакованы
- UNIT → `Merge/Merge_1/Extracted/`

### 3. Нормализация

```bash
# Нормализация имен
docprep substage normalize name \
  --input Data/2025-12-20/Processing/Processing_1/Normalize \
  --cycle 1 \
  --date 2025-12-20 \
  --verbose

# Нормализация расширений
docprep substage normalize extension \
  --input Data/2025-12-20/Processing/Processing_1/Normalize \
  --cycle 1 \
  --date 2025-12-20 \
  --verbose
```

**Результат:**
- Имена файлов нормализованы
- Расширения исправлены
- UNIT → `Merge/Merge_1/Normalized/`

### 4. Повторная классификация

```bash
# Классификация UNIT из Merge_1
docprep classifier run \
  --input Data/2025-12-20/Merge/Merge_1 \
  --cycle 2 \
  --date 2025-12-20 \
  --verbose
```

**Результат:**
- UNIT классифицированы
- Распределены по `Processing/Processing_2/`, `Merge/Merge_2/`, `Exceptions/Exceptions_2/`

### 5. Статистика после второй итерации

```bash
# Детальная статистика
docprep stats show 2025-12-20 --detailed

# Сравнение циклов
docprep stats compare 2025-12-20 --cycle1 1 --cycle2 2

# Экспорт отчета
docprep stats export 2025-12-20 --format markdown --output report.md
```

## Проверка результатов

### Просмотр структуры

```bash
# Дерево структуры
docprep inspect tree Data/2025-12-20

# UNIT в конкретной категории
docprep inspect units Data/2025-12-20/Processing/Processing_2/Convert
```

### Просмотр manifest

```bash
# Manifest конкретного UNIT
docprep inspect manifest UNIT_XXX \
  --directory Data/2025-12-20/Merge/Merge_1/Converted
```

## Полный pipeline (автоматический)

Для автоматического выполнения всех итераций:

```bash
# Полный pipeline (3 цикла)
docprep pipeline run \
  Data/2025-12-20/Input \
  Data/2025-12-20/Ready2Docling \
  --date 2025-12-20 \
  --max-cycles 3 \
  --verbose
```

## Troubleshooting

### LibreOffice не найден

```bash
sudo apt-get install libreoffice
```

### RAR extraction requires rarfile library

```bash
pip install rarfile
```

### 7z extraction requires py7zr library

```bash
pip install py7zr
```

### Invalid state transition

Убедитесь, что используете актуальную версию кода. Ошибки переходов состояний исправлены.

## Дополнительная информация

- **Полное руководство:** `docs/TESTING_GUIDE_CLI.md`
- **Архитектура:** `docs/ARCHITECTURE.md`
- **Статистика:** `docs/STATISTICS_GUIDE.md`


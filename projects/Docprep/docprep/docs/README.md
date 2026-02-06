# Документация системы Preprocessing

## Обзор

Система `docprep` - это CLI инструмент для preprocessing документов с поддержкой 3 циклов итеративной обработки, state machine, manifest v2 и audit log.

## Структура документации

### Основные документы

1. **`ARCHITECTURE.md`** - Архитектура системы (НОВОЕ)
   - Структура директорий
   - Компоненты системы (Core, Engine, Utils, CLI)
   - Поток обработки
   - Decision Engine
   - State Machine
   - Статистика и метрики

2. **`CLASSIFICATION_AND_STATISTICS.md`** - Классификация и статистика (НОВОЕ)
   - Система классификации
   - Категории обработки
   - Decision Engine и сценарии
   - Система статистики
   - CLI команды статистики
   - Метрики производительности

3. **`STATISTICS_GUIDE.md`** - Руководство по статистике (НОВОЕ)
   - CLI команды статистики
   - Формат отчета
   - Метрики и их интерпретация
   - Примеры использования
   - Технические детали

4. **`TESTING_GUIDE_CLI.md`** - Руководство по тестированию через CLI (НОВОЕ)
   - Пошаговое тестирование всех компонентов через CLI
   - Команды для каждой итерации
   - Просмотр статистики и метрик
   - Инспекция и отладка
   - Типичные проблемы и решения

5. **`QUICK_START_TESTING.md`** - Быстрый старт тестирования (НОВОЕ)
   - Краткие инструкции для быстрого начала
   - Основные команды
   - Troubleshooting

6. **`SUMMARY.md`** - Сводка проделанной работы
   - Выполненные задачи
   - Ключевые достижения
   - Статистика проекта и тестирования
   - Выполненные исправления

5. **`TESTING_GUIDE.md`** - Руководство по тестированию
   - Пошаговое тестирование всех компонентов
   - Отладка и диагностика
   - Типичные проблемы и решения

6. **`DETECTION_APPROACH.md`** - Подход к детекции файлов
   - Каскадный подход детекции
   - Архитектура детекции
   - Библиотеки и реализация

### Планы и спецификации

9. **`DEVELOPMENT_LOG.md`** - Лог разработки и исправлений (НОВОЕ)
   - Все исправления и улучшения
   - Детальное описание проблем и решений
   - История разработки

10. **`Final_plan/PlantoMake.md`** - Исходный план реализации CLI
11. **`Final_plan/plan.md`** - Детальный план реализации

## Быстрый старт

### Установка

```bash
cd /root/winners_preprocessor/final_preprocessing

# Установка зависимостей (если нужно)
pip install -r requirements.txt

# Проверка установки
python3 -m docprep.cli.main --help
```

### Первый запуск

```bash
# 1. Инициализация структуры директорий
python3 -m docprep.cli.main utils init-date 2025-03-18

# 2. Классификация UNIT
python3 -m docprep.cli.main stage classifier \
  --cycle 1 \
  --input Data/Input/2025-03-18 \
  --date 2025-03-18 \
  --verbose

# 3. Полный pipeline
python3 -m docprep.cli.main pipeline run \
  Data/Input/2025-03-18 \
  Data/Ready2Docling/2025-03-18 \
  --max-cycles 3 \
  --verbose
```

## CLI команды

### Основные команды

```bash
# Полный pipeline (3 цикла)
docprep pipeline run <input> <output> [--max-cycles 3]

# Управление циклом
docprep cycle run <1|2|3> [--input DIR] [--date YYYY-MM-DD]

# Этапы внутри цикла
docprep stage classifier --cycle 1 --input DIR
docprep stage pending --cycle 1 --pending DIR
docprep stage merge --cycle 1 --source DIR --target-base DIR

# Атомарные операции
docprep substage convert run --input DIR --cycle 1
docprep substage extract run --input DIR --cycle 1
docprep substage normalize name --input DIR --cycle 1
docprep substage normalize extension --input DIR --cycle 1

# Утилиты
docprep utils init-date 2025-03-18
docprep utils stats DIR
docprep inspect tree DIR
docprep inspect units DIR
docprep inspect manifest UNIT_ID --directory DIR
```

### Команды статистики (НОВОЕ)

```bash
# Показать статистику
docprep stats show 2025-12-20
docprep stats show 2025-12-20 --detailed
docprep stats show 2025-12-20 --output report.txt

# Сравнить циклы
docprep stats compare 2025-12-20 --cycle1 1 --cycle2 2

# Экспорт статистики
docprep stats export 2025-12-20 --format markdown --output report.md
docprep stats export 2025-12-20 --format json --output report.json
```

Полный список команд: `docprep --help`

## Архитектура

### Компоненты

- **Core** - Ядро системы (state machine, manifest, audit, config)
- **Engine** - Execution engine (classifier, converter, extractor, normalizers, merger, validator)
- **CLI** - Командная строка (Typer)
- **Utils** - Утилиты (file operations, paths)
- **Adapters** - Адаптеры для внешних систем (Docling)

### Поток обработки

```
Input/YYYY-MM-DD/
  ↓ [Classifier]
Processing/YYYY-MM-DD/Pending_1/{convert,archives,direct,normalize}/
  ↓ [Converter/Extractor/Normalizer]
Processing/YYYY-MM-DD/Pending_2/direct/
  ↓ [Merge]
Merge/YYYY-MM-DD/Merge_1/direct/
  ↓ [Merger]
Ready2Docling/YYYY-MM-DD/{ext}/UNIT_XXX/
```

## Тестирование

Подробное руководство: см. `docs/TESTING_GUIDE.md`

## Дополнительная информация

- **Архитектура:** `docs/ARCHITECTURE.md`
- **Классификация и статистика:** `docs/CLASSIFICATION_AND_STATISTICS.md`
- **Руководство по статистике:** `docs/STATISTICS_GUIDE.md`
- **Руководство по тестированию CLI:** `docs/TESTING_GUIDE_CLI.md`
- **Быстрый старт тестирования:** `docs/QUICK_START_TESTING.md`
- **Сводка работы:** `docs/SUMMARY.md`
- **Лог разработки:** `docs/DEVELOPMENT_LOG.md`
- **Детекция файлов:** `docs/DETECTION_APPROACH.md`
- **Руководство по тестированию:** `docs/TESTING_GUIDE.md`
- **План реализации:** `docs/Final_plan/PlantoMake.md`

## Последние обновления (2025-12-20)

- ✅ Исправлены ошибки классификации (unknown, ambiguous, mixed)
- ✅ Исправлена сортировка по расширениям
- ✅ Исправлена конвертация .doc файлов (doc → docx)
- ✅ Исправлено распределение PDF в Ready2Docling (scan/text/mixed)
- ✅ Реализован рекурсивный поиск файлов во вложенных директориях
- ✅ Улучшена логика определения типов файлов по расширениям
- ✅ Добавлена система статистики и метрик
- ✅ Протестировано на 2585 UNIT (2025-12-20 и 2025-03-19)
- ✅ Создана полная документация с memory bank структурой


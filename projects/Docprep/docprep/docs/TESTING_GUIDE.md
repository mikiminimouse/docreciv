# Руководство по тестированию системы Preprocessing

**Дата:** 2025-01-XX  
**Версия системы:** 1.0.0  
**Статус:** Готова к тестированию

## Обзор

Это пошаговое руководство для тестирования системы `docprep` на реальных данных из `Data/Input/2025-03-18/`.

## Предварительные требования

### 1. Проверка установки зависимостей

```bash
cd /root/winners_preprocessor/final_preprocessing

# Проверка Python пакетов
python3 -c "import typer, pydantic, magic, pypdf, jsonschema; print('✅ Все зависимости установлены')"

# Проверка системных утилит
which libreoffice || echo "⚠️  LibreOffice не установлен (нужен для конвертации)"
```

### 2. Проверка структуры данных

```bash
# Проверка наличия UNIT в Input
ls -d Data/Input/2025-03-18/UNIT_* | wc -l

# Проверка структуры одного UNIT
ls -la Data/Input/2025-03-18/UNIT_*/ | head -20
```

### 3. Инициализация структуры директорий

```bash
# Создание полной структуры директорий для даты 2025-03-18
python3 -m docprep.cli.main utils init-date 2025-03-18 --verbose

# Проверка созданной структуры
tree -L 3 Data/Processing/2025-03-18/ | head -30
tree -L 3 Data/Merge/2025-03-18/ | head -20
tree -L 3 Data/Exceptions/2025-03-18/ | head -20
tree -L 3 Data/Ready2Docling/2025-03-18/ | head -20
```

## Пошаговое тестирование

### Этап 1: Тестирование классификации (Цикл 1)

#### 1.1 Классификация одного UNIT (dry-run)

```bash
# Тест классификации в режиме dry-run
python3 -m docprep.cli.main stage classifier \
  --cycle 1 \
  --input Data/Input/2025-03-18 \
  --date 2025-03-18 \
  --dry-run \
  --verbose

# Проверка результатов (должны быть только логи, без перемещений)
ls Data/Processing/2025-03-18/Pending_1/ 2>/dev/null || echo "✅ Dry-run работает корректно"
```

#### 1.2 Классификация небольшой выборки UNIT

```bash
# Создаем тестовую директорию с несколькими UNIT
mkdir -p Data/Input/test_sample
cp -r Data/Input/2025-03-18/UNIT_* Data/Input/test_sample/ 2>/dev/null | head -5

# Классификация тестовой выборки
python3 -m docprep.cli.main stage classifier \
  --cycle 1 \
  --input Data/Input/test_sample \
  --date 2025-03-18 \
  --verbose

# Проверка результатов
echo "=== Проверка классификации ==="
find Data/Processing/2025-03-18/Pending_1 -type d -name "UNIT_*" | wc -l
find Data/Processing/2025-03-18/Pending_1 -type d -name "UNIT_*" | head -5

# Проверка manifest
find Data/Processing/2025-03-18/Pending_1 -name "manifest.json" | head -1 | xargs cat | jq '.state_machine.current_state'
```

#### 1.3 Полная классификация всех UNIT

```bash
# Классификация всех UNIT из Input
python3 -m docprep.cli.main stage classifier \
  --cycle 1 \
  --input Data/Input/2025-03-18 \
  --date 2025-03-18 \
  --verbose

# Статистика после классификации
python3 -m docprep.cli.main utils stats Data/Processing/2025-03-18/Pending_1 --verbose

# Проверка распределения по категориям
echo "=== Распределение по категориям ==="
find Data/Processing/2025-03-18/Pending_1/convert -type d -name "UNIT_*" 2>/dev/null | wc -l && echo "convert"
find Data/Processing/2025-03-18/Pending_1/direct -type d -name "UNIT_*" 2>/dev/null | wc -l && echo "direct"
find Data/Processing/2025-03-18/Pending_1/archives -type d -name "UNIT_*" 2>/dev/null | wc -l && echo "archives"
find Data/Processing/2025-03-18/Pending_1/normalize -type d -name "UNIT_*" 2>/dev/null | wc -l && echo "normalize"
find Data/Exceptions/2025-03-18/Exceptions_1 -type d -name "UNIT_*" 2>/dev/null | wc -l && echo "exceptions"
```

### Этап 2: Тестирование обработки Pending (Цикл 1)

#### 2.1 Обработка конвертации

```bash
# Тест конвертации (dry-run)
python3 -m docprep.cli.main substage convert run \
  --input Data/Processing/2025-03-18/Pending_1/convert \
  --cycle 1 \
  --date 2025-03-18 \
  --dry-run \
  --verbose

# Реальная конвертация (начните с одного UNIT)
python3 -m docprep.cli.main substage convert run \
  --input Data/Processing/2025-03-18/Pending_1/convert/doc \
  --cycle 1 \
  --date 2025-03-18 \
  --verbose

# Проверка результатов
find Data/Processing/2025-03-18/Pending_2/direct -type d -name "UNIT_*" | wc -l
```

#### 2.2 Обработка архивов

```bash
# Тест извлечения архивов
python3 -m docprep.cli.main substage extract run \
  --input Data/Processing/2025-03-18/Pending_1/archives \
  --cycle 1 \
  --date 2025-03-18 \
  --verbose

# Проверка результатов
find Data/Processing/2025-03-18/Pending_2/direct -type d -name "UNIT_*" | wc -l
```

#### 2.3 Обработка нормализации

```bash
# Нормализация имен
python3 -m docprep.cli.main substage normalize name \
  --input Data/Processing/2025-03-18/Pending_1/normalize \
  --cycle 1 \
  --date 2025-03-18 \
  --verbose

# Нормализация расширений
python3 -m docprep.cli.main substage normalize extension \
  --input Data/Processing/2025-03-18/Pending_1/normalize \
  --cycle 1 \
  --date 2025-03-18 \
  --verbose
```

#### 2.4 Полная обработка Pending_1

```bash
# Обработка всех Pending_1 поддиректорий
python3 -m docprep.cli.main stage pending \
  --cycle 1 \
  --pending Data/Processing/2025-03-18/Pending_1 \
  --date 2025-03-18 \
  --verbose

# Статистика после обработки
python3 -m docprep.cli.main utils stats Data/Processing/2025-03-18/Pending_2 --verbose
```

### Этап 3: Тестирование Merge (Цикл 1)

#### 3.1 Перемещение в Merge_1

```bash
# Перемещение готовых UNIT из Pending_1/direct в Merge_1/direct
python3 -m docprep.cli.main stage merge \
  --cycle 1 \
  --source Data/Processing/2025-03-18/Pending_1/direct \
  --target-base Data/Merge/2025-03-18 \
  --date 2025-03-18 \
  --verbose

# Проверка результатов
find Data/Merge/2025-03-18/Merge_1/direct -type d -name "UNIT_*" | wc -l
```

### Этап 4: Тестирование полного цикла

#### 4.1 Запуск одного полного цикла

```bash
# Запуск цикла 1 (классификация + обработка + merge)
python3 -m docprep.cli.main cycle run 1 \
  --input Data/Input/2025-03-18 \
  --date 2025-03-18 \
  --verbose

# Проверка результатов
echo "=== Результаты цикла 1 ==="
python3 -m docprep.cli.main utils stats Data/Merge/2025-03-18/Merge_1 --verbose
```

#### 4.2 Запуск всех 3 циклов

```bash
# Полный pipeline (3 цикла подряд)
python3 -m docprep.cli.main pipeline run \
  Data/Input/2025-03-18 \
  Data/Ready2Docling/2025-03-18 \
  --max-cycles 3 \
  --verbose

# Или поэтапно:
for cycle in 1 2 3; do
  echo "=== Цикл $cycle ==="
  python3 -m docprep.cli.main cycle run $cycle \
    --input Data/Input/2025-03-18 \
    --date 2025-03-18 \
    --verbose
done
```

### Этап 5: Тестирование Merge в Ready2Docling

#### 5.1 Сборка из всех Merge_N

```bash
# Сборка UNIT из Merge_1, Merge_2, Merge_3 в Ready2Docling
python3 -m docprep.cli.main merge collect \
  --source Data/Merge/2025-03-18 \
  --target Data/Ready2Docling/2025-03-18 \
  --verbose

# Проверка результатов
find Data/Ready2Docling/2025-03-18 -type d -name "UNIT_*" | wc -l

# Проверка сортировки PDF
find Data/Ready2Docling/2025-03-18/pdf/scan -type d -name "UNIT_*" | wc -l
find Data/Ready2Docling/2025-03-18/pdf/text -type d -name "UNIT_*" | wc -l
```

## Отладка и диагностика

### Проверка состояния UNIT

```bash
# Просмотр дерева директории
python3 -m docprep.cli.main inspect tree Data/Processing/2025-03-18/Pending_1

# Список UNIT с состояниями
python3 -m docprep.cli.main inspect units Data/Processing/2025-03-18/Pending_1

# Просмотр manifest конкретного UNIT
python3 -m docprep.cli.main inspect manifest UNIT_XXX \
  --directory Data/Processing/2025-03-18/Pending_1/direct/docx
```

### Проверка audit log

```bash
# Просмотр audit log UNIT
cat Data/Processing/2025-03-18/Pending_1/direct/docx/UNIT_XXX/audit.log.jsonl | jq '.'

# Поиск ошибок в audit log
find Data/Processing/2025-03-18 -name "audit.log.jsonl" -exec grep -l "error" {} \;
```

### Проверка валидации

```bash
# Валидация UNIT (через Python)
python3 << 'EOF'
from docprep.engine.validator import Validator
from pathlib import Path

validator = Validator()
unit_path = Path("Data/Processing/2025-03-18/Pending_1/direct/docx/UNIT_XXX")
result = validator.validate_unit(unit_path)
print(result)
EOF
```

## Типичные проблемы и решения

### Проблема 1: UNIT не перемещается

**Симптомы:**
- UNIT остается в исходной директории после операции

**Диагностика:**
```bash
# Проверка manifest
python3 -m docprep.cli.main inspect manifest UNIT_XXX --directory Data/Input/2025-03-18

# Проверка state machine
cat Data/Input/2025-03-18/UNIT_XXX/manifest.json | jq '.state_machine'
```

**Решение:**
- Проверить права доступа к директориям
- Проверить наличие manifest.json
- Проверить валидность state machine переходов

### Проблема 2: Ошибки конвертации

**Симптомы:**
- Ошибки при конвертации doc/docx/xls/xlsx

**Диагностика:**
```bash
# Проверка LibreOffice
which libreoffice
libreoffice --version

# Проверка файла
file Data/Processing/2025-03-18/Pending_1/convert/doc/UNIT_XXX/file.doc
```

**Решение:**
- Установить LibreOffice: `apt-get install libreoffice`
- Проверить целостность файла
- Проверить формат файла (может быть архивом)

### Проблема 3: Ошибки извлечения архивов

**Симптомы:**
- Ошибки при извлечении ZIP/RAR/7Z

**Диагностика:**
```bash
# Проверка архива
python3 << 'EOF'
import zipfile
with zipfile.ZipFile("path/to/archive.zip", 'r') as zf:
    print(zf.testzip())  # None = валиден
EOF
```

**Решение:**
- Проверить целостность архива
- Проверить наличие библиотек (rarfile, py7zr)
- Проверить защиту от zip bomb (MAX_UNPACK_SIZE_MB)

### Проблема 4: Неправильная классификация

**Симптомы:**
- UNIT попадает в неправильную категорию

**Диагностика:**
```bash
# Проверка детекции файла
python3 << 'EOF'
from docprep.utils.file_ops import detect_file_type
from pathlib import Path

result = detect_file_type(Path("path/to/file"))
print(result)
EOF
```

**Решение:**
- Проверить `detect_file_type()` результат
- Проверить логику классификации в `Classifier`
- Проверить manifest на наличие `is_fake_doc`, `needs_ocr`

## Чеклист тестирования

- [ ] Инициализация структуры директорий
- [ ] Классификация UNIT (dry-run)
- [ ] Классификация UNIT (реальная)
- [ ] Обработка convert
- [ ] Обработка archives
- [ ] Обработка normalize
- [ ] Перемещение в Merge
- [ ] Полный цикл 1
- [ ] Полный цикл 2
- [ ] Полный цикл 3
- [ ] Merge в Ready2Docling
- [ ] Проверка сортировки PDF (scan/text)
- [ ] Валидация UNIT
- [ ] Проверка audit log
- [ ] Проверка manifest

## Следующие шаги после тестирования

1. **Анализ результатов:**
   - Статистика по категориям
   - Статистика по ошибкам
   - Анализ audit log

2. **Оптимизация:**
   - Настройка параметров детекции
   - Настройка защиты от zip bomb
   - Оптимизация производительности

3. **Интеграция с Docling:**
   - Тестирование DoclingAdapter
   - Проверка AST nodes
   - Валидация JSON Schema


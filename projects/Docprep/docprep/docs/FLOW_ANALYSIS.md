# Анализ Flow обработки файлов: Input → Processing/Merge/Exceptions

## Обзор процесса

Полный flow обработки файлов от директории `Input` до финального распределения по директориям `Processing_1`, `Merge_0/Direct` и `Exceptions_1`.

## Шаг 1: Запуск классификации

### Команда CLI
```bash
docprep stage classifier --cycle 1 --input /path/to/Input --date 2025-03-18
```

### Точка входа: `cli/stage.py::stage_classifier()`

**Что происходит:**
1. Проверка существования директории `Input`
2. Создание экземпляра `Classifier()`
3. Вызов `process_directory_units()` для обработки всех UNIT в `Input`

## Шаг 2: Обработка каждого UNIT

### Функция: `engine/classifier.py::Classifier.classify_unit()`

### 2.1. Получение файлов UNIT

```python
files = get_unit_files(unit_path)
```

**Что делает:**
- Находит все файлы в директории UNIT (исключая `manifest.json` и `audit.log.jsonl`)
- Возвращает список `Path` объектов

**Файлы:**
- `utils/paths.py::get_unit_files()`

### 2.2. Загрузка существующего manifest (если есть)

```python
manifest = load_manifest(unit_path)
```

**Что делает:**
- Пытается загрузить `manifest.json` из директории UNIT
- Если manifest существует, извлекает `protocol_date` и `protocol_id`

**Файлы:**
- `core/manifest.py::load_manifest()`

### 2.3. Классификация каждого файла

```python
for file_path in files:
    classification = self._classify_file(file_path)
```

**Функция:** `engine/classifier.py::Classifier._classify_file()`

#### 2.3.1. Определение типа файла

```python
detection = detect_file_type(file_path)
```

**Функция:** `utils/file_ops.py::detect_file_type()`

**Что происходит:**

1. **Сбор источников истины:**
   - `collect_truth_sources()` собирает:
     - MIME тип (через `python-magic`)
     - MIME confidence (уровень уверенности)
     - Signature type (magic bytes)
     - Signature confidence
     - File extension
     - File header (первые 16 байт)

2. **Вызов Decision Engine:**
   ```python
   decision = resolve_type_decision(
       mime_type=mime_type,
       mime_confidence=mime_confidence,
       signature_type=signature_type,
       signature_confidence=signature_confidence,
       extension=extension,
   )
   ```
   - Файл: `core/decision_engine.py::resolve_type_decision()`
   - Реализует 7 формализованных сценариев
   - Возвращает: `true_type`, `classification`, `scenario`, `confidence`, `correct_extension`

3. **Структурный парсинг (для сложных форматов):**
   - ZIP/Office документы (DOCX, XLSX, PPTX)
   - PDF (проверка текстового слоя)
   - OLE2 (старые Excel файлы)
   - RAR, 7z архивы

4. **Post-Detection Validation:**
   - Проверка на fake docs (архивы с расширениями документов)
   - Проверка на polyglot files

**Результат `detect_file_type()`:**
```python
{
    "detected_type": "pdf",  # Истинный тип файла
    "mime_type": "application/pdf",
    "classification": "direct",  # direct | normalize | ambiguous
    "scenario": "1.1",  # Идентификатор сценария Decision Engine
    "confidence": 0.95,
    "correct_extension": "pdf",
    "original_extension": "pdf",
    "extension_matches_content": True,
    "is_archive": False,
    "needs_ocr": False,
}
```

#### 2.3.2. Определение категории файла

**Логика в `_classify_file()`:**

1. **Проверка на подписи:**
   ```python
   if extension in self.SIGNATURE_EXTENSIONS:
       return {"category": "special"}
   ```

2. **Проверка на неподдерживаемые форматы:**
   ```python
   if extension in self.UNSUPPORTED_EXTENSIONS:
       return {"category": "special"}
   ```

3. **Проверка на архивы:**
   ```python
   if detection.get("is_archive") or detected_type in ["zip_archive", "rar_archive", "7z_archive"]:
       return {"category": "extract", "needs_extraction": True}
   ```

4. **Проверка на необходимость конвертации:**
   ```python
   if detected_type in self.CONVERTIBLE_TYPES:  # doc, xls, ppt, rtf
       return {"category": "convert", "needs_conversion": True}
   ```

5. **Использование classification из Decision Engine:**
   ```python
   decision_classification = detection.get("classification")
   
   if decision_classification == "normalize":
       return {"category": "normalize", "needs_normalization": True}
   
   if decision_classification == "ambiguous":
       return {"category": "special"}  # Ambiguous → Exceptions
   
   if decision_classification == "direct":
       return {"category": "direct"}
   ```

**Результат `_classify_file()`:**
```python
{
    "category": "direct",  # direct | convert | extract | normalize | special
    "detected_type": "pdf",
    "needs_conversion": False,
    "needs_extraction": False,
    "needs_normalization": False,
    "extension_matches_content": True,
    "correct_extension": "pdf",
}
```

### 2.4. Определение категории UNIT

```python
category_counts = Counter(categories)
unique_categories = set(categories)
is_mixed = len(unique_categories) > 1

if is_mixed:
    unit_category = "mixed"  # → Exceptions
elif categories:
    unit_category = categories[0]  # Первая категория
else:
    unit_category = "unknown"
```

**Логика:**
- Если в UNIT файлы разных категорий → `mixed` → `Exceptions_1/Mixed/`
- Если все файлы одной категории → используем эту категорию
- Если категорий нет → `unknown`

### 2.5. Определение расширения для сортировки

```python
extension = get_extension_subdirectory(
    category=unit_category,
    classification=first_classification,
    original_extension=original_ext,
)
```

**Функция:** `core/unit_processor.py::get_extension_subdirectory()`

**Логика:**
- Для `direct`: использует `correct_extension` из Decision Engine
- Для `convert`: использует исходное расширение (doc, xls, ppt, rtf)
- Для `extract`: использует тип архива (zip, rar, 7z)
- Для `normalize`: использует `correct_extension` из Decision Engine

### 2.6. Определение целевой директории

```python
target_base_dir = self._get_target_directory_base(unit_category, cycle, protocol_date)
```

**Функция:** `engine/classifier.py::Classifier._get_target_directory_base()`

**Логика:**

1. **Для `direct` (только cycle == 1):**
   ```python
   if category == "direct" and cycle == 1:
       return merge_base / "Merge_0" / "Direct"
   ```
   - **Результат:** `Data/2025-03-18/Merge/Merge_0/Direct/`

2. **Для `special` или `mixed`:**
   ```python
   if category in ["special", "mixed"]:
       return exceptions_base / f"Exceptions_{cycle}"
   ```
   - **Результат:** `Data/2025-03-18/Exceptions/Exceptions_1/`

3. **Для `convert`, `extract`, `normalize`:**
   ```python
   processing_paths = get_processing_paths(cycle, processing_base)
   return processing_paths[category.capitalize()]
   ```
   - **Результат:** 
     - `convert` → `Data/2025-03-18/Processing/Processing_1/Convert/`
     - `extract` → `Data/2025-03-18/Processing/Processing_1/Extract/`
     - `normalize` → `Data/2025-03-18/Processing/Processing_1/Normalize/`

### 2.7. Создание/обновление manifest

```python
manifest = create_unit_manifest_if_needed(
    unit_path=unit_path,
    unit_id=unit_id,
    protocol_id=protocol_id,
    protocol_date=protocol_date,
    files=manifest_files,
    cycle=cycle,
)
```

**Функция:** `core/unit_processor.py::create_unit_manifest_if_needed()`

**Что создается:**
- `manifest.json` в директории UNIT
- Содержит: метаданные UNIT, список файлов, state machine, операции

### 2.8. Перемещение UNIT в целевую директорию

```python
target_dir = move_unit_to_target(
    unit_dir=unit_path,
    target_base_dir=target_base_dir,
    extension=extension,
    dry_run=dry_run,
    copy_mode=copy_mode,  # НОВОЕ: опция копирования
)
```

**Функция:** `core/unit_processor.py::move_unit_to_target()`

**Что происходит:**

1. **Определение целевой директории:**
   ```python
   if extension:
       target_dir = target_base_dir / extension / unit_id
   else:
       target_dir = target_base_dir / unit_id
   ```

2. **Создание директорий:**
   ```python
   target_dir.parent.mkdir(parents=True, exist_ok=True)
   ```

3. **Перемещение или копирование:**
   ```python
   if copy_mode:
       shutil.copytree(str(unit_dir), str(target_dir), dirs_exist_ok=True)
       # Исходные файлы остаются в Input
   else:
       shutil.move(str(unit_dir), str(target_dir))
       # Исходные файлы удаляются из Input
       _cleanup_empty_directories(unit_dir)
   ```

**Примеры путей:**

- **Direct файл:**
  - `Input/UNIT_xxx/` → `Merge/Merge_0/Direct/pdf/UNIT_xxx/`

- **Convert файл:**
  - `Input/UNIT_xxx/` → `Processing/Processing_1/Convert/doc/UNIT_xxx/`

- **Extract файл:**
  - `Input/UNIT_xxx/` → `Processing/Processing_1/Extract/zip/UNIT_xxx/`

- **Normalize файл:**
  - `Input/UNIT_xxx/` → `Processing/Processing_1/Normalize/docx/UNIT_xxx/`

- **Special/Mixed файл:**
  - `Input/UNIT_xxx/` → `Exceptions/Exceptions_1/Special/UNIT_xxx/`
  - или `Exceptions/Exceptions_1/Mixed/UNIT_xxx/`
  - или `Exceptions/Exceptions_1/Ambiguous/UNIT_xxx/`

### 2.9. Обновление state machine

```python
if unit_category == "direct" and cycle == 1:
    update_unit_state(
        unit_path=target_dir,
        new_state=UnitState.MERGED_DIRECT,
        cycle=cycle,
        operation={...},
    )
elif unit_category in ["special", "mixed"]:
    update_unit_state(
        unit_path=target_dir,
        new_state=UnitState.CLASSIFIED_1,  # или CLASSIFIED_2, CLASSIFIED_3
        cycle=cycle,
        operation={...},
    )
else:
    update_unit_state(
        unit_path=target_dir,
        new_state=UnitState.CLASSIFIED_1,  # или CLASSIFIED_2, CLASSIFIED_3
        cycle=cycle,
        operation={...},
    )
```

**Функция:** `core/unit_processor.py::update_unit_state()`

**Что происходит:**
- Обновляет `manifest.json` с новым состоянием
- Записывает операцию в историю
- Обновляет state machine

## Итоговое распределение

### Direct файлы
- **Путь:** `Data/2025-03-18/Merge/Merge_0/Direct/{extension}/UNIT_xxx/`
- **State:** `MERGED_DIRECT`
- **Условие:** `category == "direct"` и `cycle == 1`

### Convert файлы
- **Путь:** `Data/2025-03-18/Processing/Processing_1/Convert/{extension}/UNIT_xxx/`
- **State:** `CLASSIFIED_1`
- **Условие:** `category == "convert"`

### Extract файлы
- **Путь:** `Data/2025-03-18/Processing/Processing_1/Extract/{extension}/UNIT_xxx/`
- **State:** `CLASSIFIED_1`
- **Условие:** `category == "extract"`

### Normalize файлы
- **Путь:** `Data/2025-03-18/Processing/Processing_1/Normalize/{extension}/UNIT_xxx/`
- **State:** `CLASSIFIED_1`
- **Условие:** `category == "normalize"`

### Special/Mixed/Ambiguous файлы
- **Путь:** `Data/2025-03-18/Exceptions/Exceptions_1/{subcategory}/UNIT_xxx/`
- **State:** `CLASSIFIED_1`
- **Условие:** `category in ["special", "mixed"]` или `scenario == "ambiguous"`

## Новая опция: copy_mode

### Использование

```bash
# Копирование вместо перемещения (сохраняет исходные файлы в Input)
docprep stage classifier --cycle 1 --input /path/to/Input --date 2025-03-18 --copy
```

### Преимущества

1. **Повторные тесты:** Не нужно заново наполнять `Input` после каждого теста
2. **Отладка:** Можно проверить результаты без потери исходных данных
3. **Безопасность:** Исходные файлы остаются нетронутыми

### Реализация

- Параметр `copy_mode` добавлен в:
  - `core/unit_processor.py::move_unit_to_target()`
  - `engine/classifier.py::Classifier.classify_unit()`
  - `cli/stage.py::stage_classifier()`

- При `copy_mode=True`:
  - Используется `shutil.copytree()` вместо `shutil.move()`
  - Исходные файлы остаются в `Input`
  - Целевые файлы создаются как копии

## Готовность к тестированию

### ✅ Проверено

1. **Flow от Input до финальных директорий:**
   - ✅ Классификация каждого файла через Decision Engine
   - ✅ Определение категории UNIT
   - ✅ Создание manifest
   - ✅ Перемещение/копирование в целевую директорию
   - ✅ Обновление state machine

2. **Опция сохранения исходных файлов:**
   - ✅ Параметр `--copy` добавлен в CLI
   - ✅ Логика копирования реализована
   - ✅ Исходные файлы сохраняются в Input

3. **Структура директорий:**
   - ✅ `Processing_1/Convert`, `Extract`, `Normalize` (без Direct)
   - ✅ `Merge_0/Direct` (единственная Direct директория)
   - ✅ `Exceptions_1/Ambiguous`, `Mixed`, `Special`

### 🔍 Рекомендации для тестирования

1. **Первый тест с `--copy`:**
   ```bash
   docprep stage classifier --cycle 1 \
     --input final_preprocessing/Data/2025-03-18/Input \
     --date 2025-03-18 \
     --copy \
     --verbose
   ```

2. **Проверка результатов:**
   - Проверить, что файлы остались в `Input`
   - Проверить распределение по директориям
   - Проверить создание `manifest.json`
   - Проверить корректность определения расширений

3. **Повторные тесты:**
   - Можно запускать многократно с `--copy`
   - Исходные файлы не удаляются
   - Можно очищать целевые директории перед повторным запуском


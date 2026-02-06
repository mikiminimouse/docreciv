# DocPrep Manual

Система preprocessing документов для подготовки к Docling.

## Обзор

DocPrep выполняет 3-циклическую обработку документов:
1. **Цикл 1**: Первичная классификация, распаковка архивов, конвертация форматов
2. **Цикл 2**: Обработка извлечённых из архивов файлов
3. **Цикл 3**: Финальная обработка вложенных архивов

### Категории обработки

| Категория | Описание | Результат |
|-----------|----------|-----------|
| `direct` | PDF, DOCX, HTML — готовы к Docling | → Merge/Direct/ |
| `convert` | DOC, XLS, PPT — требуют конвертации | → Processing/Convert/ |
| `extract` | ZIP, RAR, 7Z — требуют распаковки | → Processing/Extract/ |
| `normalize` | Файлы с неверным расширением | → Processing/Normalize/ |
| `mixed` | UNIT с несколькими типами файлов | → Merge/Direct/Mixed/ |

---

## Установка

### Зависимости

```bash
# Python 3.11+
pip install typer pydantic python-magic pypdf pillow

# LibreOffice (обязательно для конвертации DOC/XLS/PPT)
apt install libreoffice-core libreoffice-writer libreoffice-calc libreoffice-impress

# Для архивов
apt install p7zip-full unrar-free
```

### Проверка установки

```bash
cd /path/to/docprep
PYTHONPATH=.. python3 -m docprep.cli.main --help
```

---

## CLI Команды

### Структура команд

```
docprep
├── pipeline          # Полный preprocessing (все 3 цикла)
├── cycle             # Один цикл (classify → pending → merge)
├── stage             # Этап внутри цикла
│   ├── classifier    # Классификация UNIT
│   ├── pending       # Обработка Processing директорий
│   └── merge         # Объединение результатов
├── substage          # Атомарные операции
│   ├── convert       # Конвертация через LibreOffice
│   ├── extract       # Распаковка архивов
│   └── normalize     # Исправление расширений
├── merge             # Merge утилиты
│   ├── collect       # Сбор UNIT из разных источников
│   └── to-docling    # Финальный merge в Ready2Docling
├── classifier        # Прямой доступ к классификатору
├── chunked-classifier # Chunked классификация с recovery
├── inspect           # Отладка и анализ
├── stats             # Статистика
└── utils             # Сервисные команды
```

### Основные команды

#### 1. Классификация (stage classifier)

```bash
PYTHONPATH=.. python3 -m docprep.cli.main stage classifier \
    --cycle 1 \
    --input ../Data/2025-12-23/Input \
    --date 2025-12-23 \
    --copy \
    --verbose
```

Параметры:
- `--cycle` — номер цикла (1, 2, 3)
- `--input` — входная директория с UNIT
- `--date` — дата протокола (YYYY-MM-DD)
- `--copy` — копировать вместо перемещения (сохраняет исходные файлы)
- `--verbose` — подробный вывод
- `--dry-run` — режим симуляции

#### 2. Обработка Pending (stage pending)

```bash
PYTHONPATH=.. python3 -m docprep.cli.main stage pending \
    --cycle 1 \
    --pending ../Data/2025-12-23/Processing/Processing_1 \
    --date 2025-12-23 \
    --verbose
```

#### 3. Merge в Ready2Docling

```bash
PYTHONPATH=.. python3 -m docprep.cli.main merge to-docling \
    ../Data/2025-12-23/Merge/Direct/pdf \
    ../Data/2025-12-23/Merge/Direct/docx \
    --target ../Data/2025-12-23/Ready2Docling \
    --verbose
```

#### 4. Полный pipeline

```bash
PYTHONPATH=.. python3 -m docprep.cli.main pipeline \
    --input ../Data/2025-12-23/Input \
    --date 2025-12-23 \
    --copy
```

---

## Структура данных

### Входные данные (Input)

```
Data/YYYY-MM-DD/Input/
├── UNIT_abc123/
│   ├── Протокол.pdf
│   └── Приложение.docx
├── UNIT_def456/
│   └── archive.zip
└── ...
```

### Результаты обработки

```
Data/YYYY-MM-DD/
├── Input/              # Исходные данные
├── Processing/         # Промежуточная обработка
│   └── Processing_1/   # Цикл 1
│       ├── Convert/    # DOC/XLS → PDF/DOCX
│       ├── Extract/    # Архивы
│       └── Normalize/  # Исправление расширений
├── Merge/              # Результаты классификации
│   └── Direct/
│       ├── pdf/        # PDF файлы
│       ├── docx/       # DOCX файлы
│       ├── html/       # HTML файлы
│       └── Mixed/      # Смешанные UNIT
├── Exceptions/         # Ошибки и исключения
│   └── Direct/
│       ├── Empty/      # Пустые UNIT
│       ├── Ambiguous/  # Неопределённые типы
│       └── Error/      # Ошибки обработки
└── Ready2Docling/      # Финальный результат для Docling
    ├── pdf/
    │   ├── text/       # PDF с текстом
    │   └── scan/       # Сканы (needs_ocr=true)
    ├── docx/
    ├── html/
    └── Mixed/
```

### Manifest (manifest.json)

Каждый UNIT содержит `manifest.json` с метаданными:

```json
{
  "unit_id": "UNIT_abc123",
  "protocol_id": "abc123",
  "protocol_date": "2025-12-23",
  "version": 2,
  "files": [
    {
      "original_name": "Протокол.pdf",
      "current_name": "Протокол.pdf",
      "mime_type": "application/pdf",
      "detected_type": "pdf",
      "needs_ocr": false,
      "sha256": "...",
      "size": 123456
    }
  ],
  "state_machine": {
    "current_state": "MERGED_DIRECT",
    "state_trace": ["RAW", "CLASSIFIED_1", "MERGED_DIRECT"]
  },
  "processing": {
    "route": "pdf_text",
    "cycle": 1
  }
}
```

---

## Типы файлов

### Поддерживаемые форматы

| Тип | Расширения | Обработка |
|-----|------------|-----------|
| PDF | .pdf | Direct (text/scan) |
| DOCX | .docx | Direct |
| DOC | .doc | Convert → PDF/DOCX |
| HTML | .html, .htm | Direct |
| XLSX | .xlsx | Direct |
| XLS | .xls | Convert → XLSX |
| PPTX | .pptx | Direct |
| PPT | .ppt | Convert → PPTX |
| ZIP | .zip | Extract |
| RAR | .rar | Extract |
| 7Z | .7z | Extract |
| Images | .jpg, .png, .tiff | Direct (OCR) |

### Определение needs_ocr для PDF

PDF анализируется на наличие текста:
- `needs_ocr: false` — PDF содержит текст → pdf/text/
- `needs_ocr: true` — PDF является сканом → pdf/scan/

---

## Troubleshooting

### LibreOffice не найден

```
LibreOfficeNotFoundError: LibreOffice not found
```

Решение:
```bash
apt install libreoffice-core libreoffice-writer
```

### FileExistsError при повторном запуске

```
FileExistsError: Target directory not empty
```

Причина: UNIT уже был обработан ранее.

Решение:
1. Очистите целевую директорию
2. Или используйте `--dry-run` для проверки

### Ошибки извлечения архивов

```
ExtractionError: Failed to extract archive
```

Проверьте:
1. Установлен ли p7zip: `apt install p7zip-full`
2. Установлен ли unrar: `apt install unrar-free`
3. Архив не повреждён

### Пустые UNIT

UNIT без файлов или только с manifest.json перемещаются в:
```
Exceptions/Direct/Empty/UNIT_xxx
```

---

## Запуск тестов

```bash
cd /path/to/docprep
PYTHONPATH=.. pytest tests/ -v
```

### Проверка покрытия

```bash
PYTHONPATH=.. pytest --cov=docprep --cov-report=term-missing tests/
```

---

## Быстрый старт

### 1. Подготовка данных

```bash
mkdir -p Data/2025-12-23/{Input,Processing,Merge,Exceptions,Ready2Docling}
# Поместите UNIT_* директории в Data/2025-12-23/Input/
```

### 2. Запуск классификации

```bash
cd docprep
PYTHONPATH=.. python3 -m docprep.cli.main stage classifier \
    --cycle 1 \
    --input ../Data/2025-12-23/Input \
    --date 2025-12-23 \
    --copy \
    --verbose
```

### 3. Проверка результатов

```bash
# Статистика по Merge/Direct
for dir in ../Data/2025-12-23/Merge/Direct/*/; do
    echo "$(basename $dir): $(ls "$dir" | wc -l)"
done
```

### 4. Обработка Processing (если есть)

```bash
PYTHONPATH=.. python3 -m docprep.cli.main stage pending \
    --cycle 1 \
    --pending ../Data/2025-12-23/Processing/Processing_1 \
    --date 2025-12-23 \
    --verbose
```

### 5. Финальный merge

```bash
PYTHONPATH=.. python3 -m docprep.cli.main merge to-docling \
    ../Data/2025-12-23/Merge/Direct/pdf \
    ../Data/2025-12-23/Merge/Direct/docx \
    ../Data/2025-12-23/Merge/Direct/Mixed \
    --target ../Data/2025-12-23/Ready2Docling \
    --verbose
```

---

## Версия

DocPrep v2.0

Последнее обновление: 2026-01-16

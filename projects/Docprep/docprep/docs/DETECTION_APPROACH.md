# Подход к детекции файлов: Реализация

**Дата:** 2025-01-XX  
**Основа:** Стандартный каскадный подход из production-систем

---

## Архитектура детекции (каскадный подход)

```
1. python-magic (magic bytes)
   ↓ если неоднозначно (application/zip, application/pdf)
   
2. Структурный парсинг
   ├─ ZIP/Office → zipfile (проверка [Content_Types].xml, word/xl/ppt папок)
   ├─ PDF → pypdf (проверка текстового слоя на нескольких страницах)
   └─ OLE2 → проверка сигнатуры \xD0\xCF\x11\xE0
   
3. Валидация целостности
   ├─ ZIP → testzip()
   └─ Другие → проверка структуры
   
4. Сравнение и бизнес-логика
   ├─ Extension vs detected_type (fake_doc)
   └─ needs_ocr для PDF (route в Docling)
```

---

## Библиотеки (все на PyPI)

| Библиотека | Назначение | Когда используется |
|------------|------------|-------------------|
| **python-magic** | Magic bytes детекция | Всегда (первая линия) |
| **zipfile** (stdlib) | Структурная проверка ZIP/Office | Когда MIME = application/zip |
| **pypdf** | PDF анализ (текстовый слой) | Когда MIME = application/pdf |
| **olefile** (опционально) | OLE2 проверка | Для точного определения xls |

---

## Реализация для плана

### Этап 1: Улучшение detect_file_type()

**Что нужно:**
1. Добавить структурную проверку ZIP/Office через `zipfile`
2. Добавить проверку PDF текстового слоя через `pypdf` (несколько страниц)
3. Добавить `testzip()` для валидации ZIP
4. Добавить проверку `is_fake_doc` (сравнение extension vs detected_type)
5. Добавить проверку OLE2 для Excel (опционально)

**Код структура:**
```python
def detect_file_type(file_path: Path) -> Dict[str, Any]:
    # 1. Базовое определение через python-magic
    mime_type = magic.Magic(mime=True).from_file(str(file_path))
    
    # 2. Если application/zip → структурный парсинг
    if mime_type.startswith("application/zip"):
        return _detect_zip_or_office(file_path, mime_type, extension)
    
    # 3. Если application/pdf → проверка текстового слоя
    elif mime_type.startswith("application/pdf"):
        return _detect_pdf_with_text_layer(file_path, mime_type)
    
    # 4. Если application/vnd.ms-excel → проверка OLE2
    elif mime_type.startswith("application/vnd.ms-excel"):
        return _detect_excel_with_ole2(file_path, mime_type, extension)
    
    # ... остальные типы
```

---

## Связь с планом

**План:** `доведение_preprocessing_до_100%_готовности_3eebb996.plan.md`

**Этап 1.1:** Улучшение `detect_file_type` включает все эти проверки.

**Источники кода:**
- `preprocessing/router/file_detection.py` — примеры структурного парсинга
- Стандартные библиотеки Python (`zipfile`, `pypdf`)

---

## Вывод

Используем стандартный каскадный подход, применяемый в production-системах. Это не кастомная логика, а проверенные практики для надежной детекции файлов.


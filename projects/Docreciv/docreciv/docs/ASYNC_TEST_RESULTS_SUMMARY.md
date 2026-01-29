# Async Test Results Summary (Январь 2026)

## Обзор

Документ содержит суммарные результаты трёх тестовых запусков async режима Docreciv downloader
в январе 2026 года. Цель тестов — проверить производительность и надёжность асинхронной
загрузки протоколов закупок.

---

## Сравнительная таблица всех тестов

| Дата | Протоколов | Файлов | Success Rate | Throughput | Duration | Ошибок |
|------|-----------|--------|-------------|-----------|----------|--------|
| 2026-01-22 | 1,703 | 1,841 | 99.4% | 3.31 f/s | 9.3 мин | 11 |
| 2026-01-24 | 1,665 | 1,823 | 98.2% | 4.25 f/s | 7.1 мин | 34 |
| 2026-01-27 | 1,502 | 1,663 | 99.8% | 4.38 f/s | 6.3 мин | 4 |
| **Среднее** | **1,623** | **1,776** | **99.1%** | **3.98 f/s** | **7.6 мин** | **16** |

---

## Детальные результаты по датам

### 2026-01-22 (Четверг)

**Характеристики:**
- Максимум за 10 дней по количеству протоколов
- Рабочий день с высокой активностью

**Метрики:**
- Протоколов: 1,703
- Файлов загружено: 1,841
- Success Rate: 99.4%
- Throughput: 3.31 files/s
- Duration: 556s (9.3 минуты)
- Ошибок: 11 (timeout/404/connection)

**Файловая система:**
- UNIT директорий: 1,701
- PDF файлов: 1,056
- DOCX файлов: 369
- Disk usage: 1.3GB

### 2026-01-24 (Суббота)

**Характеристики:**
- Первый полноценный тест после миграции на `loadDate`
- 40x больше данных по сравнению с `publicationDateTime`

**Метрики:**
- Протоколов: 1,665
- Файлов загружено: 1,823
- Success Rate: 98.2%
- Throughput: 4.25 files/s
- Duration: 429s (7.1 минуты)
- Ошибок: 34 (timeout/connection reset)

**Файловая система:**
- UNIT директорий: 1,665
- PDF файлов: 1,006
- DOCX файлов: 365
- Disk usage: 1.0GB

### 2026-01-27 (Вторник)

**Характеристики:**
- Лучший success rate среди всех тестов
- Минимальное количество ошибок

**Метрики:**
- Протоколов: 1,502
- Файлов загружено: 1,663
- Success Rate: 99.8%
- Throughput: 4.38 files/s
- Duration: 380s (6.3 минуты)
- Ошибок: 4 (timeout)

**Файловая система:**
- UNIT директорий: 1,502
- PDF файлов: 921
- DOCX файлов: 354
- Disk usage: 996MB

---

## Анализ и выводы

### Производительность

| Метрика | Значение |
|---------|----------|
| Средний throughput | 3.98 files/s |
| Средний success rate | 99.1% |
| Средняя длительность | 7.6 минут |
| Всего загружено | 4,870 протоколов, 5,327 файлов |

### Key Improvements (Январь 2026)

1. **Миграция на `loadDate`**
   - 40x больше данных по сравнению с `publicationDateTime`
   - Пример: 41 протокол → 1,665 протоколов за выходные

2. **Исправление `sanitize_filename()`**
   - Ограничение длины с учётом UTF-8 (200 chars, max 255 bytes)
   - Ноль ошибок "File name too long" после исправления

3. **Исправление дублирования пути**
   - UNIT директории теперь создаются в правильном месте
   - Путь: `/home/pak/Processing data/YYYY-MM-DD/Input/UNIT_xxx/`

4. **Graceful error handling**
   - Исправлена проблема `/app` (Docker leftovers)
   - Правильные дефолтные пути для local development

### Bottleneck Analysis

| Компонент | Статус |
|-----------|--------|
| Network (zakupki.gov.ru) | PRIMARY BOTTLENECK (~2 files/s limit) |
| MongoDB Connection Pool | OK |
| VPN Overhead | Минимален |
| Disk I/O | OK |
| Filename handling | ✅ Исправлено |
| Path handling | ✅ Исправлено |

---

## Конфигурация тестов

```bash
# Async downloader settings
export ASYNC_MAX_CONCURRENT_REQUESTS=100
export ASYNC_MAX_CONCURRENT_PROTOCOLS=50
export ASYNC_LIMIT_PER_HOST=50

# MongoDB settings
LOCAL_MONGO_SERVER=localhost:27018
MONGO_METADATA_DB=docling_metadata
MONGO_METADATA_USER=admin

# Processing data
PROCESSING_DATA_DIR=/home/pak/Processing data
```

---

## Рекомендации

### Немедленные эффекты
- ✅ Выполнено: Увеличение `limit_per_host` до 50
- ✅ Выполнено: Graceful error handling в конфигурации
- ✅ Выполнено: Правильные дефолтные пути для local dev

### Среднесрочные улучшения
- Увеличить MongoDB `maxPoolSize` до 50
- Implement connection reuse между запусками
- Adaptive chunk sizing для разных типов файлов

### Долгосрочные улучшения
- Proxy rotation для распределения запросов
- Predictive prefetching
- Circuit breaker pattern для external dependencies

---

## Связанные документы

- [SYSTEM_STATE_2026-01-28.md](./SYSTEM_STATE_2026-01-28.md) — Полное состояние системы
- [ASYNC_TEST_RESULTS_2026-01-28.md](./ASYNC_TEST_RESULTS_2026-01-28.md) — Детальные результаты первого теста
- [ARCHITECTURE.md](./ARCHITECTURE.md) — Архитектура системы
- [DATA_FLOW.md](./DATA_FLOW.md) — Потоки данных

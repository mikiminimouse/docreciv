"""
Модуль параллелизма для DocPrep.

Содержит функции для расчёта оптимального количества workers
и параллельной обработки файлов/UNIT на основе доступных ресурсов.

Паттерн взят из успешной реализации в docling_multitender (2.5x speedup).

Примечание по безопасности:
- ProcessPoolExecutor использует pickle для сериализации
- Это безопасно в данном контексте, так как обрабатываются только локальные файлы
- Все функции должны быть определены на уровне модуля (не lambda)
"""
import os
import logging
from typing import Optional, Dict, Any, List, Callable, TypeVar
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from pathlib import Path

logger = logging.getLogger(__name__)

# Типы для generic функций
T = TypeVar('T')
R = TypeVar('R')


# =============================================================================
# Memory Configuration per Worker
# =============================================================================

MEMORY_PER_WORKER_MB: Dict[str, int] = {
    "classifier": 256,      # MB - легкие I/O операции (detect_file_type)
    "converter": 512,       # MB - LibreOffice (оптимизировано x4)
    "extractor": 512,       # MB - распаковка архивов (ZIP/RAR/7Z)
    "normalizer": 256,      # MB - переименование файлов
    "unit_processor": 512,  # MB - обработка UNIT (classifier + I/O)
}

# Жёсткие лимиты workers по типам операций
# ОПТИМИЗАЦИЯ x4: Значения для 64 CPU / 16 GB RAM (агрессивное масштабирование)
MAX_WORKERS_BY_TYPE: Dict[str, int] = {
    "classifier": 64,       # x4 - I/O bound, много потоков
    "converter": 21,        # x2 - ограничен Xvfb displays (cpu_count // 3)
    "extractor": 45,        # x3 - I/O bound (распаковка архивов)
    "normalizer": 48,       # x3 - Легкие операции
    "unit_processor": 36,   # x3 - Комбинированные операции
}


def get_available_memory_mb() -> float:
    """
    Возвращает доступную память в мегабайтах.

    Использует psutil если доступен, иначе возвращает консервативную оценку.

    Returns:
        Доступная память в MB
    """
    try:
        import psutil
        return psutil.virtual_memory().available / (1024 * 1024)
    except ImportError:
        logger.warning("psutil not available, using conservative memory estimate (4GB)")
        return 4096.0  # Консервативная оценка: 4GB


def get_total_memory_mb() -> float:
    """
    Возвращает общую память системы в мегабайтах.

    Returns:
        Общая память в MB
    """
    try:
        import psutil
        return psutil.virtual_memory().total / (1024 * 1024)
    except ImportError:
        return 8192.0  # Консервативная оценка: 8GB


def calculate_optimal_workers(
    operation_type: str = "classifier",
    available_memory_mb: Optional[float] = None,
    cpu_count: Optional[int] = None,
) -> int:
    """
    Рассчитывает оптимальное количество workers для параллельной обработки.

    Учитывает:
    - Доступную память
    - Количество CPU
    - Тип операции (разные операции требуют разного количества памяти)
    - Жёсткие лимиты по типам операций

    Args:
        operation_type: Тип операции (classifier, converter, extractor, normalizer, unit_processor)
        available_memory_mb: Доступная память в MB (опционально, автоопределение)
        cpu_count: Количество CPU (опционально, автоопределение)

    Returns:
        Оптимальное количество workers (минимум 1)

    Example:
        >>> workers = calculate_optimal_workers("classifier")
        >>> print(f"Using {workers} workers for classification")
    """
    if available_memory_mb is None:
        available_memory_mb = get_available_memory_mb()

    if cpu_count is None:
        cpu_count = os.cpu_count() or 4

    # Получаем требования к памяти для типа операции
    memory_per_worker = MEMORY_PER_WORKER_MB.get(operation_type, 512)

    # Рассчитываем лимит по памяти (оставляем 20% резерв)
    usable_memory = available_memory_mb * 0.8
    memory_based_limit = int(usable_memory / memory_per_worker)

    # Получаем жёсткий лимит для типа операции
    max_for_type = MAX_WORKERS_BY_TYPE.get(operation_type, 8)

    # Выбираем минимум из трёх ограничений
    optimal = min(memory_based_limit, max_for_type, cpu_count)

    # Минимум 1 worker
    result = max(1, optimal)

    logger.debug(
        f"calculate_optimal_workers({operation_type}): "
        f"memory_limit={memory_based_limit}, type_limit={max_for_type}, "
        f"cpu_limit={cpu_count} -> {result} workers"
    )

    return result


def parallel_map_threads(
    func: Callable[[T], R],
    items: List[T],
    max_workers: Optional[int] = None,
    operation_type: str = "classifier",
    desc: str = "Processing",
) -> List[R]:
    """
    Параллельно применяет функцию к списку элементов используя ThreadPoolExecutor.

    Подходит для I/O-bound операций (чтение файлов, сетевые запросы).

    Args:
        func: Функция для применения к каждому элементу
        items: Список элементов для обработки
        max_workers: Количество workers (опционально, автоопределение)
        operation_type: Тип операции для расчёта workers
        desc: Описание операции для логирования

    Returns:
        Список результатов в том же порядке, что и входные элементы

    Example:
        >>> from docprep.utils.file_ops import detect_file_type
        >>> files = [Path("a.pdf"), Path("b.docx")]
        >>> detections = parallel_map_threads(detect_file_type, files)
    """
    if not items:
        return []

    if max_workers is None:
        max_workers = calculate_optimal_workers(operation_type)

    # Для малого количества элементов последовательная обработка быстрее
    if len(items) <= 2:
        logger.debug(f"{desc}: sequential processing ({len(items)} items)")
        return [func(item) for item in items]

    logger.info(f"{desc}: parallel processing {len(items)} items with {max_workers} workers")

    # Используем executor.map для сохранения порядка
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(func, items))

    return results


def parallel_map_processes(
    func: Callable[[T], R],
    items: List[T],
    max_workers: Optional[int] = None,
    operation_type: str = "unit_processor",
    desc: str = "Processing",
) -> List[R]:
    """
    Параллельно применяет функцию к списку элементов используя ProcessPoolExecutor.

    Подходит для CPU-bound операций.
    ВАЖНО: func должна быть определена на уровне модуля (не lambda).

    Args:
        func: Функция для применения к каждому элементу
        items: Список элементов для обработки
        max_workers: Количество workers (опционально, автоопределение)
        operation_type: Тип операции для расчёта workers
        desc: Описание операции для логирования

    Returns:
        Список результатов в том же порядке, что и входные элементы
    """
    if not items:
        return []

    if max_workers is None:
        max_workers = calculate_optimal_workers(operation_type)

    # Для малого количества элементов последовательная обработка быстрее
    # (Process creation overhead ~1-3 секунды)
    if len(items) <= 3:
        logger.debug(f"{desc}: sequential processing ({len(items)} items, process overhead)")
        return [func(item) for item in items]

    logger.info(f"{desc}: parallel processing {len(items)} items with {max_workers} processes")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(func, items))

    return results


def parallel_foreach_threads(
    func: Callable[[T], R],
    items: List[T],
    max_workers: Optional[int] = None,
    operation_type: str = "classifier",
    desc: str = "Processing",
    fail_fast: bool = False,
) -> Dict[str, Any]:
    """
    Параллельно применяет функцию к списку элементов, собирая результаты и ошибки.

    В отличие от parallel_map_threads, не прерывается при ошибках и собирает
    статистику успешных/неуспешных операций.

    Args:
        func: Функция для применения к каждому элементу
        items: Список элементов для обработки
        max_workers: Количество workers (опционально, автоопределение)
        operation_type: Тип операции для расчёта workers
        desc: Описание операции для логирования
        fail_fast: Если True, прерывается при первой ошибке

    Returns:
        Словарь с результатами:
        {
            "results": List[R],  # Успешные результаты
            "errors": List[Dict],  # Ошибки
            "succeeded": int,
            "failed": int,
            "total": int,
        }
    """
    result = {
        "results": [],
        "errors": [],
        "succeeded": 0,
        "failed": 0,
        "total": len(items),
    }

    if not items:
        return result

    if max_workers is None:
        max_workers = calculate_optimal_workers(operation_type)

    logger.info(f"{desc}: processing {len(items)} items with {max_workers} workers")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {executor.submit(func, item): item for item in items}

        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                res = future.result()
                result["results"].append(res)
                result["succeeded"] += 1
            except Exception as e:
                error_info = {
                    "item": str(item),
                    "error": str(e),
                    "error_type": type(e).__name__,
                }
                result["errors"].append(error_info)
                result["failed"] += 1
                logger.warning(f"{desc}: failed for {item}: {e}")

                if fail_fast:
                    # Отменяем оставшиеся задачи
                    for f in future_to_item:
                        f.cancel()
                    break

    logger.info(
        f"{desc}: completed - {result['succeeded']} succeeded, "
        f"{result['failed']} failed out of {result['total']}"
    )

    return result


class ParallelConfig:
    """
    Конфигурация параллелизма для сессии обработки.

    Позволяет настроить параметры параллелизма один раз и использовать
    их во всех операциях.
    """

    def __init__(
        self,
        enabled: bool = True,
        max_workers_override: Optional[Dict[str, int]] = None,
        memory_per_worker_override: Optional[Dict[str, int]] = None,
    ):
        """
        Инициализирует конфигурацию параллелизма.

        Args:
            enabled: Включить параллелизм (False = последовательная обработка)
            max_workers_override: Переопределение лимитов workers по типам
            memory_per_worker_override: Переопределение требований к памяти
        """
        self.enabled = enabled
        self.max_workers_override = max_workers_override or {}
        self.memory_per_worker_override = memory_per_worker_override or {}

        # Кешируем системные параметры
        self._cpu_count = os.cpu_count() or 4
        self._available_memory_mb = get_available_memory_mb()
        self._total_memory_mb = get_total_memory_mb()

        logger.info(
            f"ParallelConfig initialized: enabled={enabled}, "
            f"cpu_count={self._cpu_count}, "
            f"available_memory={self._available_memory_mb:.0f}MB"
        )

    def get_workers(self, operation_type: str) -> int:
        """
        Возвращает количество workers для типа операции.

        Args:
            operation_type: Тип операции

        Returns:
            Количество workers (1 если параллелизм выключен)
        """
        if not self.enabled:
            return 1

        # Проверяем override
        if operation_type in self.max_workers_override:
            return self.max_workers_override[operation_type]

        # Используем стандартный расчёт
        return calculate_optimal_workers(
            operation_type=operation_type,
            available_memory_mb=self._available_memory_mb,
            cpu_count=self._cpu_count,
        )

    def get_system_info(self) -> Dict[str, Any]:
        """
        Возвращает информацию о системе.

        Returns:
            Словарь с системными параметрами
        """
        return {
            "cpu_count": self._cpu_count,
            "total_memory_mb": self._total_memory_mb,
            "available_memory_mb": self._available_memory_mb,
            "parallel_enabled": self.enabled,
            "workers_by_type": {
                op_type: self.get_workers(op_type)
                for op_type in MAX_WORKERS_BY_TYPE.keys()
            },
        }


# Глобальная конфигурация (по умолчанию параллелизм включен)
_global_config: Optional[ParallelConfig] = None


def get_parallel_config() -> ParallelConfig:
    """
    Возвращает глобальную конфигурацию параллелизма.

    Returns:
        ParallelConfig instance
    """
    global _global_config
    if _global_config is None:
        _global_config = ParallelConfig(enabled=True)
    return _global_config


def set_parallel_config(config: ParallelConfig) -> None:
    """
    Устанавливает глобальную конфигурацию параллелизма.

    Args:
        config: Новая конфигурация
    """
    global _global_config
    _global_config = config
    logger.info(f"Parallel config updated: enabled={config.enabled}")


def enable_parallel(enabled: bool = True) -> None:
    """
    Включает или выключает параллелизм глобально.

    Args:
        enabled: True для включения, False для выключения
    """
    config = get_parallel_config()
    config.enabled = enabled
    logger.info(f"Parallel processing {'enabled' if enabled else 'disabled'}")

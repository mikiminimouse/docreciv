"""
Circuit Breaker - защита от cascade failures в preprocessing pipeline.

Реализует паттерн Circuit Breaker для graceful degradation:
- Закрыт (Closed): Нормальная обработка
- Открыт (Open): Блокировка при высокой частоте ошибок
- Полуоткрыт (Half-Open): Тестирование восстановления
"""

import time
import logging
from enum import Enum
from typing import Callable, Any, Optional, Dict, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .constants import (
    DEFAULT_CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    DEFAULT_CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SEC,
    CIRCUIT_BREAKER_HISTORY_SIZE,
    PIPELINE_CB_FILE_FAILURE_THRESHOLD,
    PIPELINE_CB_FILE_RECOVERY_TIMEOUT_SEC,
    PIPELINE_CB_CHUNK_FAILURE_THRESHOLD,
    PIPELINE_CB_CHUNK_RECOVERY_TIMEOUT_SEC,
    PIPELINE_CB_STAGE_FAILURE_THRESHOLD,
    PIPELINE_CB_STAGE_RECOVERY_TIMEOUT_SEC,
    PIPELINE_CB_EXTERNAL_FAILURE_THRESHOLD,
    PIPELINE_CB_EXTERNAL_RECOVERY_TIMEOUT_SEC,
)

logger = logging.getLogger(__name__)


class CircuitBreakerState(Enum):
    """Состояния circuit breaker."""
    CLOSED = "closed"      # Нормальная работа
    OPEN = "open"         # Защита активирована
    HALF_OPEN = "half_open"  # Тестирование восстановления


@dataclass
class CircuitBreakerMetrics:
    """Метрики circuit breaker."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    consecutive_failures: int = 0
    last_failure_time: Optional[float] = None
    state_changes: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Процент успешных вызовов."""
        if self.total_calls == 0:
            return 100.0
        return (self.successful_calls / self.total_calls) * 100

    @property
    def failure_rate(self) -> float:
        """Процент неудачных вызовов."""
        if self.total_calls == 0:
            return 0.0
        return (self.failed_calls / self.total_calls) * 100


class CircuitBreakerOpenException(Exception):
    """Исключение при открытом circuit breaker."""
    pass


class CircuitBreaker:
    """
    Circuit Breaker для защиты preprocessing pipeline.

    Предотвращает cascade failures путем временной блокировки
    обработки при высокой частоте ошибок.
    """

    def __init__(
        self,
        failure_threshold: int = DEFAULT_CIRCUIT_BREAKER_FAILURE_THRESHOLD,
        recovery_timeout: int = DEFAULT_CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SEC,
        expected_exception: Exception = Exception,
        name: str = "default"
    ):
        """
        Инициализация Circuit Breaker.

        Args:
            failure_threshold: Порог последовательных ошибок для открытия
            recovery_timeout: Время ожидания перед тестированием восстановления (сек)
            expected_exception: Тип исключений для отслеживания
            name: Имя circuit breaker для идентификации
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name

        # Состояние
        self.state = CircuitBreakerState.CLOSED
        self.metrics = CircuitBreakerMetrics()

        # Таймеры
        self.last_state_change = time.time()

        logger.info(f"🔧 CircuitBreaker '{name}' initialized (threshold={failure_threshold}, timeout={recovery_timeout}s)")

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Выполнить функцию через circuit breaker.

        Args:
            func: Функция для выполнения
            *args, **kwargs: Аргументы функции

        Returns:
            Результат выполнения функции

        Raises:
            CircuitBreakerOpenException: Если circuit breaker открыт
        """
        if self.state == CircuitBreakerState.OPEN:
            if not self._should_attempt_recovery():
                raise CircuitBreakerOpenException(
                    f"Circuit breaker '{self.name}' is OPEN (state: {self.state.value})"
                )
            else:
                # Переходим в half-open для тестирования
                self._set_state(CircuitBreakerState.HALF_OPEN)

        try:
            self.metrics.total_calls += 1

            # Выполняем функцию
            result = func(*args, **kwargs)

            # Успех - сбрасываем счетчик ошибок
            self._on_success()

            return result

        except self.expected_exception as e:
            # Ожидаемая ошибка - увеличиваем счетчик
            self._on_failure()
            raise e

        except Exception as e:
            # Неожиданная ошибка - тоже увеличиваем счетчик
            logger.warning(f"Unexpected exception in circuit breaker '{self.name}': {e}")
            self._on_failure()
            raise e

    def _on_success(self):
        """Обработка успешного выполнения."""
        self.metrics.successful_calls += 1
        self.metrics.consecutive_failures = 0

        # Если были в half-open, возвращаемся к closed
        if self.state == CircuitBreakerState.HALF_OPEN:
            self._set_state(CircuitBreakerState.CLOSED)
            logger.info(f"✅ Circuit breaker '{self.name}' recovered - back to CLOSED")

    def _on_failure(self):
        """Обработка ошибки выполнения."""
        self.metrics.failed_calls += 1
        self.metrics.consecutive_failures += 1
        self.metrics.last_failure_time = time.time()

        # Проверяем, нужно ли открыть circuit breaker
        if (self.state == CircuitBreakerState.CLOSED and
            self.metrics.consecutive_failures >= self.failure_threshold):
            self._set_state(CircuitBreakerState.OPEN)
            logger.warning(f"🚨 Circuit breaker '{self.name}' opened due to {self.metrics.consecutive_failures} consecutive failures")

    def _should_attempt_recovery(self) -> bool:
        """Проверить, пора ли пытаться восстановление."""
        if self.metrics.last_failure_time is None:
            return True

        elapsed = time.time() - self.metrics.last_failure_time
        return elapsed >= self.recovery_timeout

    def _set_state(self, new_state: CircuitBreakerState):
        """Изменить состояние circuit breaker."""
        if self.state == new_state:
            return

        old_state = self.state
        self.state = new_state
        self.last_state_change = time.time()

        # Записываем изменение состояния
        state_change = {
            "timestamp": datetime.now().isoformat(),
            "from_state": old_state.value,
            "to_state": new_state.value,
            "consecutive_failures": self.metrics.consecutive_failures,
            "total_calls": self.metrics.total_calls
        }
        self.metrics.state_changes.append(state_change)

        logger.info(f"🔄 Circuit breaker '{self.name}' state: {old_state.value} → {new_state.value}")

    def get_status(self) -> Dict[str, Any]:
        """
        Получить текущий статус circuit breaker.

        Returns:
            Подробный статус
        """
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
            "metrics": {
                "total_calls": self.metrics.total_calls,
                "successful_calls": self.metrics.successful_calls,
                "failed_calls": self.metrics.failed_calls,
                "consecutive_failures": self.metrics.consecutive_failures,
                "success_rate": self.metrics.success_rate,
                "failure_rate": self.metrics.failure_rate,
                "last_failure_time": self.metrics.last_failure_time
            },
            "state_changes": self.metrics.state_changes[-CIRCUIT_BREAKER_HISTORY_SIZE:],
            "time_since_last_change": time.time() - self.last_state_change,
            "can_attempt_recovery": self._should_attempt_recovery() if self.state == CircuitBreakerState.OPEN else None
        }

    def reset(self):
        """Сброс circuit breaker в начальное состояние."""
        self.state = CircuitBreakerState.CLOSED
        self.metrics = CircuitBreakerMetrics()
        self.last_state_change = time.time()

        logger.info(f"🔄 Circuit breaker '{self.name}' reset to initial state")

    def __str__(self) -> str:
        """Строковое представление."""
        return (f"CircuitBreaker('{self.name}', state={self.state.value}, "
                f"failures={self.metrics.consecutive_failures}/{self.failure_threshold}, "
                ".1f")


class PipelineCircuitBreaker:
    """
    Расширенный Circuit Breaker для preprocessing pipeline.

    Управляет несколькими circuit breakers для разных компонентов:
    - File processing
    - Chunk processing
    - Stage processing
    - External service calls
    """

    def __init__(self):
        self.breakers = {
            "file_processing": CircuitBreaker(
                failure_threshold=PIPELINE_CB_FILE_FAILURE_THRESHOLD,
                recovery_timeout=PIPELINE_CB_FILE_RECOVERY_TIMEOUT_SEC,
                name="file_processing"
            ),
            "chunk_processing": CircuitBreaker(
                failure_threshold=PIPELINE_CB_CHUNK_FAILURE_THRESHOLD,
                recovery_timeout=PIPELINE_CB_CHUNK_RECOVERY_TIMEOUT_SEC,
                name="chunk_processing"
            ),
            "stage_processing": CircuitBreaker(
                failure_threshold=PIPELINE_CB_STAGE_FAILURE_THRESHOLD,
                recovery_timeout=PIPELINE_CB_STAGE_RECOVERY_TIMEOUT_SEC,
                name="stage_processing"
            ),
            "external_calls": CircuitBreaker(
                failure_threshold=PIPELINE_CB_EXTERNAL_FAILURE_THRESHOLD,
                recovery_timeout=PIPELINE_CB_EXTERNAL_RECOVERY_TIMEOUT_SEC,
                name="external_calls"
            ),
        }

        logger.info("🔧 PipelineCircuitBreaker initialized with 4 breakers")

    def protect_file_processing(self, func: Callable, *args, **kwargs) -> Any:
        """Защитить обработку файла."""
        return self.breakers["file_processing"].call(func, *args, **kwargs)

    def protect_chunk_processing(self, func: Callable, *args, **kwargs) -> Any:
        """Защитить обработку чанка."""
        return self.breakers["chunk_processing"].call(func, *args, **kwargs)

    def protect_stage_processing(self, func: Callable, *args, **kwargs) -> Any:
        """Защитить обработку стадии."""
        return self.breakers["stage_processing"].call(func, *args, **kwargs)

    def protect_external_call(self, func: Callable, *args, **kwargs) -> Any:
        """Защитить внешний вызов."""
        return self.breakers["external_calls"].call(func, *args, **kwargs)

    def get_overall_status(self) -> Dict[str, Any]:
        """
        Получить общий статус всех circuit breakers.

        Returns:
            Сводный статус
        """
        status = {
            "timestamp": datetime.now().isoformat(),
            "overall_health": "healthy",
            "breakers": {},
            "summary": {
                "total_open": 0,
                "total_half_open": 0,
                "total_closed": 0
            }
        }

        for name, breaker in self.breakers.items():
            breaker_status = breaker.get_status()
            status["breakers"][name] = breaker_status

            if breaker_status["state"] == "open":
                status["summary"]["total_open"] += 1
            elif breaker_status["state"] == "half_open":
                status["summary"]["total_half_open"] += 1
            else:
                status["summary"]["total_closed"] += 1

        # Определяем overall health
        if status["summary"]["total_open"] > 0:
            status["overall_health"] = "critical"
        elif status["summary"]["total_half_open"] > 0:
            status["overall_health"] = "degraded"
        else:
            status["overall_health"] = "healthy"

        return status

    def reset_all(self):
        """Сбросить все circuit breakers."""
        for breaker in self.breakers.values():
            breaker.reset()
        logger.info("🔄 All circuit breakers reset")

    def __str__(self) -> str:
        """Строковое представление."""
        status = self.get_overall_status()
        return (f"PipelineCircuitBreaker(health={status['overall_health']}, "
                f"open={status['summary']['total_open']}, "
                f"half_open={status['summary']['total_half_open']}, "
                f"closed={status['summary']['total_closed']})")
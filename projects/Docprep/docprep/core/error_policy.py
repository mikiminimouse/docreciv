"""
Error Policy - политика обработки ошибок с retry логикой.

Поддерживает три политики:
- Retry: повторная попытка при временных сбоях
- Skip: пропуск безопасно игнорируемых случаев
- Quarantine: изоляция опасных/сломанных данных
"""
import time
import logging
from typing import Callable, Any, Dict, Optional, Type
from functools import wraps
from enum import Enum

from .exceptions import (
    PreprocessingError,
    StateTransitionError,
    ManifestError,
    OperationError,
    QuarantineError,
)

logger = logging.getLogger(__name__)


class ErrorPolicy(Enum):
    """Политика обработки ошибок."""
    RETRY = "retry"
    SKIP = "skip"
    QUARANTINE = "quarantine"


class RetryConfig:
    """Конфигурация для retry логики."""

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        retryable_exceptions: tuple = (OperationError,),
    ):
        """
        Инициализирует конфигурацию retry.

        Args:
            max_attempts: Максимальное количество попыток
            initial_delay: Начальная задержка в секундах
            max_delay: Максимальная задержка в секундах
            exponential_base: База для экспоненциальной задержки
            retryable_exceptions: Кортеж исключений, которые можно повторить
        """
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.retryable_exceptions = retryable_exceptions


def retry_on_error(
    config: Optional[RetryConfig] = None,
    on_retry: Optional[Callable[[Exception, int], None]] = None,
    on_final_failure: Optional[Callable[[Exception], None]] = None,
):
    """
    Декоратор для автоматического retry при ошибках.

    Args:
        config: Конфигурация retry
        on_retry: Callback при каждой попытке retry
        on_final_failure: Callback при финальной неудаче

    Returns:
        Декорированная функция
    """
    if config is None:
        config = RetryConfig()

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            delay = config.initial_delay

            for attempt in range(1, config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except config.retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt < config.max_attempts:
                        logger.warning(
                            f"Attempt {attempt}/{config.max_attempts} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        
                        if on_retry:
                            try:
                                on_retry(e, attempt)
                            except Exception:
                                pass
                        
                        time.sleep(delay)
                        delay = min(delay * config.exponential_base, config.max_delay)
                    else:
                        logger.error(
                            f"All {config.max_attempts} attempts failed for {func.__name__}: {e}"
                        )
                        if on_final_failure:
                            try:
                                on_final_failure(e)
                            except Exception:
                                pass
                        raise
                except Exception as e:
                    # Неповторяемые исключения пробрасываем сразу
                    raise

            # Не должно быть достигнуто, но на всякий случай
            if last_exception:
                raise last_exception

        return wrapper
    return decorator


def apply_error_policy(
    error: Exception,
    unit_id: Optional[str] = None,
    policy: ErrorPolicy = ErrorPolicy.RETRY,
) -> Dict[str, Any]:
    """
    Применяет политику обработки ошибки.

    Args:
        error: Исключение
        unit_id: Идентификатор UNIT (опционально)
        policy: Политика обработки

    Returns:
        Словарь с результатом обработки:
        - action: действие (retry, skip, quarantine)
        - should_retry: нужно ли повторить
        - should_skip: нужно ли пропустить
        - should_quarantine: нужно ли изолировать
        - message: сообщение
    """
    result = {
        "action": policy.value,
        "should_retry": False,
        "should_skip": False,
        "should_quarantine": False,
        "message": str(error),
        "error_type": type(error).__name__,
    }

    # Определяем политику на основе типа ошибки
    if isinstance(error, QuarantineError):
        result["action"] = ErrorPolicy.QUARANTINE.value
        result["should_quarantine"] = True
        result["message"] = f"Quarantine required: {error.reason or str(error)}"
    elif isinstance(error, StateTransitionError):
        # Ошибки переходов не повторяем
        result["action"] = ErrorPolicy.SKIP.value
        result["should_skip"] = True
        result["message"] = f"Invalid state transition: {error}"
    elif isinstance(error, ManifestError):
        # Ошибки manifest могут быть временными (файл заблокирован)
        if policy == ErrorPolicy.RETRY:
            result["should_retry"] = True
        else:
            result["should_skip"] = True
    elif isinstance(error, OperationError):
        # Операционные ошибки могут быть временными
        if policy == ErrorPolicy.RETRY:
            result["should_retry"] = True
        else:
            result["should_skip"] = True
    else:
        # Неизвестные ошибки пропускаем
        result["action"] = ErrorPolicy.SKIP.value
        result["should_skip"] = True
        result["message"] = f"Unknown error: {error}"

    return result


def handle_operation_error(
    func: Callable,
    *args,
    error_policy: ErrorPolicy = ErrorPolicy.RETRY,
    retry_config: Optional[RetryConfig] = None,
    **kwargs,
) -> Any:
    """
    Выполняет операцию с применением политики обработки ошибок.

    Args:
        func: Функция для выполнения
        *args: Аргументы функции
        error_policy: Политика обработки ошибок
        retry_config: Конфигурация retry
        **kwargs: Ключевые аргументы функции

    Returns:
        Результат выполнения функции

    Raises:
        PreprocessingError: Если операция не удалась после всех попыток
    """
    if retry_config is None:
        retry_config = RetryConfig()

    last_error = None
    delay = retry_config.initial_delay

    for attempt in range(1, retry_config.max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_error = e
            policy_result = apply_error_policy(e, policy=error_policy)

            if policy_result["should_quarantine"]:
                # Немедленно изолируем
                logger.error(f"Quarantine required for operation {func.__name__}: {e}")
                raise QuarantineError(
                    f"Operation requires quarantine: {e}",
                    reason=policy_result["message"],
                )

            if policy_result["should_skip"]:
                # Пропускаем операцию
                logger.warning(f"Skipping operation {func.__name__}: {e}")
                return None

            if policy_result["should_retry"] and attempt < retry_config.max_attempts:
                # Повторяем попытку
                logger.warning(
                    f"Retry {attempt}/{retry_config.max_attempts} for {func.__name__}: {e}"
                )
                time.sleep(delay)
                delay = min(delay * retry_config.exponential_base, retry_config.max_delay)
            else:
                # Финальная неудача
                logger.error(f"Operation {func.__name__} failed after {attempt} attempts: {e}")
                raise

    # Не должно быть достигнуто
    if last_error:
        raise last_error


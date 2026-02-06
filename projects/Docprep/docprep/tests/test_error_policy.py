"""
Unit тесты для error_policy.
"""
import pytest
import time

from docprep.core.error_policy import (
    ErrorPolicy,
    RetryConfig,
    retry_on_error,
    apply_error_policy,
    handle_operation_error,
)
from docprep.core.exceptions import (
    OperationError,
    QuarantineError,
    StateTransitionError,
)


def test_retry_config_defaults():
    """Тест конфигурации retry по умолчанию."""
    config = RetryConfig()
    
    assert config.max_attempts == 3
    assert config.initial_delay == 1.0
    assert config.max_delay == 60.0
    assert OperationError in config.retryable_exceptions


def test_retry_on_error_success():
    """Тест успешного выполнения с retry декоратором."""
    @retry_on_error()
    def successful_operation():
        return "success"
    
    result = successful_operation()
    assert result == "success"


def test_retry_on_error_retries():
    """Тест повторных попыток при ошибках."""
    call_count = [0]
    
    @retry_on_error(config=RetryConfig(max_attempts=3, initial_delay=0.1))
    def failing_operation():
        call_count[0] += 1
        if call_count[0] < 3:
            raise OperationError("Temporary error", operation="test")
        return "success"
    
    result = failing_operation()
    assert result == "success"
    assert call_count[0] == 3


def test_retry_on_error_final_failure():
    """Тест финальной неудачи после всех попыток."""
    @retry_on_error(config=RetryConfig(max_attempts=2, initial_delay=0.1))
    def always_failing_operation():
        raise OperationError("Always fails", operation="test")
    
    with pytest.raises(OperationError):
        always_failing_operation()


def test_apply_error_policy_quarantine():
    """Тест политики quarantine."""
    error = QuarantineError("Dangerous file", reason="zip_bomb")
    result = apply_error_policy(error, policy=ErrorPolicy.QUARANTINE)
    
    assert result["should_quarantine"] is True
    assert result["action"] == "quarantine"


def test_apply_error_policy_skip():
    """Тест политики skip."""
    error = StateTransitionError("Invalid transition", current_state="RAW", target_state="READY")
    result = apply_error_policy(error, policy=ErrorPolicy.SKIP)
    
    assert result["should_skip"] is True
    assert result["action"] == "skip"


def test_apply_error_policy_retry():
    """Тест политики retry."""
    error = OperationError("Temporary error", operation="convert")
    result = apply_error_policy(error, policy=ErrorPolicy.RETRY)
    
    assert result["should_retry"] is True
    assert result["action"] == "retry"


def test_handle_operation_error_success():
    """Тест успешного выполнения операции."""
    def successful_func():
        return "result"
    
    result = handle_operation_error(successful_func, error_policy=ErrorPolicy.RETRY)
    assert result == "result"


def test_handle_operation_error_quarantine():
    """Тест изоляции при QuarantineError."""
    def quarantine_func():
        raise QuarantineError("Dangerous", reason="zip_bomb")
    
    with pytest.raises(QuarantineError):
        handle_operation_error(quarantine_func, error_policy=ErrorPolicy.RETRY)


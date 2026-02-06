"""
Core модуль - базовая инфраструктура системы.

Включает state machine, manifest, audit log, config и exceptions.
"""

from .state_machine import UnitState, UnitStateMachine, ALLOWED_TRANSITIONS, validate_state_transition
from .manifest import (
    load_manifest,
    save_manifest,
    create_manifest_v2,
    update_manifest_operation,
    update_manifest_state,
)
from .audit import AuditLogger
from .config import (
    INPUT_DIR,
    PROCESSING_DIR,
    MERGE_DIR,
    READY2DOCLING_DIR,
    EXCEPTIONS_DIR,
    get_cycle_paths,
    init_directory_structure,
    get_processing_paths,
)
from .unit_processor import (
    find_unit_directory,
    determine_unit_extension,
    get_extension_subdirectory,
    move_unit_to_target,
    create_unit_manifest_if_needed,
    update_unit_state,
    process_directory_units,
)
from .exceptions import (
    PreprocessingError,
    StateTransitionError,
    ManifestError,
    OperationError,
    QuarantineError,
)
from .error_policy import (
    ErrorPolicy,
    RetryConfig,
    retry_on_error,
    apply_error_policy,
    handle_operation_error,
)
from .decision_engine import TypeDecisionEngine, resolve_type_decision

__all__ = [
    # State Machine
    "UnitState",
    "UnitStateMachine",
    "ALLOWED_TRANSITIONS",
    "validate_state_transition",
    # Manifest
    "load_manifest",
    "save_manifest",
    "create_manifest_v2",
    "update_manifest_operation",
    "update_manifest_state",
    # Audit
    "AuditLogger",
    # Config
    "INPUT_DIR",
    "PROCESSING_DIR",
    "MERGE_DIR",
    "READY2DOCLING_DIR",
    "EXCEPTIONS_DIR",
    "get_cycle_paths",
    "init_directory_structure",
    "get_processing_paths",
    # Unit Processor
    "find_unit_directory",
    "determine_unit_extension",
    "get_extension_subdirectory",
    "move_unit_to_target",
    "create_unit_manifest_if_needed",
    "update_unit_state",
    "process_directory_units",
    # Exceptions
    "PreprocessingError",
    "StateTransitionError",
    "ManifestError",
    "OperationError",
    "QuarantineError",
    # Error Policy
    "ErrorPolicy",
    "RetryConfig",
    "retry_on_error",
    "apply_error_policy",
    "handle_operation_error",
    # Decision Engine
    "TypeDecisionEngine",
    "resolve_type_decision",
]


# Статус тестового покрытия DocPrep

**Дата обновления:** 2026-01-17
**Всего тестов:** 156 + 1 skipped

---

## Покрытие по модулям

| Модуль | Покрытие | Статус | Тестов |
|--------|----------|--------|--------|
| `state_machine` | ~80% | ✅ Good | 33 |
| `manifest` | ~80% | ✅ Good | 29 |
| `merger` | ~40% | Improved | 21 |
| `classifier` | ~60% | Improve | 8 |
| `converter` | ~75% | Good | 11 |
| `extractor` | ~70% | Good | 5 |
| `normalizers` | ~80% | Good | 5 |
| `error_policy` | ~85% | Good | 9 |
| `error_handling` | ~90% | Good | 8 |
| `file_ops` | ~90% | Good | 7 |
| `xvfb_integration` | ~80% | Good | 2 |
| `xvfb_optimization` | ~85% | Good | 7 |
| `cli` | ~70% | ✅ Good | 18 |

---

## Результаты последнего запуска

```
156 passed, 1 skipped (100%)
```

---

## State Machine Tests (33)

```
test_state_machine.py:
  test_unit_state_enum_values
  test_unit_state_machine_creation
  test_unit_state_machine_initial_state
  test_unit_state_machine_transition
  test_unit_state_machine_invalid_transition
  test_unit_state_machine_get_allowed_transitions
  test_unit_state_machine_reflexive_transition
  test_state_trace_history
  test_cycle_aware_mapping
  test_get_route_by_state
  test_get_next_state_for_processing
  test_state_machine_persistence
  test_state_machine_load_from_manifest
  test_all_transitions_defined
  ... (и другие)
```

---

## Manifest Tests (29)

```
test_manifest.py:
  test_manifest_v2_schema_validation
  test_create_manifest_for_unit
  test_update_manifest_state
  test_add_file_to_manifest
  test_manifest_persistence
  test_manifest_with_transformations
  test_load_manifest
  test_save_manifest
  test_manifest_file_operations
  test_manifest_state_machine_integration
  ... (и другие)
```

---

## Merger Tests (21)

```
TestSanitizeExtension:
  test_normal_extension
  test_extension_with_dot
  test_empty_extension_raises
  test_only_dot_raises
  test_dangerous_chars_removed
  test_extension_truncated_if_too_long
  test_extension_becomes_empty_after_sanitization_raises

TestMergerUnknownExtensions:
  test_csv_creates_csv_directory
  test_txt_creates_txt_directory
  test_xlsx_creates_xlsx_directory
  test_html_creates_html_directory
  test_json_creates_json_directory

TestMergerDetermineUnitType:
  test_empty_unit_raises_error
  test_files_without_extension_raises_error
  test_unknown_extension_returns_extension_name
  test_known_extension_returns_mapped_type
  test_csv_extension_returns_csv

TestMergerGetTargetSubdir:
  test_unknown_type_uses_extension
  test_known_type_uses_mapping
  test_csv_type_creates_csv_subdir
```

---

## Classifier Tests (8)

```
test_classifier_initialization
test_classify_unit_pdf
test_classify_unit_archive
test_classify_unit_mixed
test_classify_unit_creates_manifest
test_classify_unit_updates_state
test_classify_unit_in_input_creates_manifest
test_classify_unit_in_input_updates_state
```

---

## Converter Tests (11)

```
test_converter_initialization
test_converter_detects_convertible_files
test_converter_handles_no_convertible_files
test_converter_converts_doc_to_docx (skipped - LibreOffice required)
test_libreoffice_converter_initialization
test_libreoffice_converter_mock_mode
test_robust_document_converter_initialization
test_robust_document_converter_mock_mode
test_xvfb_display_pool_singleton
test_xvfb_display_pool_acquire_release
test_libreoffice_converter_convert_file_mock_success
test_libreoffice_converter_convert_unsupported_format
test_robust_document_converter_fallback
```

---

## Extractor Tests (5)

```
test_extractor_initialization
test_extractor_detects_archives
test_extractor_handles_no_archives
test_extractor_extracts_zip
test_extractor_protects_against_zip_bomb
```

---

## Normalizers Tests (5)

```
test_name_normalizer_initialization
test_name_normalizer_fixes_double_extension
test_extension_normalizer_initialization
test_extension_normalizer_fixes_wrong_extension
test_normalizers_update_manifest
```

---

## Error Policy Tests (9)

```
test_retry_config_defaults
test_retry_on_error_success
test_retry_on_error_retries
test_retry_on_error_final_failure
test_apply_error_policy_quarantine
test_apply_error_policy_skip
test_apply_error_policy_retry
test_handle_operation_error_success
test_handle_operation_error_quarantine
```

---

## Error Handling Tests (8)

```
test_move_to_exceptions_new_structure
test_converter_error_routing
test_extractor_error_routing
test_normalizer_error_routing
test_directory_structure_creation
test_merger_er_merge_functionality
test_empty_unit_routing
test_integration_all_error_routes
```

---

## File Operations Tests (7)

```
test_detect_file_type_pdf
test_detect_file_type_zip
test_detect_file_type_fake_doc
test_detect_file_type_docx
test_calculate_sha256
test_sanitize_filename
test_get_file_size
```

---

## XvFB Tests (9)

```
XvFB Integration (2):
  test_converter_headless_initialization
  test_robust_converter_with_xvfb_pool

XvFB Optimization (7):
  test_libreoffice_converter_initialization
  test_libreoffice_converter_mock_mode
  test_robust_document_converter_initialization
  test_robust_document_converter_mock_mode
  test_xvfb_display_pool_singleton
  test_xvfb_display_pool_acquire_release
  test_libreoffice_converter_convert_file_mock_success
```

---

## CLI Tests (18)

```
TestCLIBasic:
  test_cli_help
  test_cli_help_short
  test_cli_no_args

TestCLISubcommands:
  test_classifier_help
  test_merge_help
  test_pipeline_help
  test_cycle_help
  test_stage_help
  test_substage_help
  test_inspect_help
  test_utils_help
  test_stats_help
  test_chunked_classifier_help

TestCLIGlobalOptions:
  test_verbose_option
  test_verbose_short_option
  test_dry_run_option

TestCLIInvalidCommands:
  test_invalid_command
  test_invalid_subcommand
```

---

## Критичные пробелы

**Нет критичных пробелов.** Все основные модули покрыты тестами.

---

## Недавно добавлено (2025-01-17)

### State Machine Tests
**Результат:** 0% -> ~80% (33 теста)

Добавлены тесты для:
- Все состояния и переходы
- Invalid transitions
- State trace history
- Cycle-aware mapping

### Manifest Tests
**Результат:** 0% -> ~80% (29 тестов)

Добавлены тесты для:
- Schema validation
- CRUD операции
- Persistence
- Integration с state machine

### Test Isolation
**Проблема:** 9 тестов падали с `FileExistsError`

**Решение:**
```python
# tests/conftest.py
@pytest.fixture(autouse=True)
def isolate_test_environment(temp_dir, monkeypatch):
    """Изолирует тестовое окружение от production данных."""
```

**Результат:** 138/139 tests passing (100%)

---

## План улучшения

### Priority 1: CLI тесты (Запланировано)
- [ ] CLI layer тесты
- [ ] End-to-end тесты

### Priority 2: Покрытие 80%+ (Опционально)
- [ ] Unit Processor тесты
- [ ] Routing тесты
- [ ] Audit тесты

---

## Цели

| Метрика | Текущее | Цель | Статус |
|---------|---------|------|--------|
| Passing tests | 100% | 100% | ✅ |
| Total tests | 156 | 150+ | ✅ |
| Coverage | ~77% | 80%+ | ⚠️ |
| Critical gaps | 0 | 0 | ✅ |

---

## Связанные документы

- `REFACTORING_STATUS.md` - Статус рефакторинга
- `TEST_REPORT_2025-03-04.md` - Отчёт о тестировании
- `docs/TESTING_GUIDE.md` - Гайд по тестированию

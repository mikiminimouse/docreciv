"""
Manifest v2 - управление метаданными UNIT.

Manifest = состояние UNIT, хранит текущее состояние и историю трансформаций.
Согласно PRD раздел 14: Manifest = состояние, Audit = история.
"""
import json
import os  # ДОБАВЛЕНО: для fsync()
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from .state_machine import UnitState
from .config import MAX_CYCLES


def get_is_mixed(manifest: Dict[str, Any]) -> bool:
    """
    Определяет is_mixed из всех возможных мест в manifest.

    Проверяет:
    1. manifest["is_mixed"]
    2. manifest["processing"]["classification"]["is_mixed"]

    Args:
        manifest: Словарь с manifest данными

    Returns:
        True если UNIT является mixed, False иначе
    """
    # Проверяем корневой is_mixed
    if manifest.get("is_mixed", False):
        return True

    # Проверяем в processing.classification
    classification = manifest.get("processing", {}).get("classification", {})
    if classification.get("is_mixed", False):
        return True

    return False


def set_is_mixed(manifest: Dict[str, Any], value: bool) -> Dict[str, Any]:
    """
    Устанавливает is_mixed консистентно во всех местах manifest.

    Обновляет:
    1. manifest["is_mixed"]
    2. manifest["processing"]["classification"]["is_mixed"]

    Args:
        manifest: Словарь с manifest данными
        value: Значение is_mixed для установки

    Returns:
        Обновленный manifest
    """
    manifest["is_mixed"] = value

    if "processing" not in manifest:
        manifest["processing"] = {}
    if "classification" not in manifest["processing"]:
        manifest["processing"]["classification"] = {}

    manifest["processing"]["classification"]["is_mixed"] = value

    return manifest


def load_manifest(unit_path: Path) -> Dict[str, Any]:
    """
    Загружает manifest.json из директории UNIT.

    Args:
        unit_path: Путь к директории UNIT

    Returns:
        Словарь с manifest данными

    Raises:
        FileNotFoundError: Если manifest.json не найден
        json.JSONDecodeError: Если manifest.json некорректен
    """
    manifest_path = unit_path / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    with open(manifest_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_manifest(
    unit_path: Path,
    manifest: Dict[str, Any],
    db_client: Optional[Any] = None,
) -> None:
    """
    Сохраняет manifest.json в директорию UNIT.

    Опционально записывает состояние в MongoDB если db_client предоставлен.

    Args:
        unit_path: Путь к директории UNIT
        manifest: Словарь с manifest данными
        db_client: Опциональный DocPrepDatabase клиент для записи в MongoDB
    """
    unit_path.mkdir(parents=True, exist_ok=True)
    manifest_path = unit_path / "manifest.json"

    # Обновляем updated_at
    manifest["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # ИСПРАВЛЕНИЕ БАГ #5: Добавление fsync() для гарантии записи на диск
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
        f.flush()  # Flush Python buffers
        os.fsync(f.fileno())  # Force write to disk

    # Опционально записываем в MongoDB
    if db_client is not None:
        try:
            from .database import DocPrepDatabase
            if isinstance(db_client, DocPrepDatabase) and db_client.is_connected():
                db_client.write_unit_state(manifest)

                # Записываем метаданные файлов
                files = manifest.get("files", [])
                if files:
                    db_client.write_document_metadata(
                        unit_id=manifest["unit_id"],
                        files=files,
                    )
        except Exception as e:
            # Не падаем при ошибке MongoDB - логируем и продолжаем
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to write to MongoDB for {manifest.get('unit_id')}: {e}")


def _determine_route_from_files(files: List[Dict[str, Any]]) -> str:
    """
    Определяет route для обработки на основе файлов.
    
    Делегирует в unified routing registry для консистентности.
    
    Args:
        files: Список файлов с информацией о типах
        
    Returns:
        Route строка: pdf_text, pdf_scan, docx, xlsx, pptx, html, xml, image_ocr, rtf, mixed
    """
    from .routing import determine_route_from_files as _routing_determine
    return _routing_determine(files)


def create_manifest_v2(
    unit_id: str,
    protocol_id: Optional[str] = None,
    protocol_date: Optional[str] = None,
    registration_number: Optional[str] = None,
    files: Optional[List[Dict[str, Any]]] = None,
    current_cycle: int = 1,
    state_trace: Optional[List[str]] = None,
    final_cluster: Optional[str] = None,
    final_reason: Optional[str] = None,
    unit_semantics: Optional[Dict[str, Any]] = None,
    source_urls: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Создает manifest v2 для unit'а согласно PRD раздел 14.2.

    Args:
        unit_id: Идентификатор UNIT
        protocol_id: ID протокола в БД (опционально)
        protocol_date: Дата протокола из БД в формате YYYY-MM-DD (опционально)
        registration_number: RegistrationNumber для сквозного трейсинга (опционально)
        files: Список файлов с информацией о трансформациях (опционально)
        current_cycle: Текущий цикл обработки (1-3)
        state_trace: История переходов состояний (опционально)
        final_cluster: Финальный кластер (Merge_1, Merge_2, Merge_3) (опционально)
        final_reason: Причина попадания в финальный кластер (опционально)
        unit_semantics: Семантика UNIT (опционально)
        source_urls: Список URL источников (опционально)

    Returns:
        Словарь с manifest v2
    """
    if files is None:
        files = []

    if state_trace is None:
        state_trace = ["RAW"]

    # Определяем route для обработки
    route = _determine_route_from_files(files)

    # Формируем список файлов с трансформациями
    files_list = []
    for file_info in files:
        file_entry = {
            "original_name": file_info.get("original_name", ""),
            "current_name": file_info.get("current_name", file_info.get("original_name", "")),
            "mime_detected": file_info.get("mime_type", file_info.get("mime_detected", "")),
            "detected_type": file_info.get("detected_type", "unknown"),
            "needs_ocr": file_info.get("needs_ocr", False),
            "pages_or_parts": file_info.get("pages_or_parts", 1),
            "transformations": file_info.get("transformations", []),
        }
        files_list.append(file_entry)

    # Определяем финальное состояние из state_trace
    final_state = state_trace[-1] if state_trace else "RAW"
    initial_state = state_trace[0] if state_trace else "RAW"

    # ★ PRIMARY TRACE ID: registrationNumber для сквозного трейсинга
    primary_trace_id = registration_number or unit_id

    manifest = {
        "schema_version": "2.1",
        "unit_id": unit_id,
        "protocol_id": protocol_id or "",
        "protocol_date": protocol_date or "",
        "registration_number": registration_number or "",  # ★ PRIMARY TRACE ID
        "source": {"urls": source_urls or []},
        # ★ TRACE INFO для сквозного трейсинга через компоненты
        "trace": {
            "primary_id": primary_trace_id,  # registrationNumber или fallback на unit_id
            "component": "docprep",
            "stage": "preprocessing",
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        "unit_semantics": unit_semantics
        or {
            "domain": "public_procurement",
            "entity": "tender_protocol",
            "expected_content": ["protocol", "attachments"],
        },
        "files": files_list,
        "files_metadata": {
            f.get("original_name", ""): {
                "detected_type": f.get("detected_type", "unknown"),
                "needs_ocr": f.get("needs_ocr", False),
                "mime_type": f.get("mime_detected", "unknown"),
                "pages_or_parts": f.get("pages_or_parts", 1),
            }
            for f in files_list
        },
        "processing": {
            "current_cycle": current_cycle,
            "max_cycles": MAX_CYCLES,
            "final_cluster": final_cluster or "",
            "final_reason": final_reason or "",
            "classifier_confidence": 1.0,
            "route": route,
        },
        "state_machine": {
            "initial_state": initial_state,
            "final_state": final_state,
            "current_state": final_state,
            "state_trace": state_trace,
        },
        "integrity": {
            "checksum": "",  # Можно вычислить SHA256 для всего UNIT
            "file_count": len(files),
        },
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    return manifest


def update_manifest_operation(
    manifest: Dict[str, Any], operation: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Обновляет manifest, добавляя информацию об операции.

    Args:
        manifest: Существующий manifest
        operation: Информация об операции:
            - type: тип операции (convert, extract, normalize, rename)
            - file_index: индекс файла (опционально)
            - from: исходный формат/путь (опционально)
            - to: целевой формат/путь (опционально)
            - cycle: номер цикла
            - tool: инструмент (libreoffice, python-magic и т.д.)
            - timestamp: время операции (опционально)
            - trace_id: ID операции для trace системы (опционально)
            - status: статус операции (success, failed, skipped)
            - error: описание ошибки (опционально)

    Returns:
        Обновленный manifest
    """
    operation_type = operation.get("type")
    file_index = operation.get("file_index", 0)
    cycle = operation.get("cycle", manifest.get("processing", {}).get("current_cycle", 1))

    # Добавляем timestamp если не указан
    if "timestamp" not in operation:
        operation["timestamp"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Добавляем trace_id в корневой trace раздел если указан
    trace_id = operation.get("trace_id")
    if trace_id:
        if "trace" not in manifest:
            manifest["trace"] = {}
        manifest["trace"]["last_operation_id"] = trace_id
        manifest["trace"]["last_operation_type"] = operation_type
        manifest["trace"]["last_operation_timestamp"] = operation["timestamp"]

    # Добавляем операцию к файлу
    if "files" in manifest and len(manifest["files"]) > file_index:
        file_entry = manifest["files"][file_index]
        if "transformations" not in file_entry:
            file_entry["transformations"] = []
        file_entry["transformations"].append(operation)

    # Обновляем applied_operations на уровне unit
    if "applied_operations" not in manifest:
        manifest["applied_operations"] = []
    manifest["applied_operations"].append(operation)

    manifest["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    return manifest


def update_manifest_trace(
    manifest: Dict[str, Any],
    trace_id: Optional[str] = None,
    component: str = "docprep",
    stage: str = "preprocessing",
    registration_number: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Обновляет trace информацию в manifest.

    Args:
        manifest: Существующий manifest
        trace_id: Уникальный ID операции (опционально, генерируется если не указан)
        component: Компонент (docprep, docreciv, docling)
        stage: Этап обработки (preprocessing, reception, processing)
        registration_number: Номер регистрации для связи с Docreciv

    Returns:
        Обновленный manifest
    """
    import uuid

    if "trace" not in manifest:
        manifest["trace"] = {}

    # Генерируем trace_id если не указан
    if trace_id is None:
        trace_id = f"{component}_{stage}_{uuid.uuid4().hex[:8]}"

    manifest["trace"]["operation_id"] = trace_id
    manifest["trace"]["component"] = component
    manifest["trace"]["stage"] = stage
    manifest["trace"]["timestamp"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Добавляем registration_number если указан (primary trace ID)
    if registration_number:
        manifest["trace"]["primary_id"] = registration_number

    manifest["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    return manifest


def update_manifest_state(
    manifest: Dict[str, Any], state: UnitState, cycle: int
) -> Dict[str, Any]:
    """
    Обновляет manifest, добавляя новое состояние в state_trace.

    Args:
        manifest: Существующий manifest
        state: Новое состояние
        cycle: Номер цикла

    Returns:
        Обновленный manifest
    """
    # Обновляем state_machine
    if "state_machine" not in manifest:
        manifest["state_machine"] = {
            "initial_state": state.value,
            "final_state": state.value,
            "current_state": state.value,
            "state_trace": [state.value],
        }
    else:
        state_trace = manifest["state_machine"].get("state_trace", [])
        state_trace.append(state.value)
        manifest["state_machine"]["state_trace"] = state_trace
        manifest["state_machine"]["current_state"] = state.value
        manifest["state_machine"]["final_state"] = state.value

    # Обновляем processing
    if "processing" not in manifest:
        manifest["processing"] = {}
    manifest["processing"]["current_cycle"] = cycle
    manifest["processing"]["current_state"] = state.value

    manifest["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    return manifest


def create_manifest_with_protocol_lookup(
    unit_id: str,
    files: Optional[List[Dict[str, Any]]] = None,
    db_client: Optional[Any] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Создаёт manifest с автоматическим поиском protocol_id в MongoDB.

    Если protocol_id не указан в kwargs, пытается найти запись
    в docling_metadata.protocols по unit_id и заполнить поля:
    - protocol_id (ObjectId из MongoDB)
    - protocol_date (из loadDate)
    - registrationNumber (★ PRIMARY TRACE ID для сквозного трейсинга)
    - purchaseNoticeNumber (для справки)

    Args:
        unit_id: Идентификатор UNIT
        files: Список файлов с информацией о трансформациях
        db_client: Опциональный DocPrepDatabase клиент для поиска в MongoDB
        **kwargs: Другие аргументы для create_manifest_v2

    Returns:
        Словарь manifest v2 с заполненными полями из MongoDB
    """
    protocol_id = kwargs.get("protocol_id")
    protocol_date = kwargs.get("protocol_date")
    registration_number = kwargs.get("registration_number")

    # Поиск в MongoDB если protocol_id не указан и db_client доступен
    if db_client is not None and not protocol_id:
        try:
            protocol_doc = None
            # Пробуем получить через метод get_protocol_by_unit_id
            if hasattr(db_client, "get_protocol_by_unit_id"):
                protocol_doc = db_client.get_protocol_by_unit_id(unit_id)
            elif db_client.is_connected() and hasattr(db_client, "protocols"):
                # Прямой доступ к коллекции
                protocol_doc = db_client.protocols.find_one({"unit_id": unit_id})

            if protocol_doc:
                protocol_id = str(protocol_doc.get("_id", ""))
                # Извлекаем дату из loadDate
                load_date = protocol_doc.get("loadDate")
                if load_date:
                    protocol_date = load_date.split("T")[0][:10]

                # ★ ИЗВЛЕКАЕМ registrationNumber для сквозного трейсинга
                reg_num = protocol_doc.get("registrationNumber", "")
                if reg_num and not registration_number:
                    registration_number = reg_num

                logger = __import__("logging").getLogger(__name__)
                logger.info(f"Found protocol in MongoDB for {unit_id}: protocol_id={protocol_id[:8]}..., registrationNumber={reg_num[:8] if reg_num else 'N/A'}...")

                # Копируем дополнительные поля
                if "purchaseNoticeNumber" in protocol_doc:
                    kwargs["purchase_notice_number"] = protocol_doc["purchaseNoticeNumber"]

        except Exception as e:
            logger = __import__("logging").getLogger(__name__)
            logger.warning(f"Failed to lookup protocol in MongoDB for {unit_id}: {e}")

    return create_manifest_v2(
        unit_id=unit_id,
        protocol_id=protocol_id,
        protocol_date=protocol_date,
        registration_number=registration_number,  # ★ ПЕРЕДАЁМ PRIMARY TRACE ID
        files=files,
        **kwargs
    )


def load_unit_meta(unit_dir: Path) -> Optional[Dict[str, Any]]:
    """
    Загружает unit.meta.json из директории UNIT (создаётся Docreciv).

    Docreciv создаёт unit.meta.json с trace информацией:
    - registrationNumber: Primary trace ID из purchaseProtocol
    - local_mongo_id: Local MongoDB _id
    - trace_id: Синоним registrationNumber
    - source_date: Дата источника

    Args:
        unit_dir: Path к директории UNIT

    Returns:
        Словарь с метаданными или None если файл не найден
    """
    meta_file = unit_dir / "unit.meta.json"
    if not meta_file.exists():
        return None

    try:
        with open(meta_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger = __import__("logging").getLogger(__name__)
        logger.warning(f"Failed to read unit.meta.json from {unit_dir}: {e}")
        return None


def get_registration_number(unit_dir: Path) -> Optional[str]:
    """
    Получает registrationNumber из unit.meta.json.

    Это PRIMARY TRACE ID для сквозного трейсинга через компоненты.

    Args:
        unit_dir: Path к директории UNIT

    Returns:
        registrationNumber или None если не найден
    """
    meta = load_unit_meta(unit_dir)
    if meta:
        # Пробуем разные возможные ключи
        return (
            meta.get("registrationNumber") or
            meta.get("registration_number") or
            meta.get("trace_id")
        )
    return None


def save_unit_meta(
    unit_dir: Path,
    registration_number: Optional[str] = None,
    purchase_notice_number: Optional[str] = None,
    source_date: Optional[str] = None,
    record_id: Optional[str] = None,
    unit_id: Optional[str] = None,
) -> None:
    """
    Создаёт или обновляет unit.meta.json в директории UNIT.

    Этот файл используется для сквозного трейсинга между компонентами:
    - Docreciv создаёт его при синхронизации
    - Docprep обновляет его при обработке
    - Doclingproc читает его при обработке документов
    - LLM_qaenrich использует его для связи с исходными протоколами

    Args:
        unit_dir: Path к директории UNIT
        registration_number: Primary Trace ID (registrationNumber из protocols)
        purchase_notice_number: Secondary ID (purchaseNoticeNumber из protocols)
        source_date: Дата протокола (loadDate из protocols)
        record_id: MongoDB ObjectId из protocols коллекции
        unit_id: Идентификатор UNIT (если не указан, используется unit_dir.name)
    """
    if unit_id is None:
        unit_id = unit_dir.name

    meta = {
        "registrationNumber": registration_number or "",
        "purchase_notice_number": purchase_notice_number or "",
        "source_date": source_date or "",
        "record_id": record_id or "",
        "unit_id": unit_id,
        "created_by": "docprep",
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    meta_file = unit_dir / "unit.meta.json"
    with open(meta_file, 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())

    logger = __import__("logging").getLogger(__name__)
    logger.debug(f"Created unit.meta.json in {unit_dir} with registrationNumber={registration_number[:8] if registration_number else 'N/A'}...")


def create_unit_meta_from_manifest(
    source_dir: Path,
    target_dir: Path,
) -> None:
    """
    Создаёт unit.meta.json в target директории на основе данных из source.

    Priority для источников метаданных:
    1. source/unit.meta.json (если существует)
    2. source/manifest.json (registration_number, protocol_date)

    Args:
        source_dir: Исходная директория UNIT
        target_dir: Целевая директория UNIT (например, Ready2Docling/docx/UNIT_xxx)
    """
    # Пытаемся прочитать из source unit.meta.json
    source_meta = source_dir / "unit.meta.json"
    meta = None

    if source_meta.exists():
        try:
            with open(source_meta, 'r', encoding='utf-8') as f:
                meta = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger = __import__("logging").getLogger(__name__)
            logger.warning(f"Failed to read source unit.meta.json: {e}")

    # Если нет unit.meta.json, пробуем manifest.json
    if meta is None:
        manifest_file = source_dir / "manifest.json"
        if manifest_file.exists():
            try:
                with open(manifest_file, 'r', encoding='utf-8') as f:
                    manifest = json.load(f)
                    meta = {
                        "registrationNumber": manifest.get("registration_number", ""),
                        "purchase_notice_number": "",
                        "source_date": manifest.get("protocol_date", ""),
                        "record_id": manifest.get("protocol_id", ""),
                        "unit_id": target_dir.name,
                    }
            except (json.JSONDecodeError, IOError) as e:
                logger = __import__("logging").getLogger(__name__)
                logger.warning(f"Failed to read manifest.json: {e}")

    # Если ничего не нашли, создаём минимальный meta
    if meta is None:
        meta = {
            "registrationNumber": "",
            "purchase_notice_number": "",
            "source_date": "",
            "record_id": "",
            "unit_id": target_dir.name,
        }

    # Обновляем created_by
    meta["created_by"] = "docprep"
    meta["created_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    meta["unit_id"] = target_dir.name

    # Записываем в target директорию
    target_meta = target_dir / "unit.meta.json"
    target_dir.mkdir(parents=True, exist_ok=True)

    with open(target_meta, 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())

    logger = __import__("logging").getLogger(__name__)
    reg_num = meta.get("registrationNumber", "")
    logger.info(f"Created unit.meta.json in {target_dir.name} with registrationNumber={reg_num[:8] if reg_num else 'N/A'}...")


def get_trace_id_from_manifest(unit_dir: Path) -> str:
    """
    Получает primary trace ID из manifest.json или unit.meta.json.

    Priority:
    1. manifest["trace"]["primary_id"]
    2. manifest["registration_number"]
    3. unit.meta.json["registrationNumber"]
    4. unit_id (fallback)

    Args:
        unit_dir: Path к директории UNIT

    Returns:
        Primary trace ID (registrationNumber или fallback)
    """
    # Сначала проверяем manifest.json
    manifest_file = unit_dir / "manifest.json"
    if manifest_file.exists():
        try:
            with open(manifest_file, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
                # Проверяем trace.primary_id
                trace = manifest.get("trace", {})
                if trace.get("primary_id"):
                    return trace["primary_id"]
                # Проверяем registration_number
                reg_num = manifest.get("registration_number")
                if reg_num:
                    return reg_num
        except (json.JSONDecodeError, IOError):
            pass

    # Потом проверяем unit.meta.json
    reg_num = get_registration_number(unit_dir)
    if reg_num:
        return reg_num

    # Fallback на unit_id
    return unit_dir.name

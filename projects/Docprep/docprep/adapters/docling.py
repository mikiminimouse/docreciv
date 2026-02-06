"""
DoclingAdapter - адаптер для построения AST узлов для Docling.

Читает manifest.json и нормализованные файлы из Ready2Docling/UNIT_xxx,
строит структуру для передачи в Docling pipeline.
"""
from pathlib import Path
from typing import Dict, Any, List, Optional

from ..core.manifest import load_manifest
from ..core.exceptions import ManifestError


class DoclingAdapter:
    """
    Адаптер для преобразования UNIT в Docling-ready структуру.

    Preprocessing НЕ знает про Docling, Docling НЕ знает про Pending/Merge.
    Связь только через этот adapter.
    """

    def __init__(self):
        """Инициализирует DoclingAdapter."""
        pass

    def build_ast_nodes(self, unit_path: Path) -> Dict[str, Any]:
        """
        Строит AST узлы для Docling из UNIT в Ready2Docling.

        Args:
            unit_path: Путь к UNIT в Ready2Docling

        Returns:
            Словарь с AST структурой:
            - DocNode: корневой узел документа
            - PageNode: страницы (для PDF/image)
            - FileNode: файлы (document/attachment роли)
            - MetadataNode: метаданные из manifest
        """
        # Загружаем manifest
        try:
            manifest = load_manifest(unit_path)
        except FileNotFoundError:
            raise ManifestError(f"Manifest not found in {unit_path}")

        unit_id = manifest.get("unit_id", unit_path.name)
        protocol_id = manifest.get("protocol_id", "")

        # Находим файлы
        files_dir = unit_path / "files"
        if not files_dir.exists():
            files_dir = unit_path

        files = [
            f
            for f in files_dir.rglob("*")
            if f.is_file() and f.name not in ["manifest.json", "audit.log.jsonl"]
        ]

        # Строим структуру AST
        doc_node = {
            "type": "DocNode",
            "unit_id": unit_id,
            "protocol_id": protocol_id,
            "metadata": self._build_metadata_node(manifest),
            "files": self._build_file_nodes(files, manifest),
            "pages": self._build_page_nodes(files, manifest),
        }

        return doc_node

    def _build_metadata_node(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """
        Строит узел метаданных из manifest.

        Args:
            manifest: Manifest данные

        Returns:
            MetadataNode словарь
        """
        processing = manifest.get("processing", {})
        files = manifest.get("files", [])
        
        # Извлекаем route из processing
        route = processing.get("route")
        
        # Извлекаем needs_ocr из файлов (для PDF)
        needs_ocr = None
        file_type_analysis = {}
        transformation_history = []
        
        for file_info in files:
            # Собираем needs_ocr для PDF файлов
            detected_type = file_info.get("detected_type", "").lower()
            if detected_type == "pdf":
                file_needs_ocr = file_info.get("needs_ocr")
                if file_needs_ocr is not None:
                    needs_ocr = file_needs_ocr
            
            # Собираем file_type_analysis
            file_type_analysis[file_info.get("current_name", "")] = {
                "detected_type": detected_type,
                "mime_detected": file_info.get("mime_detected"),
                "needs_ocr": file_info.get("needs_ocr"),
                "pages_or_parts": file_info.get("pages_or_parts", 1),
            }
            
            # Собираем transformation history
            transformations = file_info.get("transformations", [])
            for trans in transformations:
                transformation_history.append({
                    "file": file_info.get("current_name", ""),
                    "type": trans.get("type"),
                    "details": trans.get("details", {}),
                })
        
        return {
            "type": "MetadataNode",
            "unit_id": manifest.get("unit_id"),
            "protocol_id": manifest.get("protocol_id"),
            "protocol_date": manifest.get("protocol_date"),
            "unit_semantics": manifest.get("unit_semantics", {}),
            "processing": {
                **processing,
                "route": route,
                "needs_ocr": needs_ocr,
            },
            "file_type_analysis": file_type_analysis,
            "transformation_history": transformation_history,
            "state_machine": manifest.get("state_machine", {}),
            "created_at": manifest.get("created_at"),
            "updated_at": manifest.get("updated_at"),
        }

    def _build_file_nodes(
        self, files: List[Path], manifest: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Строит узлы файлов.

        Args:
            files: Список путей к файлам
            manifest: Manifest данные

        Returns:
            Список FileNode словарей
        """
        file_nodes = []
        manifest_files = manifest.get("files", [])

        for file_path in files:
            # Ищем информацию о файле в manifest по current_name и original_name
            file_info = None
            for mf in manifest_files:
                current_name = mf.get("current_name", "")
                original_name = mf.get("original_name", "")
                if (
                    current_name == file_path.name
                    or original_name == file_path.name
                    or file_path.name in [current_name, original_name]
                ):
                    file_info = mf
                    break

            # Определяем роль файла на основе transformations
            role = "document"  # По умолчанию документ
            if file_info:
                transformations = file_info.get("transformations", [])
                # Если файл был конвертирован или нормализован, это основной документ
                if any(
                    t.get("type") in ["convert", "normalize", "extract"]
                    for t in transformations
                ):
                    role = "document"
                # Если файл был извлечен из архива как attachment, это вложение
                elif any(t.get("type") == "extract" for t in transformations):
                    # Проверяем детали извлечения
                    extract_trans = next(
                        (t for t in transformations if t.get("type") == "extract"), None
                    )
                    if extract_trans and extract_trans.get("details", {}).get("role") == "attachment":
                        role = "attachment"
                    else:
                        role = "document"
                else:
                    # Для файлов без трансформаций определяем по типу
                    detected_type = file_info.get("detected_type", "").lower()
                    if detected_type in ["pdf", "docx", "xlsx", "pptx", "html", "xml"]:
                        role = "document"
                    else:
                        role = "attachment"

            file_node = {
                "type": "FileNode",
                "path": str(file_path),
                "name": file_path.name,
                "original_name": file_info.get("original_name", file_path.name) if file_info else file_path.name,
                "current_name": file_info.get("current_name", file_path.name) if file_info else file_path.name,
                "role": role,
                "mime_type": file_info.get("mime_detected") if file_info else None,
                "detected_type": file_info.get("detected_type") if file_info else None,
                "needs_ocr": file_info.get("needs_ocr") if file_info else None,
                "transformations": file_info.get("transformations", []) if file_info else [],
            }
            file_nodes.append(file_node)

        return file_nodes

    def _build_page_nodes(
        self, files: List[Path], manifest: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Строит узлы страниц для PDF/image файлов.

        Args:
            files: Список путей к файлам
            manifest: Manifest данные

        Returns:
            Список PageNode словарей
        """
        page_nodes = []

        for file_path in files:
            # Только для PDF и изображений
            if file_path.suffix.lower() in [".pdf", ".jpg", ".jpeg", ".png", ".tiff"]:
                page_node = {
                    "type": "PageNode",
                    "file_path": str(file_path),
                    "page_number": 1,  # TODO: Определить номер страницы для PDF
                }
                page_nodes.append(page_node)

        return page_nodes

    def validate_readiness(self, unit_path: Path) -> Dict[str, Any]:
        """
        Валидирует готовность UNIT к обработке Docling.

        Args:
            unit_path: Путь к UNIT

        Returns:
            Словарь с результатами валидации
        """
        validation = {"ready": True, "errors": [], "warnings": []}

        # Проверка наличия manifest
        manifest_path = unit_path / "manifest.json"
        if not manifest_path.exists():
            validation["ready"] = False
            validation["errors"].append("manifest.json not found")
            return validation

        try:
            manifest = load_manifest(unit_path)
        except Exception as e:
            validation["ready"] = False
            validation["errors"].append(f"Failed to load manifest: {str(e)}")
            return validation

        # Проверка состояния - поддерживаем несколько состояний готовности
        state_machine = manifest.get("state_machine", {})
        current_state = state_machine.get("current_state")
        ready_states = ["READY_FOR_DOCLING", "MERGED_DIRECT", "MERGED_PROCESSED"]
        if current_state not in ready_states:
            validation["ready"] = False
            validation["errors"].append(
                f"Unit not ready: current state is {current_state}, "
                f"expected one of {ready_states}"
            )

        # Проверка наличия файлов
        files_dir = unit_path / "files"
        if not files_dir.exists():
            files_dir = unit_path

        files = [
            f
            for f in files_dir.rglob("*")
            if f.is_file() and f.name not in ["manifest.json", "audit.log.jsonl"]
        ]

        if not files:
            validation["ready"] = False
            validation["errors"].append("No files found in UNIT")

        return validation


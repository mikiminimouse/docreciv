"""
Decision Engine для разрешения конфликтов между источниками истины.

Реализует формализованные сценарии из эталонного подхода для определения
истинного типа файла на основе MIME, Signature и Extension.
"""
from typing import Dict, Any, Optional
from pathlib import Path


class TypeDecisionEngine:
    """
    Decision Engine для разрешения конфликтов между источниками истины.

    Реализует 7 формализованных сценариев из эталонного подхода.
    """

    # Порог confidence для принятия решения
    CONFIDENCE_THRESHOLD = 0.7

    def resolve(
        self,
        mime_type: str,
        mime_confidence: float,
        signature_type: Optional[str],
        signature_confidence: float,
        extension: str,
    ) -> Dict[str, Any]:
        """
        Разрешает конфликты между источниками истины.

        Args:
            mime_type: MIME тип файла
            mime_confidence: уверенность MIME (0.0-1.0)
            signature_type: тип по сигнатуре или None
            signature_confidence: уверенность сигнатуры (0.0-1.0)
            extension: расширение файла

        Returns:
            Словарь с решением (true_type, classification, scenario, etc.)
        """
        # Нормализуем типы для сравнения
        mime_norm = self._normalize_type(mime_type)
        sig_norm = self._normalize_type(signature_type) if signature_type else None
        ext_norm = self._normalize_type_from_extension(extension)

        # Специальная обработка для zip_or_office: если signature указывает на zip_or_office,
        # а MIME на zip, то для сравнения используем zip_or_office для обоих
        if sig_norm == "zip_or_office" and mime_norm == "zip":
            mime_norm = "zip_or_office"

        ctx = {
            "mime_norm": mime_norm,
            "sig_norm": sig_norm,
            "ext_norm": ext_norm,
            "mime_conf": mime_confidence,
            "sig_conf": signature_confidence,
            "mime_type_raw": mime_type,
        }

        # Scenario 2.1: Все три совпали
        if mime_norm == sig_norm == ext_norm and sig_norm:
            return self._result(sig_norm, "direct", "2.1", 1.0, True, None)

        # Scenario 2.2: MIME+Signature совпали ≠ Extension
        if mime_norm == sig_norm != ext_norm and sig_norm:
            return self._handle_mismatch_extension(ctx)

        # Scenario 2.3: Signature+Extension совпали ≠ MIME
        if sig_norm == ext_norm != mime_norm and sig_norm:
            return self._handle_mismatch_mime(ctx)

        # Scenario 2.4: MIME+Extension совпали ≠ Signature
        if mime_norm == ext_norm != sig_norm and mime_norm:
            return self._handle_mismatch_signature(ctx)

        # Scenario 2.5: Все три разные
        if mime_norm and sig_norm and ext_norm and mime_norm != sig_norm != ext_norm and mime_norm != ext_norm:
            return self._handle_all_differs(ctx)

        # Scenario 2.6: MIME = octet-stream
        if mime_type == "application/octet-stream":
            return self._handle_octet_stream(ctx)

        # Scenario 2.7: Signature отсутствует
        if not sig_norm:
            return self._handle_missing_signature(ctx)

        # Fallback
        return self._result(
            mime_norm or ext_norm, 
            "ambiguous", 
            "unknown", 
            0.0, 
            False, 
            "Unable to resolve"
        )
    
    def _result(
        self,
        true_type: Optional[str],
        classification: str,
        scenario: str,
        confidence: float,
        agreement: bool,
        conflict: Optional[str],
        correct_ext: Optional[str] = None
    ) -> Dict[str, Any]:
        """Helper to construct result dictionary."""
        return {
            "true_type": true_type,
            "classification": classification,
            "scenario": scenario,
            "confidence": confidence,
            "sources_agreement": agreement,
            "conflict_reason": conflict,
            "correct_extension": correct_ext,
        }

    def _handle_mismatch_extension(self, ctx: Dict[str, Any]) -> Dict[str, Any]:
        sig_norm = ctx["sig_norm"]
        ext_norm = ctx["ext_norm"]
        conf = min(ctx["mime_conf"], ctx["sig_conf"])
        
        needs_parse = sig_norm in ["zip_or_office", "ole2"]
        conflict = f"MIME+Signature={sig_norm} but Extension={ext_norm}"
        if needs_parse:
            conflict += ", needs structural parsing"
        
        correct_ext = None if needs_parse else self._type_to_extension(sig_norm)
        return self._result(sig_norm, "normalize", "2.2", conf, False, conflict, correct_ext)

    def _handle_mismatch_mime(self, ctx: Dict[str, Any]) -> Dict[str, Any]:
        sig_norm = ctx["sig_norm"]
        mime_norm = ctx["mime_norm"]
        conf = ctx["sig_conf"]
        
        needs_parse = sig_norm in ["zip_or_office", "ole2"]
        conflict = f"Signature+Extension={sig_norm} but MIME={mime_norm}"
        if needs_parse:
            conflict += ", needs structural parsing"
            
        correct_ext = None if needs_parse else self._type_to_extension(sig_norm)
        return self._result(sig_norm, "normalize", "2.3", conf, False, conflict, correct_ext)

    def _handle_mismatch_signature(self, ctx: Dict[str, Any]) -> Dict[str, Any]:
        mime_norm = ctx["mime_norm"]
        sig_norm = ctx["sig_norm"]
        conf = ctx["mime_conf"]
        
        if conf >= self.CONFIDENCE_THRESHOLD:
            return self._result(
                mime_norm, "direct", "2.4", conf, False, 
                f"MIME+Extension={mime_norm} but Signature={sig_norm}"
            )
        else:
            return self._result(
                None, "ambiguous", "2.4", conf, False, 
                f"Low MIME confidence ({conf})"
            )

    def _handle_all_differs(self, ctx: Dict[str, Any]) -> Dict[str, Any]:
        sig_norm = ctx["sig_norm"]
        mime_norm = ctx["mime_norm"]
        sig_conf = ctx["sig_conf"]
        mime_conf = ctx["mime_conf"]
        
        if sig_conf >= self.CONFIDENCE_THRESHOLD:
            return self._result(
                sig_norm, "normalize", "2.5", sig_conf, False, 
                "All sources differ, using signature", self._type_to_extension(sig_norm)
            )
        elif mime_conf >= self.CONFIDENCE_THRESHOLD:
            return self._result(
                mime_norm, "normalize", "2.5", mime_conf, False, 
                "All sources differ, using MIME", self._type_to_extension(mime_norm)
            )
        else:
            return self._result(
                None, "ambiguous", "2.5", max(sig_conf, mime_conf), False, 
                "All sources differ, low confidence"
            )

    def _handle_octet_stream(self, ctx: Dict[str, Any]) -> Dict[str, Any]:
        sig_norm = ctx["sig_norm"]
        ext_norm = ctx["ext_norm"]
        sig_conf = ctx["sig_conf"]
        mime_type_raw = ctx["mime_type_raw"]

        # ОПТИМИЗАЦИЯ: Для Office форматов (docx/xlsx/pptx) с правильным расширением
        # возвращаем "direct" вместо "normalize", даже если MIME=octet-stream
        # Это исправляет проблему когда python-magic неправильно определяет MIME тип OOXML документов
        office_formats = {
            "docx": "docx",
            "xlsx": "xlsx",
            "pptx": "pptx",
        }
        if ext_norm in office_formats:
            # Проверяем сигнатуру PK\x03\x04 (ZIP/OOXML)
            if sig_norm in ["zip_or_office", "zip"]:
                # Это действительно Office документ с неправильным MIME
                return self._result(
                    ext_norm, "direct", "2.6", sig_conf or 0.9, True,
                    f"MIME=octet-stream but {ext_norm.upper()} signature+extension match"
                )
            # Если нет сигнатуры, но расширение правильное Office формата
            return self._result(
                ext_norm, "direct", "2.6", 0.7, True,
                f"MIME=octet-stream but {ext_norm.upper()} extension (assuming Office format)"
            )

        if sig_norm:
            return self._result(
                sig_norm, "normalize", "2.6", sig_conf, False,
                "MIME=octet-stream, using signature", self._type_to_extension(sig_norm)
            )
        elif ext_norm:
            return self._result(
                ext_norm, "normalize", "2.6", 0.5, False,
                "MIME=octet-stream, using extension", self._type_to_extension(ext_norm)
            )
        else:
            return self._result(
                None, "ambiguous", "2.6", 0.0, False, "MIME=octet-stream, no other sources"
            )

    def _handle_missing_signature(self, ctx: Dict[str, Any]) -> Dict[str, Any]:
        mime_norm = ctx["mime_norm"]
        ext_norm = ctx["ext_norm"]
        mime_conf = ctx["mime_conf"]
        
        if mime_conf >= self.CONFIDENCE_THRESHOLD:
            return self._result(
                mime_norm, "normalize", "2.7", mime_conf, False, 
                "No signature, using MIME", self._type_to_extension(mime_norm)
            )
        elif ext_norm:
            return self._result(
                ext_norm, "normalize", "2.7", 0.5, False, 
                "No signature, low MIME confidence, using extension", self._type_to_extension(ext_norm)
            )
        else:
            return self._result(
                None, "ambiguous", "2.7", 0.0, False, "No signature, no reliable sources"
            )

    def _normalize_type(self, detected_type: Optional[str]) -> Optional[str]:
        """
        Нормализует тип для сравнения.

        Args:
            detected_type: Тип файла (MIME или signature type)

        Returns:
            Нормализованный тип для внутреннего сравнения
        """
        if not detected_type:
            return None

        # Специальные случаи для zip_or_office и ole2
        if detected_type == "zip_or_office":
            return "zip_or_office"
        if detected_type == "ole2":
            return "ole2"

        # Маппинг MIME типов на внутренние типы
        mime_to_type = {
            "application/pdf": "pdf",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
            "application/msword": "doc",
            "application/vnd.ms-excel": "xls",
            "application/vnd.ms-powerpoint": "ppt",
            "application/zip": "zip",
            "application/x-zip-compressed": "zip",
            "application/x-rar": "rar",
            "application/x-7z-compressed": "7z",
            "application/x-7z": "7z",
            "image/jpeg": "jpeg",
            "image/png": "png",
            "image/gif": "gif",
            "image/tiff": "tiff",
            "text/html": "html",
            "text/xml": "xml",
            "application/xml": "xml",
            "text/plain": "txt",
            "application/rtf": "rtf",
            "text/rtf": "rtf",
        }

        normalized = mime_to_type.get(detected_type)
        if normalized:
            return normalized

        # Если это уже внутренний тип (pdf, docx, и т.д.), возвращаем как есть
        return detected_type.lower()

    def _normalize_type_from_extension(self, extension: str) -> Optional[str]:
        """
        Нормализует тип из расширения.

        Args:
            extension: Расширение файла (с точкой или без)

        Returns:
            Нормализованный тип или None
        """
        if not extension:
            return None
        ext_clean = extension.lstrip(".").lower()
        return ext_clean if ext_clean else None

    def _type_to_extension(self, file_type: str) -> Optional[str]:
        """
        Конвертирует тип файла в расширение.

        Args:
            file_type: Внутренний тип файла (pdf, docx, и т.д.)

        Returns:
            Расширение с точкой или None
        """
        TYPE_TO_EXTENSION = {
            "pdf": ".pdf",
            "docx": ".docx",
            "doc": ".doc",
            "xlsx": ".xlsx",
            "xls": ".xls",
            "pptx": ".pptx",
            "ppt": ".ppt",
            "zip_archive": ".zip",
            "zip": ".zip",
            "rar_archive": ".rar",
            "rar": ".rar",
            "7z_archive": ".7z",
            "7z": ".7z",
            "jpeg": ".jpeg",
            "jpg": ".jpg",
            "png": ".png",
            "gif": ".gif",
            "tiff": ".tiff",
            "html": ".html",
            "xml": ".xml",
            "txt": ".txt",
            "rtf": ".rtf",
        }
        return TYPE_TO_EXTENSION.get(file_type)


def _needs_structural_parsing(sources: Dict[str, Any], true_type: Optional[str]) -> bool:
    """
    Определяет, нужен ли структурный парсинг для окончательного определения типа.

    Args:
        sources: Словарь с источниками истины
        true_type: Предполагаемый тип из Decision Engine

    Returns:
        True если нужен структурный парсинг
    """
    # Нужен парсинг для zip_or_office (различать ZIP от DOCX/XLSX/PPTX)
    if sources.get("signature_type") == "zip_or_office" or true_type == "zip_or_office":
        return True

    # Нужен парсинг для ole2 (различать DOC/XLS/PPT)
    if sources.get("signature_type") == "ole2" or true_type == "ole2":
        return True

    return False


def resolve_type_decision(sources: Dict[str, Any], file_path: Path) -> Dict[str, Any]:
    """
    Главная функция для разрешения конфликтов типов.

    Использует TypeDecisionEngine и добавляет структурный парсинг при необходимости.

    Args:
        sources: Словарь с источниками истины из collect_truth_sources()
        file_path: Путь к файлу (для структурного парсинга если нужно)

    Returns:
        Словарь с решением Decision Engine
    """
    engine = TypeDecisionEngine()

    decision = engine.resolve(
        mime_type=sources["mime_type"],
        mime_confidence=sources["mime_confidence"],
        signature_type=sources["signature_type"],
        signature_confidence=sources["signature_confidence"],
        extension=sources["extension"],
    )

    # Добавляем источники для прозрачности
    decision["sources"] = sources

    # Определяем, нужен ли структурный парсинг
    decision["needs_structural_parsing"] = _needs_structural_parsing(
        sources, decision.get("true_type")
    )

    return decision


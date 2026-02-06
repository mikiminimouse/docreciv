"""
Dependency Checker для Docprep.

Проверяет наличие системных утилит и Python библиотек, необходимых
для работы Docprep (LibreOffice, Xvfb, unrar, p7zip, etc.).

Использование:
    from docprep.utils.dependencies import DependencyChecker

    # Проверить конкретную утилиту
    lo_cmd = DependencyChecker.check_system_tool("libreoffice")

    # Проверить все зависимости
    result = DependencyChecker.check_all()
"""
import shutil
import logging
import sys
from typing import Dict, List, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class DependencyChecker:
    """
    Проверка системных зависимостей Docprep.

    Проверяет как системные утилиты (через shutil.which), так и
    Python библиотеки (через import).
    """

    # Системные утилиты для проверки
    # Ключ - имя утилиты для логов, значение - список возможных команд
    SYSTEM_TOOLS: Dict[str, List[str]] = {
        "libreoffice": ["libreoffice", "soffice"],
        "xvfb": ["Xvfb"],
        "unrar": ["unrar"],
        "p7zip": ["7z", "7za"],
        "file": ["file"],  # для file command
    }

    # Python библиотеки для проверки
    # Ключ - имя для логов, значение - имя модуля для import
    PYTHON_LIBS: Dict[str, str] = {
        "rarfile": "rarfile",
        "py7zr": "py7zr",
        "pymongo": "pymongo",
        "python-magic": "magic",
        "pypdf": "pypdf",
    }

    # Инструкции по установке системных утилит
    SYSTEM_INSTALL_INSTRUCTIONS: Dict[str, str] = {
        "libreoffice": "sudo apt-get install libreoffice-core libreoffice-writer libreoffice-calc libreoffice-impress",
        "xvfb": "sudo apt-get install xvfb",
        "unrar": "sudo apt-get install unrar",
        "p7zip": "sudo apt-get install p7zip-full",
        "file": "sudo apt-get install file",
    }

    # Инструкции по установке Python библиотек
    PYTHON_INSTALL_INSTRUCTIONS: Dict[str, str] = {
        "rarfile": "pip install rarfile",
        "py7zr": "pip install py7zr",
        "pymongo": "pip install pymongo",
        "python-magic": "pip install python-magic",
        "pypdf": "pip install pypdf",
    }

    # Критические зависимости (без которых система не работает)
    CRITICAL_SYSTEM_TOOLS: List[str] = ["libreoffice", "xvfb"]
    CRITICAL_PYTHON_LIBS: List[str] = ["python-magic"]

    @classmethod
    def check_system_tool(cls, tool_name: str) -> Optional[str]:
        """
        Проверяет наличие системной утилиты.

        Args:
            tool_name: Имя утилиты (ключ из SYSTEM_TOOLS)

        Returns:
            Имя найденной команды или None если не найдена
        """
        commands = cls.SYSTEM_TOOLS.get(tool_name, [tool_name])
        for cmd in commands:
            if shutil.which(cmd):
                return cmd
        return None

    @classmethod
    def check_python_lib(cls, lib_name: str) -> bool:
        """
        Проверяет наличие Python библиотеки.

        Args:
            lib_name: Имя библиотеки (ключ из PYTHON_LIBS)

        Returns:
            True если библиотека доступна, иначе False
        """
        module_name = cls.PYTHON_LIBS.get(lib_name, lib_name)
        try:
            __import__(module_name)
            return True
        except ImportError:
            return False

    @classmethod
    def get_install_instruction(cls, dependency_type: str, name: str) -> Optional[str]:
        """
        Возвращает инструкцию по установке зависимости.

        Args:
            dependency_type: Тип зависимости ("system" или "python")
            name: Имя зависимости

        Returns:
            Строка с инструкцией или None
        """
        if dependency_type == "system":
            return cls.SYSTEM_INSTALL_INSTRUCTIONS.get(name)
        elif dependency_type == "python":
            return cls.PYTHON_INSTALL_INSTRUCTIONS.get(name)
        return None

    @classmethod
    def check_all(cls, include_optional: bool = False) -> Dict[str, Any]:
        """
        Проверяет все зависимости.

        Args:
            include_optional: Включать ли опциональные зависимости в список проблем

        Returns:
            Словарь с результатами проверки:
            {
                "system": {tool: {"available": bool, "command": str|None}},
                "python": {lib: {"available": bool}},
                "critical_issues": [str],
                "optional_issues": [str],
                "all_ok": bool
            }
        """
        result = {
            "system": {},
            "python": {},
            "critical_issues": [],
            "optional_issues": [],
            "all_ok": True,
        }

        # Проверяем системные утилиты
        for tool in cls.SYSTEM_TOOLS:
            found = cls.check_system_tool(tool)
            result["system"][tool] = {
                "available": found is not None,
                "command": found,
            }
            if not found:
                if tool in cls.CRITICAL_SYSTEM_TOOLS or include_optional:
                    instruction = cls.get_install_instruction("system", tool)
                    issue = f"Missing system tool: {tool}"
                    if instruction:
                        issue += f". Install: {instruction}"
                    if tool in cls.CRITICAL_SYSTEM_TOOLS:
                        result["critical_issues"].append(issue)
                        result["all_ok"] = False
                    else:
                        result["optional_issues"].append(issue)

        # Проверяем Python библиотеки
        for lib in cls.PYTHON_LIBS:
            available = cls.check_python_lib(lib)
            result["python"][lib] = {"available": available}
            if not available:
                if lib in cls.CRITICAL_PYTHON_LIBS or include_optional:
                    instruction = cls.get_install_instruction("python", lib)
                    issue = f"Missing Python library: {lib}"
                    if instruction:
                        issue += f". Install: {instruction}"
                    if lib in cls.CRITICAL_PYTHON_LIBS:
                        result["critical_issues"].append(issue)
                        result["all_ok"] = False
                    else:
                        result["optional_issues"].append(issue)

        return result

    @classmethod
    def check_archive_support(cls) -> Dict[str, Dict[str, Any]]:
        """
        Проверяет поддержку форматов архивов.

        Returns:
            Словарь с информацией о поддержке каждого формата:
            {
                "zip": {"python": true, "system": true, "ready": true},
                "rar": {"python": bool, "system": bool, "ready": bool},
                "7z": {"python": bool, "system": bool, "ready": bool},
            }
        """
        result = {}

        # ZIP всегда доступен (встроенный)
        result["zip"] = {
            "python": True,  # zipfile встроенный
            "system": True,  # не требует системных утилит
            "ready": True,
        }

        # RAR
        rar_python = cls.check_python_lib("rarfile")
        rar_system = cls.check_system_tool("unrar") is not None
        result["rar"] = {
            "python": rar_python,
            "system": rar_system,
            "ready": rar_python and rar_system,
        }

        # 7Z
        sevenz_python = cls.check_python_lib("py7zr")
        # py7zr не требует системной утилиты 7z (чистая Python реализация)
        result["7z"] = {
            "python": sevenz_python,
            "system": True,  # py7zr не требует системной утилиты
            "ready": sevenz_python,
        }

        return result

    @classmethod
    def check_conversion_support(cls) -> Dict[str, Any]:
        """
        Проверяет поддержку конвертации документов.

        Returns:
            Словарь с информацией о поддержке конвертации:
            {
                "libreoffice": bool,
                "xvfb": bool,
                "ready": bool,
            }
        """
        libreoffice = cls.check_system_tool("libreoffice") is not None
        xvfb = cls.check_system_tool("xvfb") is not None

        return {
            "libreoffice": libreoffice,
            "xvfb": xvfb,
            "ready": libreoffice and xvfb,
        }

    @classmethod
    def format_report(cls, check_result: Dict[str, Any]) -> str:
        """
        Форматирует результат проверки в читаемый текст.

        Args:
            check_result: Результат из check_all()

        Returns:
            Отформатированная строка с отчётом
        """
        lines = ["Dependency Check Report", "=" * 40]

        # Системные утилиты
        lines.append("\nSystem Tools:")
        for tool, info in check_result["system"].items():
            status = "✓" if info["available"] else "✗"
            cmd = info.get("command") or "not found"
            lines.append(f"  {status} {tool}: {cmd}")

        # Python библиотеки
        lines.append("\nPython Libraries:")
        for lib, info in check_result["python"].items():
            status = "✓" if info["available"] else "✗"
            lines.append(f"  {status} {lib}")

        # Проблемы
        if check_result["critical_issues"]:
            lines.append("\nCritical Issues:")
            for issue in check_result["critical_issues"]:
                lines.append(f"  ✗ {issue}")

        if check_result["optional_issues"]:
            lines.append("\nOptional Issues:")
            for issue in check_result["optional_issues"]:
                lines.append(f"  ⚠ {issue}")

        # Итог
        if check_result["all_ok"]:
            lines.append("\n✓ All dependencies satisfied!")
        else:
            lines.append("\n✗ Some dependencies are missing!")

        return "\n".join(lines)


def check_dependencies_command() -> int:
    """
    Функция для использования в CLI команде check-deps.

    Returns:
        Код выхода (0 если всё ОК, 1 если есть проблемы)
    """
    result = DependencyChecker.check_all(include_optional=True)

    print(DependencyChecker.format_report(result))

    return 0 if result["all_ok"] else 1

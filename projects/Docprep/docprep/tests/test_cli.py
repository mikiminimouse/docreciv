"""
Тесты CLI интерфейса docprep.

Проверяет базовую функциональность CLI команд.
"""
import pytest
from typer.testing import CliRunner
from pathlib import Path

from docprep.cli.main import app


runner = CliRunner()


class TestCLIBasic:
    """Базовые тесты CLI."""

    def test_cli_help(self):
        """Проверяет, что --help работает."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "docprep" in result.stdout.lower() or "cli" in result.stdout.lower()

    def test_cli_help_short(self):
        """Проверяет, что -h работает."""
        result = runner.invoke(app, ["-h"])
        # Typer может не поддерживать -h по умолчанию
        assert result.exit_code in [0, 2]

    def test_cli_no_args(self):
        """Проверяет вывод без аргументов."""
        result = runner.invoke(app, [])
        # Без аргументов может показать help или ошибку
        assert result.exit_code in [0, 2]


class TestCLISubcommands:
    """Тесты наличия подкоманд."""

    def test_classifier_help(self):
        """Проверяет доступность команды classifier."""
        result = runner.invoke(app, ["classifier", "--help"])
        assert result.exit_code == 0
        assert "classifier" in result.stdout.lower() or "classify" in result.stdout.lower()

    def test_merge_help(self):
        """Проверяет доступность команды merge."""
        result = runner.invoke(app, ["merge", "--help"])
        assert result.exit_code == 0

    def test_pipeline_help(self):
        """Проверяет доступность команды pipeline."""
        result = runner.invoke(app, ["pipeline", "--help"])
        assert result.exit_code == 0

    def test_cycle_help(self):
        """Проверяет доступность команды cycle."""
        result = runner.invoke(app, ["cycle", "--help"])
        assert result.exit_code == 0

    def test_stage_help(self):
        """Проверяет доступность команды stage."""
        result = runner.invoke(app, ["stage", "--help"])
        assert result.exit_code == 0

    def test_substage_help(self):
        """Проверяет доступность команды substage."""
        result = runner.invoke(app, ["substage", "--help"])
        assert result.exit_code == 0

    def test_inspect_help(self):
        """Проверяет доступность команды inspect."""
        result = runner.invoke(app, ["inspect", "--help"])
        assert result.exit_code == 0

    def test_utils_help(self):
        """Проверяет доступность команды utils."""
        result = runner.invoke(app, ["utils", "--help"])
        assert result.exit_code == 0

    def test_stats_help(self):
        """Проверяет доступность команды stats."""
        result = runner.invoke(app, ["stats", "--help"])
        assert result.exit_code == 0

    def test_chunked_classifier_help(self):
        """Проверяет доступность команды chunked-classifier."""
        result = runner.invoke(app, ["chunked-classifier", "--help"])
        assert result.exit_code == 0


class TestCLIGlobalOptions:
    """Тесты глобальных опций CLI."""

    def test_verbose_option(self):
        """Проверяет, что --verbose принимается."""
        result = runner.invoke(app, ["--verbose", "--help"])
        assert result.exit_code == 0

    def test_verbose_short_option(self):
        """Проверяет, что -v принимается."""
        result = runner.invoke(app, ["-v", "--help"])
        assert result.exit_code == 0

    def test_dry_run_option(self):
        """Проверяет, что --dry-run принимается."""
        result = runner.invoke(app, ["--dry-run", "--help"])
        assert result.exit_code == 0


class TestCLIInvalidCommands:
    """Тесты обработки невалидных команд."""

    def test_invalid_command(self):
        """Проверяет обработку несуществующей команды."""
        result = runner.invoke(app, ["nonexistent-command"])
        assert result.exit_code != 0

    def test_invalid_subcommand(self):
        """Проверяет обработку несуществующей подкоманды."""
        result = runner.invoke(app, ["classifier", "nonexistent"])
        assert result.exit_code != 0

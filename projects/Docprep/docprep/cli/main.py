"""
Main CLI - точка входа для всех команд docprep.

Обрабатывает глобальные опции и подключает все подкоманды.
"""
import typer
from pathlib import Path
from typing import Optional

# Импорты подкоманд
from . import pipeline
from . import cycle
from . import stage
from . import substage
from . import classifier
from . import merge
from . import inspect_cmd as inspect
from . import utils
from . import stats
from . import chunked_classifier
from . import mongo

app = typer.Typer(
    name="docprep",
    help="CLI система для preprocessing документов",
    add_completion=False,
)

# Глобальные опции
verbose_option = typer.Option(False, "--verbose", "-v", help="Включить подробный вывод")
dry_run_option = typer.Option(
    False, "--dry-run", help="Режим проверки без выполнения операций"
)


def get_context_options(ctx: typer.Context) -> dict:
    """Получает глобальные опции из контекста."""
    return {
        "verbose": ctx.params.get("verbose", False),
        "dry_run": ctx.params.get("dry_run", False),
    }


# Подключаем подкоманды
app.add_typer(pipeline.app, name="pipeline")
app.add_typer(cycle.app, name="cycle")
app.add_typer(stage.app, name="stage")
app.add_typer(substage.app, name="substage")
app.add_typer(classifier.app, name="classifier")
app.add_typer(chunked_classifier.app, name="chunked-classifier")
app.add_typer(merge.app, name="merge")
app.add_typer(inspect.app, name="inspect")
app.add_typer(utils.app, name="utils")
app.add_typer(stats.app, name="stats")
app.add_typer(mongo.app, name="mongo")


@app.callback()
def main(
    verbose: bool = verbose_option,
    dry_run: bool = dry_run_option,
):
    """
    DocPrep - CLI система для preprocessing документов.

    Обрабатывает директории UNIT целиком с поддержкой 3 циклов обработки.
    """
    # Глобальные опции обрабатываются через параметры команд
    pass


if __name__ == "__main__":
    app()


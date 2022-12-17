from datetime import datetime, timedelta
import enum
import os
from pathlib import Path
from typing import Optional

from dotenv import find_dotenv, load_dotenv
from rich.console import Console
from rich.table import Table
from tqdm import tqdm
import typer
from loguru import logger

from cloudpathlib import implementation_registry


from runner import main, normalize_results, results_to_csv

# make loguru and tqdm play nicely together
logger.remove()
logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)

# get environement variables
load_dotenv(find_dotenv())

# enumerate cloudpathlib implementations
CloudEnum = enum.Enum("CloudEnum", {k: k for k in implementation_registry.keys()})

# initialize CLI
cli = typer.Typer()


def _configure(
    backend: CloudEnum,
    bucket: Optional[str] = None,
):
    # common configuration for all commands
    logger.info(f"Setting up tests with backend {backend}.")

    if bucket is None:
        logger.info("Bucket not set explicitly, loading from environment variable.")
        bucket = {
            "s3": os.environ.get("LIVE_S3_BUCKET"),
            "gs": os.environ.get("LIVE_AZURE_CONTAINER"),
            "azure": os.environ.get("LIVE_GS_BUCKET"),
        }.get(backend.value)

    logger.info(f"Bucket: {bucket}")

    # get the actual implementation
    backend = implementation_registry.get(backend.value)

    return backend.path_class(f"{backend.path_class.cloud_prefix}{bucket}/performance_tests")


def results_to_rich_table(results):
    table = Table(title=f"Performance suite results: ({datetime.now().isoformat().split('Z')[0]})")

    var_to_title = lambda x: (" ".join(x.split("_"))).title()

    all_fields, row_list = normalize_results(results)

    for field in all_fields:
        col_kwargs = {}
        # get a sample value
        val = row_list[0][field]

        if isinstance(val, (int, float)):
            col_kwargs["justify"] = "right"

        # average performance value is highlighted in green
        if field == "mean":
            col_kwargs["style"] = "green"

        table.add_column(var_to_title(field), **col_kwargs)

    def _format_row(r):
        formatted = []
        for f in all_fields:
            val = r[f]

            if f in ["mean", "min", "max", "std"]:
                val = str(timedelta(seconds=val))
            elif isinstance(val, int):
                val = f"{val:,}"

            if f == "std":
                val = "± " + val

            formatted.append(val)

        return formatted

    for row in row_list:
        table.add_row(*_format_row(row))

    return table


@cli.command(short_help="Runs peformance test suite against a specific backend and bucket.")
def run(
    backend: CloudEnum,
    bucket: Optional[str] = None,
    iterations: int = 10,
    burn_in: int = 2,
    save_csv: Optional[Path] = None,
):
    root = _configure(backend, bucket)

    results = main(root, iterations, burn_in)

    c = Console()
    c.print(results_to_rich_table(results))

    if save_csv is not None:
        results_to_csv(results, save_csv)


if __name__ == "__main__":
    cli()

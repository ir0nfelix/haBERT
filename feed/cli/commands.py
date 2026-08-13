import asyncio
import os
from pathlib import Path
import typer

from feed.helpers import get_data_path
from feed.services.headfraction import execute_headfraction
from feed.services.scraper import init_ray, scraper as execute_scraper
from feed.settings import RAW_SCRAPES_FILE, settings

app = typer.Typer(help="haBERT CLI - Scraper & Analytics Pipeline Manager")


@app.command()
def run_scraper(
    start_id: int = typer.Option(None, help="Start publication ID"),
    end_id: int = typer.Option(None, help="End publication ID"),
    batch_size: int = typer.Option(None, help="Batch size per scrape turn"),
    concurrency: int = typer.Option(None, help="Concurrent request limit"),
    output_file: str = typer.Option(None, help="Path to output JSONL file"),
    use_ray: bool = typer.Option(True, help="Enable Ray cluster integration for gap probing"),
) -> None:
    typer.secho(">>> ЗАПУСК СЛОЯ EXTRACT: Парсинг Хабра", fg=typer.colors.CYAN)

    s_id = start_id if start_id is not None else settings.START_ID
    e_id = end_id if end_id is not None else settings.END_ID
    b_size = batch_size if batch_size is not None else settings.BATCH_SIZE
    conc = concurrency if concurrency is not None else settings.CONCURRENCY
    out_file = output_file if output_file is not None else (settings.OUTPUT_FILE or get_data_path(RAW_SCRAPES_FILE))

    if use_ray:
        init_ray()

    asyncio.run(
        execute_scraper(
            start_id=s_id,
            end_id=e_id,
            batch_size=b_size,
            concurrency=conc,
            output_file=out_file,
            use_ray=use_ray,
        )
    )
    typer.secho("Парсинг завершен!", fg=typer.colors.GREEN)


@app.command()
def run_headfraction(
    input_file: str = typer.Argument(
        None,
        help="Path to raw scraper result file",
    ),
) -> None:
    """Run DuckDB analytics layer on extracted JSONL data."""
    target_path = input_file or settings.OUTPUT_FILE or get_data_path(RAW_SCRAPES_FILE)
    typer.secho(f">>> ЗАПУСК СЛОЯ ПЕРВИЧНОЙ АНАЛИТИКИ: Обработка {target_path}", fg=typer.colors.CYAN)

    resolved_path = target_path
    if not os.path.exists(resolved_path) and os.path.exists(os.path.join("feed", target_path)):
        resolved_path = os.path.join("feed", target_path)

    execute_headfraction(resolved_path)
    typer.secho("Аналитика завершена!", fg=typer.colors.GREEN)


@app.command()
def run_all_steps(
    start_id: int = typer.Option(None, help="Start publication ID"),
    end_id: int = typer.Option(None, help="End publication ID"),
    batch_size: int = typer.Option(None, help="Batch size per scrape turn"),
    concurrency: int = typer.Option(None, help="Concurrent request limit"),
    output_file: str = typer.Option(None, help="Path to output JSONL file"),
    use_ray: bool = typer.Option(True, help="Enable Ray cluster integration"),
) -> None:
    """Run both Extract (Scraper) and Transform (DuckDB Analytics) layers in sequence."""
    typer.secho(">>> НАЧАЛО ПОЛНОГО ПАЙПЛАЙНА (СКРАПЕР + ПЕРВАЧ) ===", fg=typer.colors.MAGENTA, bold=True)
    run_scraper(
        start_id=start_id,
        end_id=end_id,
        batch_size=batch_size,
        concurrency=concurrency,
        output_file=output_file,
        use_ray=use_ray,
    )
    out_file = output_file if output_file is not None else (settings.OUTPUT_FILE or get_data_path(RAW_SCRAPES_FILE))
    run_headfraction(input_file=out_file)
    typer.secho("=== ПОЛНЫЙ ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН ===", fg=typer.colors.MAGENTA, bold=True)


if __name__ == "__main__":
    app()


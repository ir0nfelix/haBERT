import asyncio

import typer

from feed.ray import init_ray
from feed.services.headfraction import headfraction
from feed.services.scraper import scraper
from feed.settings import settings

app = typer.Typer(help="haBERT CLI - Scraper & Analytics Pipeline Manager")


@app.command()
def run_scraper(
    start_id: int = typer.Option(None, help="Start publication ID"),
    end_id: int = typer.Option(None, help="End publication ID"),
    batch_size: int = typer.Option(None, help="Batch size per scrape turn"),
    concurrency: int = typer.Option(None, help="Concurrent request limit"),
    output_file: str = typer.Option(None, help="Path to output JSONL file"),
    use_ray: bool = typer.Option(None, help="Enable Ray cluster integration for gap probing"),
) -> None:
    typer.secho(">>> ЗАПУСК ЦАП-ЦАРАПКИ", fg=typer.colors.CYAN)

    s_id = start_id if start_id is not None else settings.START_ID
    e_id = end_id if end_id is not None else settings.END_ID
    b_size = batch_size if batch_size is not None else settings.BATCH_SIZE
    conc = concurrency if concurrency is not None else settings.CONCURRENCY
    out_file = output_file if output_file is not None else settings.SCRAPER_OUTPUT_FILE
    u_ray = use_ray if use_ray is not None else settings.USE_RAY

    if u_ray:
        init_ray()

    asyncio.run(
        scraper(
            start_id=s_id,
            end_id=e_id,
            batch_size=b_size,
            concurrency=conc,
            output_file=out_file,
            use_ray=u_ray,
        )
    )
    typer.secho("Парсинг контента завершен!", fg=typer.colors.GREEN)


@app.command()
def run_headfraction(
    input_file: str = typer.Argument(
        None,
        help="Path to raw scraper result file",
    ),
) -> None:
    target_path = input_file if input_file is not None else settings.SCRAPER_OUTPUT_FILE
    typer.secho(f">>> ЗАПУСК ПЕРВАЧА: Обработка файла {target_path}", fg=typer.colors.CYAN)
    headfraction(target_path)
    typer.secho("Аналитика завершена!", fg=typer.colors.GREEN)


@app.command()
def run_all_steps(
    start_id: int = typer.Option(None, help="Start publication ID"),
    end_id: int = typer.Option(None, help="End publication ID"),
    batch_size: int = typer.Option(None, help="Batch size per scrape turn"),
    concurrency: int = typer.Option(None, help="Concurrent request limit"),
    output_file: str = typer.Option(None, help="Path to output JSONL file"),
    use_ray: bool = typer.Option(None, help="Enable Ray cluster integration"),
) -> None:
    typer.secho(">>> НАЧАЛО ПОЛНОГО ПАЙПЛАЙНА (СКРАПЕР + ПЕРВАЧ)", fg=typer.colors.MAGENTA, bold=True)

    s_id = start_id if start_id is not None else settings.START_ID
    e_id = end_id if end_id is not None else settings.END_ID
    b_size = batch_size if batch_size is not None else settings.BATCH_SIZE
    conc = concurrency if concurrency is not None else settings.CONCURRENCY
    out_file = output_file if output_file is not None else settings.SCRAPER_OUTPUT_FILE
    u_ray = use_ray if use_ray is not None else settings.USE_RAY

    if u_ray:
        init_ray()

    asyncio.run(
        scraper(
            start_id=s_id,
            end_id=e_id,
            batch_size=b_size,
            concurrency=conc,
            output_file=out_file,
            use_ray=u_ray,
        )
    )

    typer.secho("--- ЭТАП 2: ТРАНСФОРМАЦИЯ И ОЧИСТКА (ПЕРВАЧ) ---", fg=typer.colors.BLUE)
    headfraction(out_file)
    typer.secho("=== ПОЛНЫЙ ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН ===", fg=typer.colors.MAGENTA, bold=True)

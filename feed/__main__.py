from feed.cli.commands import app
from feed.helpers import setup_logging

if __name__ == "__main__":
    setup_logging()
    app()

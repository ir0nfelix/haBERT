FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# Install dependencies layer first for build caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire project directory
COPY . .

# Create output data directory inside the feed app
RUN mkdir -p /app/feed/data

# Volume target for persistent jsonl logs inside container
VOLUME ["/app/feed/data"]

# Run scraper as default entrypoint
ENTRYPOINT ["python", "-m", "feed.services.scraper"]

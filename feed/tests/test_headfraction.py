import json
import os
import tempfile
import pytest
from feed.services.headfraction import headfraction


def test_headfraction_generates_all_csvs(tmp_path) -> None:
    """Test that headfraction DuckDB processing generates all 6 target CSV files."""
    jsonl_path = tmp_path / "habr_analytics.jsonl"

    sample_records = [
        {
            "timestamp": "2026-08-12T12:00:00Z",
            "pub_id": 560001,
            "http_status": 200,
            "error_code": None,
            "post_type": "article",
            "is_seo": True,
            "article_score": 15,
            "author_score": 40,
            "hubs": ["python", "machine_learning"],
            "tags": ["python", "ai", "duckdb", "ray", "nlp", "bert"],
        },
        {
            "timestamp": "2026-08-12T12:01:00Z",
            "pub_id": 560002,
            "http_status": 200,
            "error_code": None,
            "post_type": "news",
            "is_seo": False,
            "article_score": 5,
            "author_score": 10,
            "hubs": ["python", "news"],
            "tags": ["python", "ai", "duckdb", "ray", "nlp", "bert"],
        },
        {
            "timestamp": "2026-08-12T12:02:00Z",
            "pub_id": 560003,
            "http_status": 404,
            "error_code": "NOT_FOUND",
            "post_type": None,
            "is_seo": False,
            "article_score": 0,
            "author_score": 0,
            "hubs": [],
            "tags": [],
        },
        {
            "timestamp": "2026-08-12T12:03:00Z",
            "pub_id": 560001,  # Duplicate pub_id (testing deduplication)
            "http_status": 200,
            "error_code": None,
            "post_type": "article",
            "is_seo": True,
            "article_score": 20,
            "author_score": 45,
            "hubs": ["python", "machine_learning"],
            "tags": ["python", "ai", "duckdb", "ray", "nlp", "bert"],
        },
    ]

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for rec in sample_records:
            f.write(json.dumps(rec) + "\n")

    # Run DuckDB analytics
    headfraction(str(jsonl_path))

    expected_csvs = [
        "http_statuses.csv",
        "seo_flags.csv",
        "post_types.csv",
        "hubs_frequency.csv",
        "tags_frequency.csv",
        "hub_tag_edges.csv",
    ]

    for csv_name in expected_csvs:
        csv_file = tmp_path / csv_name
        assert csv_file.exists(), f"Expected CSV file {csv_name} was not created"
        assert csv_file.stat().st_size > 0, f"CSV file {csv_name} is empty"


def test_headfraction_nonexistent_file(capsys) -> None:
    """Test graceful handling of non-existent input file."""
    headfraction("non_existent_file.jsonl")
    captured = capsys.readouterr()
    assert "Ошибка: Файл non_existent_file.jsonl не найден" in captured.out

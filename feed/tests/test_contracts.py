"""TDD Contract tests validating Pydantic v2 DTO models against raw Habr API fixtures."""

from feed.schemas.habr_models import HabrPublicationDTO
from feed.tests.factory import HabrPublicationDTOFactory


def test_parse_news_contract(raw_habr_news: dict) -> None:
    """Validate news raw payload against HabrPublicationDTO schema."""
    dto = HabrPublicationDTO.model_validate(raw_habr_news)

    assert dto.id == "1068052"
    assert dto.post_type == "news"
    assert dto.is_corporative is False
    assert dto.lang == "ru"
    assert "Claude Opus 5" in dto.title_html
    assert dto.author.alias == "denis-19"
    assert dto.author.is_seo is False
    assert dto.author.score_stats is not None
    assert dto.author.score_stats.score == 1066
    assert dto.statistics.score == 1
    assert dto.statistics.reading_count == 571
    assert dto.statistics.comments_count == 9

    # Assert arrays and flattening
    assert len(dto.hubs) == 5
    assert dto.hubs[0].alias == "artificial_intelligence"
    assert dto.hubs[0].title == "Искусственный интеллект"

    assert len(dto.flows) == 4
    assert dto.flows[0].alias == "develop"

    assert dto.tags == ["Claude Opus 5", "rm -rf"]


def test_parse_article_contract(raw_habr_article: dict) -> None:
    """Validate article raw payload against HabrPublicationDTO schema."""
    dto = HabrPublicationDTO.model_validate(raw_habr_article)

    assert dto.id == "1041202"
    assert dto.post_type == "article"
    assert dto.is_corporative is True
    assert dto.lang == "ru"
    assert "Семь примеров" in dto.title_html
    assert dto.author.alias == "T1_IT"
    assert dto.author.is_seo is False
    assert dto.author.score_stats is not None
    assert dto.author.score_stats.score == 17
    assert dto.statistics.score == 0
    assert dto.statistics.reading_count == 172
    assert dto.statistics.comments_count == 0

    # Assert arrays and flattening
    assert len(dto.hubs) == 3
    assert dto.hubs[0].alias == "T1Holding"
    assert dto.hubs[0].type == "corporative"

    assert len(dto.flows) == 2
    assert dto.flows[0].alias == "admin"

    assert dto.tags == ["базы данных", "способы применения"]


def test_polyfactory_dto_generation() -> None:
    """Test mock object generation using Polyfactory."""
    mock_dto = HabrPublicationDTOFactory.build()
    assert isinstance(mock_dto, HabrPublicationDTO)
    assert mock_dto.id is not None
    assert isinstance(mock_dto.tags, list)

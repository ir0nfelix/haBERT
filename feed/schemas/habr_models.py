"""Pydantic v2 DTO Data Contracts for Habr API Responses."""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, field_validator


class AuthorScoreStatsDTO(BaseModel):
    """Author score and karma voting statistics."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    score: int = 0
    votes_count: int = Field(default=0, alias="votesCount")


class AuthorDTO(BaseModel):
    """Author profile information."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    id: str
    alias: str
    fullname: str | None = None
    avatar_url: str | None = Field(default=None, alias="avatarUrl")
    speciality: str | None = None
    deleted: bool = False
    is_seo: bool = Field(default=False, alias="isSeo")
    score_stats: AuthorScoreStatsDTO | None = Field(default=None, alias="scoreStats")
    rating: float | None = None


class StatisticsDTO(BaseModel):
    """Publication engagement and view metrics."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    comments_count: int = Field(default=0, alias="commentsCount")
    favorites_count: int = Field(default=0, alias="favoritesCount")
    reading_count: int = Field(default=0, alias="readingCount")
    score: int = 0
    votes_count: int = Field(default=0, alias="votesCount")
    votes_count_plus: int = Field(default=0, alias="votesCountPlus")
    votes_count_minus: int = Field(default=0, alias="votesCountMinus")
    reach: int = 0
    readers: int = 0


class HubDTO(BaseModel):
    """Hub category entity."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    id: str
    alias: str
    type: str | None = None
    title: str
    title_html: str | None = Field(default=None, alias="titleHtml")
    is_profiled: bool = Field(default=False, alias="isProfiled")


class FlowDTO(BaseModel):
    """Global flow section entity."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    id: str
    alias: str
    title: str
    title_html: str | None = Field(default=None, alias="titleHtml")


class LeadDataDTO(BaseModel):
    """Publication lead announcement metadata."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    text_html: str | None = Field(default=None, alias="textHtml")
    image_url: str | None = Field(default=None, alias="imageUrl")
    button_text_html: str | None = Field(default=None, alias="buttonTextHtml")


class HabrPublicationDTO(BaseModel):
    """Root DTO model for Habr publications (Articles and News)."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    id: str
    post_type: str = Field(alias="postType")
    time_published: str = Field(alias="timePublished")
    is_corporative: bool = Field(default=False, alias="isCorporative")
    lang: str = "ru"
    title_html: str = Field(alias="titleHtml")
    lead_data: LeadDataDTO | None = Field(default=None, alias="leadData")
    text_html: str = Field(alias="textHtml")
    editor_version: str | None = Field(default=None, alias="editorVersion")
    author: AuthorDTO
    statistics: StatisticsDTO
    hubs: list[HubDTO] = Field(default_factory=list)
    flows: list[FlowDTO] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)

    @field_validator("tags", mode="before")
    @classmethod
    def extract_tags(cls, v: Any) -> list[str]:
        """Flatten nested tags dictionaries into a list of strings."""
        if isinstance(v, list):
            res = []
            for item in v:
                if isinstance(item, dict):
                    title = item.get("titleHtml") or item.get("title") or ""
                    if title:
                        res.append(title)
                elif isinstance(item, str):
                    res.append(item)
            return res
        return []

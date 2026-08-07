"""Polyfactory factories for generating mock Habr DTOs."""

from polyfactory.factories.pydantic_factory import ModelFactory
from feed.schemas.habr_models import HabrPublicationDTO


class HabrPublicationDTOFactory(ModelFactory[HabrPublicationDTO]):
    """Polyfactory ModelFactory for HabrPublicationDTO mock generation."""

    __model__ = HabrPublicationDTO

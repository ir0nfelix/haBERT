"""Habr publication parser service stub."""

from feed.schemas.habr_models import HabrPublicationDTO


class HabrParserService:
    """Service stub for parsing and processing Habr publications."""

    def parse_publication(self, raw_payload: dict) -> HabrPublicationDTO:
        """Parse raw API dictionary into HabrPublicationDTO contract."""
        return HabrPublicationDTO.model_validate(raw_payload)

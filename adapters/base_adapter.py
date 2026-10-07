from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseAuthorityAdapter(ABC):

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique identifier for the source (e.g., 'SAPS_ZA', 'INTERPOL', 'NAMUS_US')."""
        pass

    @property
    @abstractmethod
    def country_code(self) -> str:
        """ISO-2 country code (e.g., 'ZA', 'INT', 'US')."""
        pass

    @abstractmethod
    def fetch_active_external_ids(self) -> List[str]:
        """Fetches list of active cases currently published by the authority."""
        pass

    @abstractmethod
    def parse_case_details(self, external_id: str) -> Dict[str, Any]:
        """
        Parses case details into a unified payload format:
        {
            "external_id": str,
            "full_name": str,
            "gender": str,
            "eye_color": str,
            "hair_color": str,
            "country": str,
            "circumstances": str,
            "is_minor": bool,
            "raw_photo_url": str,
            "source_url": str
        }
        """
        pass

    def sort_ids_by_recency(self, ids: List[str]) -> List[str]:
        """
        Optionally re-order IDs newest-first before the ingest limit is applied.
        Default is a no-op — override in adapters that can provide ordering.
        """
        return ids
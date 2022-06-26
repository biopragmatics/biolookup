# -*- coding: utf-8 -*-

"""An in-memory backend for the Biolookup Service based on PyOBO functions."""

from typing import Any, Callable, List, Mapping, Optional

import pyobo

from .backend import Backend

__all__ = [
    "MemoryBackend",
]


class MemoryBackend(Backend):
    """A resolution service using a dictionary-based in-memory cache."""

    def __init__(
        self,
        *,
        get_id_name_mapping,
        get_id_species_mapping,
        get_alts_to_id,
        get_id_synonyms_mapping=None,
        summarize_names: Optional[Callable[[], Mapping[str, Any]]],
        summarize_alts: Optional[Callable[[], Mapping[str, Any]]] = None,
        summarize_definitions: Optional[Callable[[], Mapping[str, Any]]] = None,
        summarize_species: Optional[Callable[[], Mapping[str, Any]]] = None,
        get_id_definition_mapping=None,
    ) -> None:
        """Initialize the in-memory backend.

        :param get_id_name_mapping: A function for getting id-name mappings
        :param get_id_species_mapping: A function for getting id-species mappings
        :param get_alts_to_id: A function for getting alts-id mappings
        :param get_id_synonyms_mapping: A function for getting id-synonyms mappings
        :param summarize_names: A function for summarizing references
        :param summarize_alts: A function for summarizing alts
        :param summarize_definitions: A function for summarizing definitions
        :param summarize_species: A function for summarizing species
        :param get_id_definition_mapping: A function for getting id-def mappings
        """
        self.get_id_name_mapping = get_id_name_mapping
        self.get_id_species_mapping = get_id_species_mapping
        self.get_alts_to_id = get_alts_to_id
        self.get_id_definition_mapping = get_id_definition_mapping
        self.get_id_synonyms_mapping = get_id_synonyms_mapping or pyobo.get_id_synonyms_mapping
        self._summarize_names = summarize_names
        self._summarize_alts = summarize_alts
        self._summarize_definitions = summarize_definitions
        self._summarize_species = summarize_species

    def has_prefix(self, prefix: str) -> bool:
        """Check for the prefix using the id/name getter."""
        return self.get_id_name_mapping(prefix) is not None

    def get_primary_id(self, prefix: str, identifier: str) -> str:
        """Get the canonical identifier with the alts/id getter."""
        alts_to_id = self.get_alts_to_id(prefix) or {}
        return alts_to_id.get(identifier, identifier)

    def get_name(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the name with the id/name getter."""
        id_name_mapping = self.get_id_name_mapping(prefix) or {}
        return id_name_mapping.get(identifier)

    def get_species(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the species with the id/species getter."""
        id_species_mapping = self.get_id_species_mapping(prefix) or {}
        return id_species_mapping.get(identifier)

    def get_definition(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the name with the id/definition getter, if available."""
        if self.get_id_definition_mapping is None:
            return None
        id_definition_mapping = self.get_id_definition_mapping(prefix) or {}
        return id_definition_mapping.get(identifier)

    def get_synonyms(self, prefix: str, identifier: str) -> List[str]:
        """Get the synonyms with the id/synonym getter, if available."""
        x = self.get_id_synonyms_mapping(prefix) or {}
        return x.get(identifier, [])

    def summarize_names(self) -> Mapping[str, Any]:
        """Summarize the names with the internal name summary function, if available."""
        if self._summarize_names is None:
            return {}
        return self._summarize_names()

    def count_prefixes(self) -> int:
        """Count prefixes using the name summary."""
        return len(self.summarize_names().keys())

    def count_names(self) -> int:
        """Count names using the name summary."""
        return sum(self.summarize_names().values())

    def summarize_alts(self) -> Mapping[str, Any]:
        """Summarize the alts with the internal alt summary function, if available."""
        if self._summarize_alts is None:
            return {}
        return self._summarize_alts()

    def count_alts(self) -> int:
        """Count alts using the alt summary."""
        return sum(self.summarize_alts().values())

    def summarize_definitions(self) -> Mapping[str, Any]:
        """Summarize the definitions with the internal definition summary function, if available."""
        if self._summarize_definitions is None:
            return {}
        return self._summarize_definitions()

    def count_definitions(self) -> int:
        """Count definitions using the definition summary."""
        return sum(self.summarize_definitions().values())

    def summarize_species(self) -> Mapping[str, Any]:
        """Summarize the species with the internal species summary function, if available."""
        if self._summarize_species is None:
            return {}
        return self._summarize_species()

    def count_species(self) -> int:
        """Count species using the species summary."""
        return sum(self.summarize_species().values())

# -*- coding: utf-8 -*-

"""Base class for backends."""

import logging
from typing import Any, List, Mapping, Optional

import bioregistry
import pandas as pd

__all__ = [
    "Backend",
]


logger = logging.getLogger(__name__)


class Backend:
    """A resolution service."""

    def has_prefix(self, prefix: str) -> bool:
        """Check if there is a resource available with the given prefix."""
        raise NotImplementedError

    def get_primary_id(self, prefix: str, identifier: str) -> str:
        """Get the canonical identifier in the given resource."""
        raise NotImplementedError

    def get_name(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the canonical/preferred (english) name for the identifier in the given resource."""
        raise NotImplementedError

    def get_species(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the species for the prefix/identifier if it is species-specific."""
        raise NotImplementedError

    def get_definition(self, prefix: str, identifier: str) -> Optional[str]:
        """Get the definition associated with the prefix/identifier."""
        raise NotImplementedError

    def get_synonyms(self, prefix: str, identifier: str) -> List[str]:
        """Get a list of synonyms."""
        logger.warning(f"getting synonyms is not yet implemented for {self.__class__}")
        return []

    def get_xrefs(self, prefix: str, identifier: str) -> List[Mapping[str, str]]:
        """Get a list of xrefs."""
        logger.warning(f"getting xrefs is not yet implemented for {self.__class__}")
        return []

    def get_rels(self, prefix: str, identifier: str) -> List[Mapping[str, str]]:
        """Get a list of relations."""
        logger.warning(f"getting relations is not yet implemented for {self.__class__}")
        return []

    def summarize_names(self) -> Mapping[str, Any]:
        """Summarize the names."""
        raise NotImplementedError

    def summarize_alts(self) -> Mapping[str, Any]:
        """Summarize the alternate identifiers."""
        raise NotImplementedError

    def summarize_definitions(self) -> Mapping[str, Any]:
        """Summarize the definitions."""
        raise NotImplementedError

    def summarize_species(self) -> Mapping[str, Any]:
        """Summarize the species."""
        raise NotImplementedError

    def summarize_synonyms(self) -> Mapping[str, Any]:
        """Summarize the synonyms."""
        raise NotImplementedError

    def summarize_xrefs(self) -> Mapping[str, Any]:
        """Summarize the xrefs."""
        raise NotImplementedError

    def summarize_rels(self) -> Mapping[str, Any]:
        """Summarize the relations."""
        raise NotImplementedError

    def count_all(self):
        """Count all."""
        self.count_prefixes()
        self.count_definitions()
        self.count_alts()
        self.count_names()
        self.count_species()
        self.count_synonyms()
        self.count_xrefs()
        self.count_rels()

    def count_names(self) -> Optional[int]:
        """Count the number of names in the database."""

    def count_definitions(self) -> Optional[int]:
        """Count the number of definitions in the database."""

    def count_alts(self) -> Optional[int]:
        """Count the number of alternative identifiers in the database."""

    def count_prefixes(self) -> Optional[int]:
        """Count the number of prefixes in the database."""

    def count_species(self) -> Optional[int]:
        """Count the number of species links in the database."""

    def count_synonyms(self) -> Optional[int]:
        """Count the number of synonyms in the database."""

    def count_xrefs(self) -> Optional[int]:
        """Count the number of xrefs in the database."""

    def count_rels(self) -> Optional[int]:
        """Count the number of relations in the database."""

    def lookup(self, curie: str, *, resolve_alternate: bool = True) -> Mapping[str, Any]:
        """Return the results and summary when resolving a CURIE string."""
        prefix, identifier = bioregistry.parse_curie(curie)
        if prefix is None or identifier is None:
            return dict(
                query=curie,
                success=False,
                message="Could not identify prefix",
            )

        providers = bioregistry.get_providers(prefix, identifier)
        if not self.has_prefix(prefix):
            rv = dict(
                query=curie,
                prefix=prefix,
                identifier=identifier,
                providers=providers,
                success=False,
                message=f"Could not find id->name mapping for {prefix}",
            )
            return rv

        name = self.get_name(prefix, identifier)
        if name is None and resolve_alternate:
            identifier, _secondary_id = self.get_primary_id(prefix, identifier), identifier
            if identifier != _secondary_id:
                providers = bioregistry.get_providers(prefix, identifier)
                name = self.get_name(prefix, identifier)

        if name is None:
            return dict(
                query=curie,
                prefix=prefix,
                identifier=identifier,
                success=False,
                providers=providers,
                message="Could not look up identifier",
            )
        rv = dict(
            query=curie,
            prefix=prefix,
            identifier=identifier,
            name=name,
            success=True,
            providers=providers,
        )
        definition = self.get_definition(prefix, identifier)
        if definition:
            rv["definition"] = definition
        species = self.get_species(prefix, identifier)
        if species:
            rv["species"] = species
        synonyms = self.get_synonyms(prefix, identifier)
        if synonyms:
            rv["synonyms"] = synonyms
        xrefs = self.get_xrefs(prefix, identifier)
        if xrefs:
            rv["xrefs"] = xrefs
        rels = self.get_rels(prefix, identifier)
        if rels:
            rv["relations"] = rels

        return rv

    def summary_df(self) -> pd.DataFrame:
        """Generate a summary dataframe."""
        summary_names = self.summarize_names()
        summary_alts = self.summarize_alts() if self.summarize_alts is not None else {}
        summary_defs = (
            self.summarize_definitions() if self.summarize_definitions is not None else {}
        )
        summary_species = self.summarize_species() if self.summarize_species is not None else {}
        summary_synonyms = self.summarize_synonyms() if self.summarize_synonyms is not None else {}
        summary_xrefs = self.summarize_xrefs() if self.summarize_xrefs is not None else {}
        summary_rels = self.summarize_rels() if self.summarize_rels is not None else {}
        return pd.DataFrame(
            [
                (
                    prefix,
                    bioregistry.get_name(prefix),
                    bioregistry.get_homepage(prefix),
                    bioregistry.get_example(prefix),
                    bioregistry.get_link(prefix, bioregistry.get_example(prefix)),
                    names_count,
                    summary_alts.get(prefix, 0),
                    summary_defs.get(prefix, 0),
                    summary_species.get(prefix, 0),
                    summary_synonyms.get(prefix, 0),
                    summary_xrefs.get(prefix, 0),
                    summary_rels.get(prefix, 0),
                )
                for prefix, names_count in summary_names.items()
            ],
            columns=[
                "prefix",
                "name",
                "homepage",
                "example",
                "link",
                "names",
                "alts",
                "defs",
                "species",
                "synonyms",
                "xrefs",
                "rels",
            ],
        )

"""
Retrieve document objects for materials properties, which includes overall reference information.
"""

from abc import ABC, abstractmethod
from typing import List

from emmet.core.mpid import MPID
from emmet.core.chemenv import ChemEnvDoc
from emmet.core.dois import DOIDoc
from emmet.core.elasticity import ElasticityDoc
from emmet.core.eos import EOSDoc
from emmet.core.grain_boundary import GrainBoundaryDoc
from emmet.core.absorption import AbsorptionDoc
from emmet.core.material import MaterialsDoc
from emmet.core.magnetism import MagnetismDoc
from emmet.core.bonds import BondingDoc
from emmet.core.polar import DielectricDoc
from emmet.core.thermo import ThermoDoc

from materials_project_etl.api_client.rest_client import RestClient


class DocsAbstractClient(ABC):
    def __init__(self, rest_client: RestClient):
        self._client = rest_client


    @abstractmethod
    def get_materials_ids(self, chemsys_formula_abstract: str) -> List[MPID]:
        """
        Returns overall material ID-s from materials project
        """
        raise NotImplementedError()

    @abstractmethod
    def search_materials(self, materials_ids: List[str] | List[MPID]) -> List[MaterialsDoc]:
        """
        Get overall data for material by ID.
        """
        raise NotImplementedError()

    @abstractmethod
    def search_materials_data_in_magnetism_docs(self, materials_ids: List[str | List[MPID]]) -> List[MagnetismDoc]:
        """
        Query magnetism docs using a variety of search criteria.
        """
        raise NotImplementedError()

    @abstractmethod
    def search_materials_data_for_absorption(self, materials_ids: List[str] | List[MPID]) -> List[AbsorptionDoc]:
        """
        Query for optical absorption spectra data.
        """
        raise NotImplementedError()

    @abstractmethod
    def search_materials_for_bonds(self, materials_ids: List[str] | List[MPID]) -> List[BondingDoc]:
        """
        Query bonding docs using a variety of search criteria.
        """
        raise NotImplementedError()

    @abstractmethod
    def search_materials_for_electronic_structure_dos(self, materials_ids: List[str] | List[MPID]):
        """
        Query density of states summary data in electronic structure docs using a variety of search criteria.
        """
        raise NotImplementedError()

    @abstractmethod
    def search_materials_chemenv(self, materials_ids: List[str] | List[MPID]) -> List[ChemEnvDoc]:
        """
        Query for chemical environment data.
        """
        raise NotImplementedError()

    @abstractmethod
    def search_materials_elasticity_properties(self, materials_ids: List[str] | List[MPID]) -> List[ElasticityDoc]:
        """
        Query elasticity docs using a variety of search criteria.
        """
        raise NotImplementedError()

    @abstractmethod
    def search_materials_dielectric_properties(self, materials_ids: List[str] | List[MPID]) -> List[DielectricDoc]:
        """
        Query dielectric docs using a variety of search criteria.
        """
        raise NotImplementedError()

    @abstractmethod
    def search_materials_doi(self, materials_ids: List[str] | List[MPID]) -> List[DOIDoc]:
        """
        Query for DOI data.
        """
        raise NotImplementedError()

    @abstractmethod
    def search_materials_eos(self, materials_ids: List[str] | List[MPID]) -> List[EOSDoc]:
        """
        Query equations of state docs using a variety of search criteria.
        """
        raise NotImplementedError()

    @abstractmethod
    def search_materials_thermo_properties(self, materials_ids: List[str] | List[MPID]) -> List[ThermoDoc]:
        """
        Query core thermo docs using a variety of search criteria.
        """
        raise NotImplementedError()

    @abstractmethod
    def search_materials_grain_boundaries(self, materials_ids: List[str] | List[MPID]) -> List[GrainBoundaryDoc]:
        """
        Query grain boundary docs using a variety of search criteria.
        """
        raise NotImplementedError()


class DocsClient(DocsAbstractClient):
    def __init__(self, rest_client: RestClient):
        super().__init__(rest_client=rest_client)

    def get_materials_ids(self, chemsys_formula_abstract: str) -> List[MPID]:
        material_ids = self._client.mp_rester.get_material_ids(chemsys_formula_abstract)
        return material_ids

    def search_materials(self, materials_ids: List[str] | List[MPID]) -> List[MaterialsDoc]:
        material_data = self._client.mp_rester.materials.search(material_ids=materials_ids)
        return material_data

    def search_materials_data_in_magnetism_docs(self, materials_ids: List[str] | List[MPID]) -> List[MagnetismDoc]:
        magnetism_data = self._client.mp_rester.magnetism.search(material_ids=materials_ids)
        return magnetism_data

    def search_materials_data_for_absorption(self, materials_ids: List[str] | List[MPID]) -> List[AbsorptionDoc]:
        absorption_data = self._client.mp_rester.absorption.search(material_ids=materials_ids)
        return absorption_data

    def search_materials_for_bonds(self, materials_ids: List[str] | List[MPID]) -> List[BondingDoc]:
        bond_data = self._client.mp_rester.bonds.search(material_ids=materials_ids)
        return bond_data

    def search_materials_for_electronic_structure_dos(self, materials_ids: List[str] | List[MPID]):  # TODO
        pass

    def search_materials_chemenv(self, materials_ids: List[str] | List[MPID]) -> List[ChemEnvDoc]:
        chemenv = self._client.mp_rester.chemenv.search(material_ids=materials_ids)
        return chemenv

    def search_materials_elasticity_properties(self, materials_ids: List[str] | List[MPID]) -> List[ElasticityDoc]:
        elasticity = self._client.mp_rester.elasticity.search(material_ids=materials_ids)
        return elasticity

    def search_materials_dielectric_properties(self, materials_ids: List[str] | List[MPID]) -> List[DielectricDoc]:
        dielectric = self._client.mp_rester.dielectric.search(material_ids=materials_ids)
        return dielectric

    def search_materials_doi(self, materials_ids: List[str] | List[MPID]) -> List[DOIDoc]:
        doi = self._client.mp_rester.doi.search(material_ids=materials_ids)
        return doi

    def search_materials_eos(self, materials_ids: List[str] | List[MPID]) -> List[EOSDoc]:
        eos = self._client.mp_rester.eos.search(material_ids=materials_ids)
        return eos

    def search_materials_thermo_properties(self, materials_ids: List[str] | List[MPID]) -> List[ThermoDoc]:
        thermo = self._client.mp_rester.thermo.search(material_ids=materials_ids)
        return thermo

    def search_materials_grain_boundaries(self, materials_ids: List[str] | List[MPID]) -> List[GrainBoundaryDoc]:
        grain_boundaries = self._client.mp_rester.grain_boundaries.search(material_ids=materials_ids)
        return grain_boundaries

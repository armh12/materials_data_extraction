"""
Client for retrieving materials properties.
"""
from abc import ABC, abstractmethod
from typing import Literal, Dict, List

from emmet.core.mpid import MPID
from pymatgen.analysis.pourbaix_diagram import PourbaixEntry
from pymatgen.analysis.wulff import WulffShape
from pymatgen.core import Structure
from pymatgen.electronic_structure.bandstructure import BandStructureSymmLine
from pymatgen.electronic_structure.dos import CompleteDos
from pymatgen.entries.computed_entries import ComputedStructureEntry
from pymatgen.io.vasp import Chgcar

from materials_project_etl.api_client.rest_client import RestClient


class PropertiesAbstractClient(ABC):
    def __init__(self, rest_client: RestClient):
        self._client = rest_client

    @abstractmethod
    def get_band_structure(self, material_id: str | MPID) -> BandStructureSymmLine:
        """
        Return band structure for material ID.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_entries(self, material_id: str | MPID) -> ComputedStructureEntry | List[ComputedStructureEntry]:
        """
        Get structure entry for material ID.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_charge_density(self, material_id: str | MPID) -> Chgcar:
        """
        Get charge density for material ID.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_phonon_band_structure(self, material_id: str | MPID) -> BandStructureSymmLine:
        """
        Get phonon dispersion data corresponding to a material_id.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_structure(self, material_id: str | MPID, final: bool = True) -> Structure:
        """
        Get a Structure corresponding to a material_id.
        Args:
            final – Whether to get the final structure, or the list of initial (pre-relaxation) structures. Defaults to True.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_wulff_shape(self, material_id: str | MPID) -> WulffShape:
        """
        Constructs a Wulff shape for a material.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_pourbaix_entries(self,
                             chemsys: str | List[str],
                             solid_compat: str = "MaterialsProject2020Compatibility",
                             use_gibbs: Literal[300] | None = None) -> List[PourbaixEntry]:
        """
        A helper function to get all entries necessary to generate a Pourbaix diagram from the rest interface.
        Params:
        chemsys – Chemical system string comprising element symbols separated by dashes, e. g.,
        "Li-Fe-O" or List of element symbols, e. g., ["Li", "Fe", "O"].

        solid_compat – Compatibility scheme used to pre-process solid DFT energies prior to applying aqueous energy adjustments.
        May be passed as a class (e. g. MaterialsProject2020Compatibility) or an instance (e. g., MaterialsProject2020Compatibility()).
        If None, solid DFT energies are used as-is. Default: MaterialsProject2020Compatibility

        use_gibbs – Set to 300 (for 300 Kelvin) to use a machine learning model to estimate solid free energy from DFT energy (see GibbsComputedStructureEntry).
        This can slightly improve the accuracy of the Pourbaix diagram in some cases.
        Default: None. Note that temperatures other than 300K are not permitted here, because MaterialsProjectAqueousCompatibility corrections,
        used in Pourbaix diagram construction, are calculated based on 300 K data.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_cohesive_energy(self,
                            material_ids: list[MPID | str],
                            normalization: Literal["atom", "formula_unit"] = "atom") -> float | Dict[str, float]:
        """
        Obtain the cohesive energy of the structure(s) corresponding to multiple MPIDs.
        Params:
        material_ids – List of MPIDs to compute cohesive energies.
        normalization – Whether to normalize the cohesive energy by the number of atoms (default) or by the number of formula units.
        Note that the current default is inconsistent with the legacy API.
        Returns:
        The cohesive energies (in eV/ atom or eV/ formula unit) for each material, indexed by MPI
        """
        raise NotImplementedError()

    @abstractmethod
    def get_dos_by_material_id(self, material_id: MPID | str) -> CompleteDos:
        """
        Get the complete density of states pymatgen object associated with a Materials Project ID.
        """
        raise NotImplementedError()
    
    
class PropertiesClient(PropertiesAbstractClient):
    def __init__(self, rest_client: RestClient):
        super().__init__(rest_client=rest_client)

    def get_band_structure(self, material_id: str | MPID) -> BandStructureSymmLine:
        band_structure = self._client.mp_rester.get_bandstructure_by_material_id(material_id)
        return band_structure

    def get_entries(self, material_id: str | MPID) -> ComputedStructureEntry | List[ComputedStructureEntry]:
        entries = self._client.mp_rester.get_entries(material_id)
        return entries

    def get_charge_density(self, material_id: str | MPID) -> Chgcar:
        charge_density = self._client.mp_rester.get_charge_density_from_material_id(material_id)
        return charge_density

    def get_phonon_band_structure(self, material_id: str | MPID) -> BandStructureSymmLine:
        band_structure = self._client.mp_rester.get_phonon_bandstructure_by_material_id(material_id)
        return band_structure

    def get_structure(self, material_id: str | MPID, final: bool = True) -> Structure:
        structure = self._client.mp_rester.get_structures(material_id, final=final)
        return structure

    def get_wulff_shape(self, material_id: str | MPID) -> WulffShape:
        wulff_shape = self._client.mp_rester.get_wulff_shape(material_id)
        return wulff_shape

    def get_pourbaix_entries(self,
                             chemsys: str | List[str],
                             solid_compat: str = "MaterialsProject2020Compatibility",
                             use_gibbs: Literal[300] | None = None) -> List[PourbaixEntry]:
        pourbaix_entries = self._client.mp_rester.get_pourbaix_entries(chemsys, solid_compat, use_gibbs)
        return pourbaix_entries

    def get_cohesive_energy(self,
                            material_ids: list[MPID | str],
                            normalization: Literal["atom", "formula_unit"] = "atom") -> float | Dict[str, float]:
        cohesive_energy = self._client.mp_rester.get_cohesive_energy(material_ids, normalization)
        return cohesive_energy

    def get_dos_by_material_id(self, material_id: MPID | str) -> CompleteDos:
        dos_by_material_id = self._client.mp_rester.get_dos_by_material_id(material_id)
        return dos_by_material_id

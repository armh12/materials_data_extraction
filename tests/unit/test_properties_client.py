import pytest
from emmet.core.mpid import MPID
from pymatgen.analysis.wulff import WulffShape
from pymatgen.core import Structure
from pymatgen.electronic_structure.bandstructure import BandStructureSymmLine
from pymatgen.electronic_structure.dos import CompleteDos
from pymatgen.entries.computed_entries import ComputedStructureEntry
from pymatgen.io.vasp import Chgcar


@pytest.fixture
def material_id() -> MPID:
    return MPID("mp-2920")  # mp-2914


def test_get_band_structure(properties_client, material_id):
    band_struct = properties_client.get_band_structure(material_id)
    assert isinstance(band_struct, BandStructureSymmLine)


def test_get_entries(properties_client, material_id):
    entries = properties_client.get_entries(material_id)
    assert isinstance(entries[0], ComputedStructureEntry)


def test_charge_density(properties_client, material_id):
    charge_density = properties_client.get_charge_density(material_id)
    assert isinstance(charge_density, Chgcar)


@pytest.mark.skip(reason="No data returned")
def test_get_phonon_bandstructure(properties_client, material_id):
    bandstructure = properties_client.get_phonon_band_structure(material_id)
    assert isinstance(bandstructure, BandStructureSymmLine)


@pytest.mark.skip(reason="No data returned")
def test_get_structure(properties_client, material_id):
    structure = properties_client.get_structure(material_id)
    assert isinstance(structure, Structure)


@pytest.mark.skip(reason="No data returned")
def test_get_wulff_shape(properties_client, material_id):
    wulff_shape = properties_client.get_wulff_shape(material_id)
    assert isinstance(wulff_shape, WulffShape)


@pytest.mark.skip(reason="Not suitable")
def test_get_pourbaix_entries(properties_client, material_id):
    entries = properties_client.get_pourbaix_entries(material_id)
    assert isinstance(entries[0], ComputedStructureEntry)

@pytest.mark.skip(reason="Not working")
def test_get_cohesive_energy(properties_client, material_id):
    entries = properties_client.get_cohesive_energy([material_id])


def test_get_dos_by_material_id(properties_client, material_id):
    dos_by_material_id = properties_client.get_dos_by_material_id(material_id)
    assert isinstance(dos_by_material_id, CompleteDos)
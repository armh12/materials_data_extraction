import pytest

from emmet.core.mpid import MPID
from pymatgen.core import Composition

from materials_project_etl.transform.parsers import (
    parse_composition,
    parse_crystal_system,
    parse_structure
)

mpid = MPID("mp-2879")

@pytest.fixture(name="material_data")
def material_data_fixture(docs_client) -> dict:
    materials_info = docs_client.search_materials([MPID("mp-2879")])
    return materials_info[0].model_dump()


def test_parse_composition(material_data):
    composition: Composition = material_data["composition"]
    parsed_structure = parse_composition(composition)
    assert isinstance(parsed_structure, dict)
    req_fields = {"A", "B", "C", "A_atoms", "B_atoms", "C_atoms"}
    assert req_fields == set(parsed_structure.keys())


def test_parse_crystal_system(material_data):
    crystal_system = material_data["symmetry"]
    parsed_crystal_system = parse_crystal_system(crystal_system)
    assert isinstance(parsed_crystal_system, dict)
    req_fields = {'symbol', 'number', 'point_group', 'symprec', 'angle_tolerance', 'crystal_system'}
    assert req_fields == set(parsed_crystal_system.keys())


def test_parse_structure(material_data):
    structure = material_data["structure"]
    parsed_structure = parse_structure(structure)

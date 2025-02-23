import pytest
from emmet.core.mpid import MPID
from pyspark import Row

from materials_project_etl.transform.docs_parser import MaterialInfoParser, MagnetismParser, ThermoParser


@pytest.fixture
def material_transformer(spark, docs_client) -> MaterialInfoParser:
    material_info = docs_client.search_materials([MPID("mp-2879")])
    return MaterialInfoParser(material_info[0])

def test_get_composition(material_transformer):
    comp_row = material_transformer.get_composition()
    assert isinstance(comp_row, Row)


def test_get_crystal_system(material_transformer):
    crystal_system = material_transformer.get_crystal_system()
    assert isinstance(crystal_system, Row)


def test_get_structure(material_transformer):
    structure = material_transformer.get_structure()
    assert isinstance(structure, Row)


def test_get_entries(material_transformer):
    entries = material_transformer.get_entries()
    assert isinstance(entries, Row)

def test_get_general_material_info(material_transformer):
    general_material_info = material_transformer.get_general_material_info()
    assert isinstance(general_material_info, Row)


@pytest.fixture
def magnetism_transformer(docs_client) -> MagnetismParser:
    material_info = docs_client.search_materials_data_in_magnetism_docs([MPID("mp-2879")])
    return MagnetismParser(material_info[0])


def test_get_magnetism_info(magnetism_transformer):
    magnetism_data = magnetism_transformer.get_magnetism_info()
    assert isinstance(magnetism_data, Row)


@pytest.fixture
def thermo_transformer(docs_client) -> ThermoParser:
    thermo_info = docs_client.search_materials_thermo_properties([MPID("mp-2879")])
    return ThermoParser(thermo_info[0])


def test_get_thermo_info(thermo_transformer):
    thermo_data = thermo_transformer.get_general_thermo_data()
    assert isinstance(thermo_data, Row)

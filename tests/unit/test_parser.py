import pytest

from emmet.core.mpid import MPID

from materials_project_etl.transform.parsers import (
    parse_composition,
    parse_crystal_system,
    parse_structure,
    parse_entries,
    parse_materials_general_info,
    parse_magnetism_info,
    parse_general_thermo_info,
    parse_band_structure
)

mpid = MPID("mp-2879")


@pytest.fixture(name="material_data")
def material_data_fixture(docs_client) -> dict:
    materials_info = docs_client.search_materials([MPID("mp-2879")])
    return materials_info[0].model_dump()


def test_parse_composition(material_data):
    parsed_structure = parse_composition(material_data)
    assert isinstance(parsed_structure, dict)
    req_fields = {"A", "B", "C", "A_atoms", "B_atoms", "C_atoms", "formula"}
    assert req_fields == set(parsed_structure.keys())


def test_parse_crystal_system(material_data):
    parsed_crystal_system = parse_crystal_system(material_data)
    assert isinstance(parsed_crystal_system, dict)
    req_fields = {'symbol', 'number', 'point_group', 'symprec', 'angle_tolerance', 'crystal_system'}
    assert req_fields == set(parsed_crystal_system.keys())


def test_parse_structure(material_data):
    parsed_structure = parse_structure(material_data)
    assert isinstance(parsed_structure, dict)
    req_fields = {'label_A_1', 'a_A_1', 'b_A_1', 'c_A_1', 'x_A_1', 'y_A_1', 'z_A_1', 'label_A_2', 'a_A_2', 'b_A_2',
                  'c_A_2', 'x_A_2', 'y_A_2', 'z_A_2', 'label_A_3', 'a_A_3', 'b_A_3', 'c_A_3', 'x_A_3', 'y_A_3', 'z_A_3',
                  'label_A_4', 'a_A_4', 'b_A_4', 'c_A_4', 'x_A_4', 'y_A_4', 'z_A_4', 'label_B_1', 'a_B_1', 'b_B_1',
                  'c_B_1', 'x_B_1', 'y_B_1', 'z_B_1', 'label_B_2', 'a_B_2', 'b_B_2', 'c_B_2', 'x_B_2', 'y_B_2', 'z_B_2',
                  'label_B_3', 'a_B_3', 'b_B_3', 'x_B_3', 'y_B_3', 'label_B_4', 'a_B_4', 'b_B_4', 'x_B_4', 'y_B_4',
                  'z_B_4', 'label_C_1', 'a_C_1', 'b_C_1', 'c_C_1', 'x_C_1', 'y_C_1', 'z_C_1', 'label_C_2', 'a_C_2',
                  'b_C_2', 'c_C_2', 'x_C_2', 'y_C_2', 'z_C_2', 'label_C_3', 'a_C_3', 'b_C_3', 'c_C_3', 'x_C_3', 'y_C_3',
                  'z_C_3', 'label_C_4', 'a_C_4', 'b_C_4', 'c_C_4', 'x_C_4', 'y_C_4', 'z_C_4', 'label_C_5', 'a_C_5',
                  'b_C_5', 'c_C_5', 'x_C_5', 'y_C_5', 'z_C_5', 'label_C_6', 'a_C_6', 'b_C_6', 'c_C_6', 'x_C_6', 'y_C_6',
                  'z_C_6', 'label_C_7', 'a_C_7', 'b_C_7', 'c_C_7', 'x_C_7', 'y_C_7', 'z_C_7', 'label_C_8', 'a_C_8',
                  'b_C_8', 'c_C_8', 'x_C_8', 'y_C_8', 'z_C_8', 'label_C_9', 'a_C_9', 'b_C_9', 'c_C_9', 'x_C_9', 'y_C_9',
                  'z_C_9', 'label_C_10', 'a_C_10', 'b_C_10', 'c_C_10', 'x_C_10', 'y_C_10', 'z_C_10', 'label_C_11',
                  'a_C_11', 'b_C_11', 'c_C_11', 'x_C_11', 'y_C_11', 'z_C_11', 'label_C_12', 'a_C_12', 'b_C_12',
                  'c_C_12', 'x_C_12', 'y_C_12', 'z_C_12', 'formula', 'density', 'volume'}
    assert req_fields == set(parsed_structure.keys())


def test_parse_entries(material_data):
    parsed_entries = parse_entries(material_data)
    assert isinstance(parsed_entries, dict)
    req_fields = {'formula', 'energy', 'energy_correction', 'correction_uncertainty', 'energy_per_atom',
                  'energy_per_atom_correction', 'correction_uncertainty_per_atom', 'oxide_type', 'aspherical'}
    assert req_fields == set(parsed_entries.keys())


def test_parse_general_info(material_data):
    general_info = parse_materials_general_info(material_data)
    assert isinstance(general_info, dict)
    req_cols = {'formula', 'nsites', 'nelements', 'chemsys', 'volume', 'density', 'density_atomic'}
    assert req_cols == set(general_info.keys())


def test_parse_magnetism_info(docs_client):
    magnetism_data = docs_client.search_materials_data_in_magnetism_docs([mpid])[0].model_dump()
    magnetism_info = parse_magnetism_info(magnetism_data)
    assert isinstance(magnetism_info, dict)
    req_cols = {'formula', 'is_magnetic', 'exchange_symmetry', 'num_magnetic_sites', 'num_unique_magnetic_sites',
                'total_magnetization', 'total_magnetization_normalized_vol',
                'total_magnetization_normalized_formula_units'}
    assert req_cols == set(magnetism_info.keys())


def test_parse_general_thermo_info(docs_client):
    thermo_data = docs_client.search_materials_thermo_properties([mpid])[0].model_dump()
    thermo_info = parse_general_thermo_info(thermo_data)
    assert isinstance(thermo_info, dict)
    req_cols = {'formula', 'uncorrected_energy_per_atom', 'energy_per_atom', 'energy_uncertainy_per_atom',
                'formation_energy_per_atom', 'energy_above_hull', 'is_stable', 'equilibrium_reaction_energy_per_atom',
                'decomposition_enthalpy'}
    assert req_cols == set(thermo_info.keys())


def test_parse_band_structure(properties_client):
    band_struct = properties_client.get_band_structure(mpid)
    band_struct_parsed = parse_band_structure(band_struct)

import pytest
from emmet.core.absorption import AbsorptionDoc
from emmet.core.bonds import BondingDoc
from emmet.core.chemenv import ChemEnvDoc
from emmet.core.dois import DOIDoc
from emmet.core.elasticity import ElasticityDoc
from emmet.core.eos import EOSDoc
from emmet.core.grain_boundary import GrainBoundaryDoc
from emmet.core.magnetism import MagnetismDoc
from emmet.core.material import MaterialsDoc
from emmet.core.mpid import MPID
from emmet.core.polar import DielectricDoc
from emmet.core.qchem.molecule import MoleculeDoc
from emmet.core.thermo import ThermoDoc


def test_get_materials_ids(docs_client, formula):
    material_ids = docs_client.get_materials_ids(formula)
    assert len(material_ids) > 0
    assert all([isinstance(mpid, MPID) for mpid in material_ids])


def test_search_materials(docs_client, material_ids):
    material_info = docs_client.search_materials(material_ids)
    assert len(material_info) == 4
    assert all([isinstance(doc, MaterialsDoc) for doc in material_info])


def test_search_materials_elasticity_properties(docs_client, material_ids):
    elasticity = docs_client.search_materials_elasticity_properties(material_ids)
    assert len(elasticity) == 4
    assert all([isinstance(doc, ElasticityDoc) for doc in elasticity])


def test_search_materials_data_in_magnetism_docs(docs_client, material_ids):
    magnetism = docs_client.search_materials_data_in_magnetism_docs(material_ids)
    assert len(magnetism) == 4
    assert all([isinstance(doc, MagnetismDoc) for doc in magnetism])


def test_search_materials_data_in_materials_docs(docs_client, material_ids):
    doi = docs_client.search_materials_doi(material_ids)
    assert len(doi) == 4
    assert all([isinstance(doc, DOIDoc) for doc in doi])


@pytest.mark.skip(reason="No data returned")
def test_search_materials_eos(docs_client, material_ids):
    eos = docs_client.search_materials_eos(material_ids)
    assert len(eos) == 4
    assert all([isinstance(doc, EOSDoc) for doc in eos])


def test_search_materials_chemenv(docs_client, material_ids):
    chemenv = docs_client.search_materials_chemenv(material_ids)
    assert len(chemenv) == 4
    assert all([isinstance(doc, ChemEnvDoc) for doc in chemenv])


@pytest.mark.skip(reason="No data returned")
def test_search_materials_data_for_absorption(docs_client, material_ids):
    absorption = docs_client.search_materials_data_for_absorption(material_ids)
    assert len(absorption) == 4
    assert all([isinstance(doc, AbsorptionDoc) for doc in absorption])


@pytest.mark.skip(reason="Returned not for all materials")
def test_search_materials_dielectric_properties(docs_client, material_ids):
    dielectric = docs_client.search_materials_dielectric_properties(material_ids)
    assert len(dielectric) == 4
    assert all([isinstance(doc, DielectricDoc) for doc in dielectric])


@pytest.mark.skip(reason="No data returned")
def test_search_materials_grain_boundaries(docs_client, material_ids):
    grain_boundaries = docs_client.search_materials_grain_boundaries(material_ids)
    assert len(grain_boundaries) == 4
    assert all([isinstance(doc, GrainBoundaryDoc) for doc in grain_boundaries])


def test_search_materials_for_bonds(docs_client, material_ids):
    bonds = docs_client.search_materials_for_bonds(material_ids)
    assert len(bonds) == 4
    assert all([isinstance(doc, BondingDoc) for doc in bonds])


@pytest.mark.skip(reason="Not valid implementation provided")
def test_search_materials_molecules_properties(docs_client, material_ids):
    molecules = docs_client.search_materials_molecules_properties(material_ids)
    assert len(molecules) == 4
    assert all([isinstance(doc, MoleculeDoc) for doc in molecules])


def test_search_materials_thermo_properties(docs_client, material_ids):
    thermo = docs_client.search_materials_thermo_properties(material_ids)
    assert len(thermo) == 11
    assert all([isinstance(doc, ThermoDoc) for doc in thermo])


@pytest.mark.skip(reason="Not implemented yet")
def test_search_materials_for_electronic_structure_dos(docs_client, material_ids):
    electronic = docs_client.search_materials_for_electronic_structure_dos(material_ids)

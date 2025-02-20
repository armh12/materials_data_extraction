import pytest
from emmet.core.elasticity import ElasticityDoc
from emmet.core.material import MaterialsDoc
from emmet.core.mpid import MPID


def test_get_materials_ids(docs_client, formula):
    material_ids = docs_client.get_materials_ids(formula)
    assert len(material_ids) > 0
    assert all([isinstance(mpid, MPID) for mpid in material_ids])


def test_search_materials(docs_client, material_ids):
    material_info = docs_client.search_materials(material_ids)
    assert len(material_info) == 4
    assert all([isinstance(material_info, MaterialsDoc) for material_info in material_info])


def test_search_materials_elasticity_properties(docs_client, material_ids):
    elasticity = docs_client.search_materials_elasticity_properties(material_ids)
    print(elasticity)
    assert len(elasticity) == 4
    assert all([isinstance(elasticity, ElasticityDoc) for elasticity in elasticity])



def test_search_materials_elasticity_classes(docs_client, material_ids):
    elasticity = docs_client.search_materials_data_in_magnetism_docs(material_ids)

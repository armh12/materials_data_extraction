import pytest
from emmet.core.mpid import MPID

from materials_project_etl.transform.material_parser import MaterialInfoParser


@pytest.fixture
def transformer(spark, docs_client) -> MaterialInfoParser:
    material_info = docs_client.search_materials([MPID("mp-2879")])
    return MaterialInfoParser(spark, material_info[0])

def test_transform_material_info_doc(transformer):
    doc = transformer._prepare_doc(transformer.doc)

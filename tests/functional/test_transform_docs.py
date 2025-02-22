import pytest
from emmet.core.mpid import MPID

from materials_project_etl.transform.docs_preparation import DocsTransformer
from materials_project_etl.transform.parsers import parse_composition


@pytest.fixture
def transformer(spark) -> DocsTransformer:
    return DocsTransformer(spark)

def test_transform_material_info_doc(docs_client, transformer):
    material_info = docs_client.search_materials([MPID("mp-2879")])
    mat_info_dict = material_info[0].model_dump()
    comp = mat_info_dict["composition"]
    material_info = [doc.model_dump() for doc in material_info]
    df = transformer.get_material_info(material_info[0])
    # df = parse_composition(comp)
    print(df)

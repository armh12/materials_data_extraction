import pytest
from emmet.core.mpid import MPID

from materials_project_etl.api_client.docs_client import DocsClient
from materials_project_etl.api_client.properties_client import PropertiesClient
from materials_project_etl.configuration import build_client_configuration, get_local_spark_session

PEROVSKITE_FORMULA_CLASSIC = "ABC3"


@pytest.fixture(name="clients")
def clients_fixture():
    clients_configuration = build_client_configuration()
    return clients_configuration


@pytest.fixture(name="docs_client")
def docs_client_fixture(clients) -> DocsClient:
    return clients.docs_client


@pytest.fixture(name="properties_client")
def properties_client_fixture(clients) -> PropertiesClient:
    return clients.properties_client


@pytest.fixture(name="formula")
def formula_fixture():
    return PEROVSKITE_FORMULA_CLASSIC


@pytest.fixture(name="material_ids")
def material_ids_fixture():
    return [MPID("mp-2879"), MPID("mp-2914"), MPID("mp-2920"), MPID("mp-2928")]


@pytest.fixture(name="spark")
def spark_fixture():
    spark = get_local_spark_session()
    return spark

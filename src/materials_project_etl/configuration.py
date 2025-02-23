import os
from dotenv import load_dotenv

from materials_project_etl.api_client.rest_client import RestClient
from materials_project_etl.api_client.docs_client import DocsClient
from materials_project_etl.api_client.properties_client import PropertiesClient

from materials_project_etl.entities import Clients
from materials_project_etl.entities import Parsers
from materials_project_etl.transform.docs_parser import MaterialInfoParser, ThermoParser, MagnetismParser
from materials_project_etl.transform.properties_parsers import BandStructureParser


def build_client_configuration() -> Clients:
    load_dotenv()
    api_key = os.environ.get("API_KEY")
    rest_client = RestClient(api_key)
    docs_client = DocsClient(rest_client)
    properties_client = PropertiesClient(rest_client)
    return Clients(
        docs_client=docs_client,
        properties_client=properties_client
    )


def build_parser_configuration() -> Parsers:
    return Parsers(
        material_info_parser=MaterialInfoParser(),
        thermo_parser=ThermoParser(),
        magnetism_parser=MagnetismParser(),
        band_structure_parser=BandStructureParser(),
    )

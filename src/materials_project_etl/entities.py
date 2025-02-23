from dataclasses import dataclass

from materials_project_etl.api_client.docs_client import DocsClient
from materials_project_etl.api_client.properties_client import PropertiesClient
from materials_project_etl.transform.properties_parsers import BandStructureParser
from materials_project_etl.transform.docs_parser import (
    MaterialInfoParser,
    ThermoParser,
    MagnetismParser,
)


@dataclass
class Clients:
    docs_client: DocsClient
    properties_client: PropertiesClient


@dataclass
class Parsers:
    material_info_parser: MaterialInfoParser
    thermo_parser: ThermoParser
    magnetism_parser: MagnetismParser
    band_structure_parser: BandStructureParser

from dataclasses import dataclass

from materials_project_etl.api_client.docs_client import DocsClient
from materials_project_etl.api_client.properties_client import PropertiesClient
from materials_project_etl.transform.properties_parsers import BandStructureParser
from materials_project_etl.transform.docs_parser import (
    MaterialDataRepository,
    ThermoDataRepository,
    MagnetismDataRepository,
)


@dataclass
class Clients:
    docs_client: DocsClient
    properties_client: PropertiesClient


@dataclass
class Repositories:
    material_repo: MaterialDataRepository
    thermo_repo: ThermoDataRepository
    magnetism_repo: MagnetismDataRepository
    band_structure_parser: BandStructureParser

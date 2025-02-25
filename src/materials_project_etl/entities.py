from dataclasses import dataclass

from materials_project_etl.api_client.docs_client import DocsClient
from materials_project_etl.api_client.properties_client import PropertiesClient
from materials_project_etl.transform.properties_repository import PropertiesRepository
from materials_project_etl.transform.docs_repository import (
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
    properties_repo: PropertiesRepository

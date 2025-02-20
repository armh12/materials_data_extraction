from dataclasses import dataclass
from materials_project_etl.api_client.docs_client import DocsClient
from materials_project_etl.api_client.properties_client import PropertiesClient

@dataclass
class Clients:
    docs_client: DocsClient
    properties_client: PropertiesClient
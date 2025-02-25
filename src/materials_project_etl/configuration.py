import os
import multiprocessing
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from materials_project_etl.api_client.rest_client import RestClient
from materials_project_etl.api_client.docs_client import DocsClient
from materials_project_etl.api_client.properties_client import PropertiesClient

from materials_project_etl.entities import Clients
from materials_project_etl.entities import Repositories
from materials_project_etl.transform.docs_repository import MaterialDataRepository, ThermoDataRepository, \
    MagnetismDataRepository
from materials_project_etl.transform.properties_repository import PropertiesRepository

DEVELOPMENT_MODE = os.environ.get("DEVELOPMENT_MODE") == "true"
spark_logging_level = "WARN" if DEVELOPMENT_MODE else "INFO"
multiprocessing.set_start_method('spawn', force=True)


def get_local_spark_session(num_of_cores: int = 4) -> SparkSession:
    spark_session = (SparkSession.builder
                     .appName("MaterialsProjectETL")
                     .master(f"local[{num_of_cores}]")
                     .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
                     .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
                     .getOrCreate())
    spark_session.sparkContext.setLogLevel(spark_logging_level)
    return spark_session


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


def build_repositories_configuration(clients: Clients, spark_session: SparkSession) -> Repositories:
    return Repositories(
        material_repo=MaterialDataRepository(
            client=clients.docs_client,
            spark=spark_session
        ),
        thermo_repo=ThermoDataRepository(
            client=clients.docs_client,
            spark=spark_session
        ),
        magnetism_repo=MagnetismDataRepository(
            client=clients.docs_client,
            spark=spark_session
        ),
        properties_repo=PropertiesRepository(
            client=clients.properties_client,
            spark=spark_session
        ),
    )

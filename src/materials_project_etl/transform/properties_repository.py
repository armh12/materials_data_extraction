from typing import List
from emmet.core.mpid import MPID
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from materials_project_etl.api_client.properties_client import PropertiesClient
from materials_project_etl.transform.parsers import parse_band_structure


class PropertiesRepository:
    def __init__(self,
                 client: PropertiesClient,
                 spark: SparkSession):
        self._client = client
        self._spark = spark

    def get_band_structure(self, material_ids: List[MPID]) -> DataFrame:
        band_structures = [self._client.get_band_structure(material_id) for material_id in material_ids]
        rdd = self._spark.sparkContext.parallelize(band_structures)
        parsed_rows = rdd.map(parse_band_structure)
        band_df = self._spark.createDataFrame(parsed_rows)
        return band_df

from abc import ABC, abstractmethod
from typing import List, Tuple

import pyspark.sql as ps
from emmet.core.mpid import MPID
from pyspark.sql import SparkSession

from materials_project_etl.entities import Clients, Parsers
from materials_project_etl.processor.loader import AbstractLoader, LocalLoader


class DataHandler(ABC):
    def __init__(self,
                 clients_entity: Clients,
                 parsers_entity: Parsers,
                 loader: AbstractLoader,
                 spark_session: SparkSession,):
        self.clients_entity = clients_entity
        self.parsers_entity = parsers_entity
        self.loader = loader
        self._spark = spark_session

    @abstractmethod
    def load_material_info_data(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def load_magnetism_data(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def load_thermo_data(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def load_band_structure_data(self) -> None:
        raise NotImplementedError()

    def _split_material_ids_to_batches(self):
        ...

    def _process_material_info_data(self,
                                    material_ids_batch: List[MPID]
                                    ) -> Tuple[ps.DataFrame, ps.DataFrame, ps.DataFrame, ps.DataFrame, ps.DataFrame]:
        """
        Parse material info overall data.
        :param material_ids_batch:
        :return:
            pyspark DataFrame: Composition, Crystal System, Structure, Entries, General Information
        """
        docs = self.clients_entity.docs_client.search_materials(material_ids_batch)
        rdd_par = self._spark.sparkContext.parallelize(docs)
        composition_df = self._spark.createDataFrame(
            rdd_par.map(self.parsers_entity.material_info_parser.get_composition)
        )
        crystal_system_df = self._spark.createDataFrame(
            rdd_par.map(self.parsers_entity.material_info_parser.get_crystal_system)
        )
        structure_df = self._spark.createDataFrame(
            rdd_par.map(self.parsers_entity.material_info_parser.get_structure)
        )
        entries_df = self._spark.createDataFrame(
            rdd_par.map(self.parsers_entity.material_info_parser.get_entries)
        )
        general_info_df = self._spark.createDataFrame(
            rdd_par.map(self.parsers_entity.material_info_parser.get_general_material_info)
        )
        return composition_df, crystal_system_df, structure_df, entries_df, general_info_df

    def _process_magnetism_data(self, material_ids_batch: List[MPID]) -> ps.DataFrame:
        """
        Parse magnetism info overall data.
        :param material_ids_batch:
        :return:
            pyspark DataFrame: Magnetism Information
        """
        docs = self.clients_entity.docs_client.search_materials_data_in_magnetism_docs(material_ids_batch)
        rdd_par = self._spark.sparkContext.parallelize(docs)
        magnetism_df = self._spark.createDataFrame(
            rdd_par.map(self.parsers_entity.magnetism_parser.get_magnetism_info)
        )
        return magnetism_df

    def _process_thermo_data(self, material_ids_batch: List[MPID]) -> ps.DataFrame:
        """
        Parse thermo info overall data.
        :param material_ids_batch:
        :return:
            pyspark DataFrame: Thermo Information
        """
        docs = self.clients_entity.docs_client.search_materials_thermo_properties(material_ids_batch)
        rdd_par = self._spark.sparkContext.parallelize(docs)
        thermo_df = self._spark.createDataFrame(
            rdd_par.map(self.parsers_entity.thermo_parser.get_general_thermo_data)
        )
        return thermo_df

    def _process_band_structure(self, material_ids_batch: List[MPID]) -> ps.DataFrame:
        """
        Parse materials band structure data.
        :param material_ids_batch:
        :return:
            pyspark DataFrame: Band Structure Information
        """
        band_structs = [
            self.clients_entity.properties_client.get_band_structure(material_id)
            for material_id in material_ids_batch
        ]
        rdd_par = self._spark.sparkContext.parallelize(band_structs)
        band_structure_df = self._spark.createDataFrame(
            rdd_par.map(self.parsers_entity.band_structure_parser.parse_band_structure)
        )
        return band_structure_df


class LocalDataHandler(DataHandler):
    def __init__(self,
                 clients_entity: Clients,
                 parsers_entity: Parsers,
                 loader: LocalLoader,
                 spark_session: SparkSession,
                 chemsys_formula_abstract: str = "ABC3", ):
        super().__init__(
            clients_entity=clients_entity,
            parsers_entity=parsers_entity,
            loader=loader,
            spark_session=spark_session,
        )
        self._materials_ids = self.clients_entity.docs_client.get_materials_ids(chemsys_formula_abstract)

    def load_material_info_data(self) -> None:
        ...

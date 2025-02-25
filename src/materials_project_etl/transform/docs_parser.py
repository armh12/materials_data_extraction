from abc import ABC, abstractmethod
from typing import List, Dict, Set

from emmet.core.magnetism import MagnetismDoc
from emmet.core.material import MaterialsDoc
from emmet.core.mpid import MPID
from emmet.core.structure import StructureMetadata
from emmet.core.thermo import ThermoDoc

from pyspark.sql import Row, SparkSession
from pyspark.sql import DataFrame

from materials_project_etl.api_client.docs_client import DocsClient, DocsAbstractClient
from materials_project_etl.transform.parsers import (
    # materials
    parse_composition,
    parse_crystal_system,
    parse_structure,
    parse_entries,
    parse_materials_general_info,
    # magnetism
    parse_magnetism_info,
    # thermo
    parse_general_thermo_info
)


class AbstractMaterialRepository(ABC):
    _metadata: Set[str] = {
        "composition_reduced", "builder_meta", "origins", "warnings", "last_updated",
        "fields_not_requested", "deprecated", "deprecation_reasons"
    }
    _docs_metadata: Set[str] = set()
    _unnecessary_fields: Set[str] = set()

    def __init__(self,
                 client: DocsAbstractClient,
                 spark: SparkSession):
        self._client = client
        self._spark = spark
        self._material_docs: List[Dict] | None = None

    def _prepare_doc(self, doc: StructureMetadata):
        doc_to_dict = doc.model_dump()
        for metadata in self._metadata.union(self._docs_metadata).union(self._unnecessary_fields):
            del doc_to_dict[metadata]
        return doc_to_dict

    def _request_material_docs(self, material_ids: List[MPID]) -> List[Dict]:
        if self._material_docs is None:
            self._update_material_docs(material_ids)
        else:
            material_ids_exist = [doc["material_id"] for doc in self._material_docs]
            if set(material_ids) != set(material_ids_exist):
                self._update_material_docs(material_ids)
        return self._material_docs

    def _update_material_docs(self, material_ids: List[MPID]) -> None:
        material_docs = self.__request_for_updates(material_ids)
        material_docs_as_dict = [self._prepare_doc(doc) for doc in material_docs]
        self._material_docs = material_docs_as_dict

    @abstractmethod
    def __request_for_updates(self, material_ids: List[MPID]) -> List[StructureMetadata]:
        raise NotImplementedError()


class MaterialDataRepository(AbstractMaterialRepository):
    _docs_metadata = {
        "created_at", "calc_types", "task_types", "run_types", "task_ids", "deprecated_tasks"
    }
    _unnecessary_fields = set()

    def __init__(self,
                 client: DocsClient,
                 spark: SparkSession):
        super().__init__(
            client=client,
            spark=spark
        )
    def __request_for_updates(self, material_ids: List[MPID]) -> List[MaterialsDoc]:
        material_docs = self._client.search_materials(material_ids)
        return material_docs

    def get_composition(self, material_ids: List[MPID]) -> DataFrame:
        material_docs_as_dict = self._request_material_docs(material_ids)
        rdd = self._spark.sparkContext.parallelize(material_docs_as_dict)
        parsed_rows = rdd.map(parse_composition)
        comp_df = self._spark.createDataFrame(parsed_rows)
        return comp_df

    def get_crystal_system(self, material_ids: List[MPID]) -> DataFrame:
        material_docs_as_dict = self._request_material_docs(material_ids)
        rdd = self._spark.sparkContext.parallelize(material_docs_as_dict)
        parsed_rows = rdd.map(parse_crystal_system)
        crys_df = self._spark.createDataFrame(parsed_rows)
        return crys_df

    def get_structure(self, material_ids: List[MPID]) -> DataFrame:
        material_docs_as_dict = self._request_material_docs(material_ids)
        rdd = self._spark.sparkContext.parallelize(material_docs_as_dict)
        parsed_rows = rdd.map(parse_structure)
        structure_df = self._spark.createDataFrame(parsed_rows)
        return structure_df

    def get_entries(self, material_ids: List[MPID]) -> DataFrame:
        material_docs_as_dict = self._request_material_docs(material_ids)
        rdd = self._spark.sparkContext.parallelize(material_docs_as_dict)
        parsed_rows = rdd.map(parse_entries)
        entries_df = self._spark.createDataFrame(parsed_rows)
        return entries_df

    def get_general_material_info(self, material_ids: List[MPID]) -> DataFrame:
        material_docs_as_dict = self._request_material_docs(material_ids)
        rdd = self._spark.sparkContext.parallelize(material_docs_as_dict)
        parsed_rows = rdd.map(parse_materials_general_info)
        general_df = self._spark.createDataFrame(parsed_rows)
        return general_df


class MagnetismDataRepository(AbstractMaterialRepository):
    _docs_metadata = {
        'ordering'
    }
    _unnecessary_fields = {
        'nsites', 'elements', 'nelements', 'composition', 'formula_anonymous', 'chemsys',
        'volume', 'density', 'density_atomic', 'symmetry'
    }

    def __init__(self,
                 client: DocsClient,
                 spark: SparkSession):
        super().__init__(
            client=client,
            spark=spark
        )

    def __request_for_updates(self, material_ids: List[MPID]) -> List[MagnetismDoc]:
        magnetism_docs = self._client.search_materials_data_in_magnetism_docs(material_ids)
        return magnetism_docs

    def get_magnetism_info(self, material_ids: List[MPID]) -> DataFrame:
        magnetism_docs_as_dict = self._request_material_docs(material_ids)
        rdd = self._spark.sparkContext.parallelize(magnetism_docs_as_dict)
        parsed_rows = rdd.map(parse_magnetism_info)
        magn_df = self._spark.createDataFrame(parsed_rows)
        return magn_df


class ThermoDataRepository(AbstractMaterialRepository):
    _docs_metadata = {}
    _unnecessary_fields = {
        'nsites', 'elements', 'nelements', 'composition', 'formula_anonymous', 'chemsys',
        'volume', 'density', 'density_atomic', 'symmetry', 'entries'
    }
    def __request_for_updates(self, material_ids: List[MPID]) -> List[ThermoDoc]:
        thermo_docs = self._client.search_materials_thermo_properties(material_ids)
        return thermo_docs

    def get_general_thermo_data(self, material_ids: List[MPID]) -> DataFrame:
        thermo_docs_as_dict = self._request_material_docs(material_ids)
        rdd = self._spark.sparkContext.parallelize(thermo_docs_as_dict)
        parsed_rows = rdd.map(parse_general_thermo_info)
        thermo_df = self._spark.createDataFrame(parsed_rows)
        return thermo_df

    def get_decomposition_enthalpy_materials(self) -> Row: # TODO implement
        pass

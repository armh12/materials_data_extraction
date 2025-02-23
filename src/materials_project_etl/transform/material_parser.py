from typing import List

from emmet.core.material import MaterialsDoc
from emmet.core.mpid import MPID
from emmet.core.symmetry import CrystalSystem
from pymatgen.core import Structure, Composition
from pymatgen.entries.computed_entries import ComputedStructureEntry

from pyspark.sql import SparkSession, Row

from materials_project_etl.transform.parsers import (
    parse_composition,
    parse_crystal_system,
    parse_structure
)


class MaterialInfoParser:
    _materials_info_metadata = [
        "composition_reduced", "builder_meta", "task_ids", "deprecated_tasks",
        "calc_types", "origins", "warnings", "task_types", "created_at",
        "last_updated", "run_types", "fields_not_requested", "deprecated",
        "deprecation_reasons"
    ]

    def __init__(self,
                 spark: SparkSession,
                 doc: MaterialsDoc):
        self.spark = spark
        self.material_info = self._prepare_doc(doc)

    def _prepare_doc(self, doc: MaterialsDoc):
        doc_to_dict = doc.model_dump()
        for metadata in self._materials_info_metadata:
            del doc_to_dict[metadata]
        return doc_to_dict

    def get_composition(self) -> Row:
        composition = self.material_info.pop("composition")
        parsed_composition = parse_composition(composition)
        return Row(**parsed_composition)

    def get_crystal_system(self) -> Row:
        crystal_system = self.material_info.pop("symmetry")
        parsed_crystal_system = parse_crystal_system(crystal_system)
        return Row(**parsed_crystal_system)

    def get_structure(self) -> Row:
        structure = self.material_info.pop("structure")
        parsed_structure = parse_structure(structure)
        return Row(**parsed_structure)

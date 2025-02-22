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


class DocsTransformer:
    _materials_info_metadata = [
        "composition_reduced", "builder_meta", "task_ids", "deprecated_tasks",
        "calc_types", "origins", "warnings", "task_types", "created_at",
        "last_updated", "run_types", "fields_not_requested", "deprecated",
        "deprecation_reasons"
    ]

    def __init__(self,
                 spark: SparkSession):
        self.spark = spark

    def get_material_info(self, doc: dict) -> Row:
        # extract properties which need to be parsed
        composition: Composition = doc.pop("composition")
        symmetry = doc.pop("symmetry")
        structure: Structure = doc.pop("structure")
        initial_structures: Structure = doc.pop("initial_structures")
        entries: ComputedStructureEntry = doc.pop("entries")
        material_id: MPID = doc.pop("material_id")
        # delete all unnecessary metadata
        for metadata in self._materials_info_metadata:
            del doc[metadata]
        del doc["elements"]  # will parse in composition
        doc["material_id"] = material_id.string
        comp_row = parse_composition(composition)
        sym_row = parse_crystal_system(symmetry)
        struct_row = parse_structure(structure)


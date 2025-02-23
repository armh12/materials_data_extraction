from abc import ABC, abstractmethod

from emmet.core.magnetism import MagnetismDoc
from emmet.core.material import MaterialsDoc
from emmet.core.structure import StructureMetadata
from emmet.core.thermo import ThermoDoc
from pyspark.sql import Row

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


class AbstractMaterialParser(ABC):
    _metadata = {
        "composition_reduced", "builder_meta", "origins", "warnings", "last_updated",
        "fields_not_requested", "deprecated", "deprecation_reasons"
    }

    @staticmethod
    def _update_id(material_info: dict, data: dict):
        material_id = material_info["material_id"].string
        data["id"] = material_id
        return data

    @abstractmethod
    def _prepare_doc(self, doc: StructureMetadata):
        raise NotImplementedError()


class MaterialInfoParser(AbstractMaterialParser):
    _materials_info_metadata = {
        "created_at", "calc_types", "task_types", "run_types", "task_ids", "deprecated_tasks"
    }

    def _prepare_doc(self, doc: MaterialsDoc):
        doc_to_dict = doc.model_dump()
        for metadata in self._metadata.union(self._materials_info_metadata):
            del doc_to_dict[metadata]
        return doc_to_dict

    def get_composition(self, doc: MaterialsDoc) -> Row:
        material_info = self._prepare_doc(doc)
        composition = material_info.pop("composition")
        parsed_composition = parse_composition(composition)
        parsed_composition = self._update_id(parsed_composition, material_info)
        return Row(**parsed_composition)

    def get_crystal_system(self, doc: MaterialsDoc) -> Row:
        material_info = self._prepare_doc(doc)
        crystal_system = material_info.pop("symmetry")
        parsed_crystal_system = parse_crystal_system(crystal_system)
        parsed_crystal_system = self._update_id(parsed_crystal_system, material_info)
        return Row(**parsed_crystal_system)

    def get_structure(self, doc: MaterialsDoc) -> Row:
        material_info = self._prepare_doc(doc)
        structure = material_info.pop("structure")
        parsed_structure = parse_structure(structure)
        parsed_structure = self._update_id(parsed_structure, material_info)
        return Row(**parsed_structure)

    def get_entries(self, doc: MaterialsDoc) -> Row:
        material_info = self._prepare_doc(doc)
        entries = material_info.pop("entries")
        parsed_entries = parse_entries(entries)
        parsed_entries = self._update_id(parsed_entries, material_info)
        return Row(**parsed_entries)

    def get_general_material_info(self, doc: MaterialsDoc) -> Row:
        material_info = self._prepare_doc(doc)
        general_info = parse_materials_general_info(material_info)
        general_info = self._update_id(general_info, material_info)
        return Row(**general_info)


class MagnetismParser(AbstractMaterialParser):
    _magnetism_metadata = {
        'ordering'
    }
    _unnecessary_fields = {
        'nsites', 'elements', 'nelements', 'composition', 'formula_anonymous', 'chemsys',
        'volume', 'density', 'density_atomic', 'symmetry'
    }

    def _prepare_doc(self, doc: MagnetismDoc):
        doc_to_dict = doc.model_dump()
        for metadata in self._metadata.union(self._magnetism_metadata).union(self._unnecessary_fields):
            del doc_to_dict[metadata]
        return doc_to_dict

    def get_magnetism_info(self, doc: MagnetismDoc) -> Row:
        magnetism_info = self._prepare_doc(doc)
        magnetism_data = parse_magnetism_info(magnetism_info)
        magnetism_data = self._update_id(magnetism_data, magnetism_info)
        return Row(**magnetism_data)


class ThermoParser(AbstractMaterialParser):
    _thermo_metadata = {}
    _unnecessary_fields = {
        'nsites', 'elements', 'nelements', 'composition', 'formula_anonymous', 'chemsys',
        'volume', 'density', 'density_atomic', 'symmetry', 'entries'
    }

    def _prepare_doc(self, doc: ThermoDoc):
        doc_to_dict = doc.model_dump()
        for metadata in self._metadata.union(self._thermo_metadata).union(self._unnecessary_fields):
            del doc_to_dict[metadata]
        return doc_to_dict

    def get_general_thermo_data(self, doc: ThermoDoc) -> Row:
        thermo_info = self._prepare_doc(doc)
        general_thermo_info = parse_general_thermo_info(thermo_info)
        general_thermo_data = self._update_id(general_thermo_info, thermo_info)
        return Row(**general_thermo_data)

    def get_decomposition_enthalpy_materials(self) -> Row: # TODO implement
        pass

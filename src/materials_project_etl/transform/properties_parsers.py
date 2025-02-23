from pymatgen.electronic_structure.bandstructure import BandStructureSymmLine

from materials_project_etl.transform.parsers import parse_band_structure


class BandStructureParser:
    @staticmethod
    def parse_band_structure(band_structure: BandStructureSymmLine):
        parsed_band_structure = parse_band_structure(band_structure)
        return parsed_band_structure

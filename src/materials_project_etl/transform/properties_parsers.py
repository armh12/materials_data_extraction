from emmet.core.mpid import MPID
from pymatgen.electronic_structure.bandstructure import BandStructureSymmLine

from materials_project_etl.transform.properties_parser import parse_band_structure


class BandStructureParser:
    def __init__(self,
                 band_structure: BandStructureSymmLine,
                 material_id: str | MPID):
        self.band_structure = band_structure
        material_id = material_id.string if isinstance(material_id, MPID) else material_id
        self.material_id = int(material_id.replace("mp-", "").strip())

    def parse_band_structure(self):
        parsed_band_structure = parse_band_structure(self.band_structure)
        parsed_band_structure["id"] = self.material_id
        return parsed_band_structure

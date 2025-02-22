from emmet.core.symmetry import CrystalSystem
from pymatgen.core import Composition, Structure


def parse_composition(composition: Composition) -> dict:
    composition = composition.as_dict()
    parsed_composition = {}
    for i, elem_name in enumerate(composition):
        if i == 0:
            parsed_composition["A"] = elem_name
            parsed_composition["A_atoms"] = composition[elem_name]
        elif i == 1:
            parsed_composition["B"] = elem_name
            parsed_composition["B_atoms"] = composition[elem_name]
        elif i == 2:
            parsed_composition["C"] = elem_name
            parsed_composition["C_atoms"] = composition[elem_name]

    return parsed_composition


def parse_crystal_system(crystal_system: dict) -> dict:
    _crystal_system: CrystalSystem = crystal_system.pop("crystal_system")
    crystal_system["crystal_system"] = _crystal_system.value
    del crystal_system["version"]
    return crystal_system


def parse_structure(structure: Structure) -> dict:
    structure = structure.as_dict()
    sites = structure.pop("sites")
    for specie in sites:
        ...
    parsed_structure = {}
from emmet.core.symmetry import CrystalSystem
from pymatgen.core import Composition, Structure
from collections import OrderedDict


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
    def __group_by_label(data):
        grouped = {}
        for atom in data:
            label = atom['label']
            atom_info = {
                'abc': atom['abc'],
                'properties': atom['properties'],
                'xyz': atom['xyz'],
            }
            if label not in grouped:
                grouped[label] = []
            grouped[label].append(atom_info)
        return grouped

    structure = structure.as_dict()
    sites = structure.pop("sites")
    # define elements order in structure
    species = [site["species"] for site in sites]
    elements = []
    for specie in species:
        element = specie[0]["element"]
        if element not in elements:
            elements.append(element)
    order = {
        1: "A",
        2: "B",
        3: "C",
    }
    elements = {element: order[i] for i, element in enumerate(elements, start=1)}

    grouped_sites = __group_by_label(sites)
    for label, sites in grouped_sites.items():
        label_order = elements[label]
        for i, site in enumerate(sites, start=1):
            label_order_atomic = f'{label_order}_{i}'
            abc = site["abc"]

            print(label_order_atomic)
            print(site)
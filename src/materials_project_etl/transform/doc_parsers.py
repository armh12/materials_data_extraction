from emmet.core.symmetry import CrystalSystem
from pymatgen.core import Composition, Structure
from pymatgen.entries.computed_entries import ComputedStructureEntry


def parse_composition(composition: Composition) -> dict:
    formula = composition.formula
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
    parsed_composition["formula"] = formula
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

    def __parse_abc(abc: list, atomic_label: str) -> dict:
        abc_dict = {}
        order = {
            0: f"a_{atomic_label}",
            1: f"b_{atomic_label}",
            2: f"c_{atomic_label}",
        }
        for num in abc:
            abc_dict[order[abc.index(num)]] = num
        return abc_dict

    def __parse_xyz(xyz: list, atomic_label: str) -> dict:
        xyz_dict = {}
        order = {
            0: f"x_{atomic_label}",
            1: f"y_{atomic_label}",
            2: f"z_{atomic_label}",
        }
        for num in xyz:
            xyz_dict[order[xyz.index(num)]] = num
        return xyz_dict

    formula = structure.formula
    volume = structure.volume
    density = structure.density
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
    parsed_structures = {}
    for label, sites in grouped_sites.items():
        label_order = elements[label]
        for i, site in enumerate(sites, start=1):
            label_order_atomic = f'{label_order}_{i}'
            abc = __parse_abc(site["abc"], label_order_atomic)
            xyz = __parse_xyz(site["xyz"], label_order_atomic)
            labels_dict = {f"label_{label_order_atomic}": label}
            combined_dict = dict(**labels_dict, **abc, **xyz)
            parsed_structures.update(combined_dict)
    parsed_structures["formula"] = formula
    parsed_structures["volume"] = volume
    parsed_structures["density"] = density
    return parsed_structures


def parse_entries(entries: dict) -> dict:
    entry: ComputedStructureEntry = entries['GGA']
    parsed_entry = {
        "formula": entry.formula,
        "energy": entry.energy,
        "energy_correction": entry.correction,
        "correction_uncertainty": entry.correction_uncertainty,
        "energy_per_atom": entry.energy_per_atom,
        "energy_per_atom_correction": entry.correction_per_atom,
        "correction_uncertainty_per_atom": entry.correction_uncertainty_per_atom
    }
    entry_data = entry.data
    parsed_entry["oxide_type"] = entry_data["oxide_type"]
    parsed_entry["aspherical"] = entry_data["aspherical"]
    return parsed_entry


def parse_materials_general_info(material_info: dict) -> dict:
    general_info = {
        "formula": material_info["formula_pretty"],
        "nsites": material_info["nsites"],
        "nelements": material_info["nelements"],
        "chemsys": material_info["chemsys"],
        "volume": material_info["volume"],
        "density": material_info["density"],
        "density_atomic": material_info["density_atomic"]
    }
    return general_info


def parse_magnetism_info(magnetism_info: dict) -> dict:
    parsed_magnetism_info = {
        "formula": magnetism_info["formula_pretty"],
        "is_magnetic": magnetism_info["is_magnetic"],
        "exchange_symmetry": magnetism_info["exchange_symmetry"],
        "num_magnetic_sites": magnetism_info["num_magnetic_sites"],
        "num_unique_magnetic_sites": magnetism_info["num_unique_magnetic_sites"],
        "total_magnetization": magnetism_info["total_magnetization"],
        "total_magnetization_normalized_vol": magnetism_info["total_magnetization_normalized_vol"],
        "total_magnetization_normalized_formula_units": magnetism_info["total_magnetization_normalized_formula_units"],
    }
    return parsed_magnetism_info


def parse_general_thermo_info(thermo_info: dict) -> dict:
    parsed_thermo_info = {
        'formula': thermo_info["formula_pretty"],
        'uncorrected_energy_per_atom': thermo_info['uncorrected_energy_per_atom'],
        'energy_per_atom': thermo_info['energy_per_atom'],
        'energy_uncertainy_per_atom': thermo_info['energy_uncertainy_per_atom'],
        'formation_energy_per_atom': thermo_info['formation_energy_per_atom'],
        'energy_above_hull': thermo_info['energy_above_hull'],
        'is_stable': thermo_info['is_stable'],
        'equilibrium_reaction_energy_per_atom': thermo_info['equilibrium_reaction_energy_per_atom'],
        'decomposition_enthalpy': thermo_info['decomposition_enthalpy'],
    }
    return parsed_thermo_info

def parse_decomposition_enthalpy_materials(decomposition_enthalpy_materials: dict) -> dict:
    ...

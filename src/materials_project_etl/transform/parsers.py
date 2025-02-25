from emmet.core.mpid import MPID
from emmet.core.symmetry import CrystalSystem
from pymatgen.core import Composition, Structure
from pymatgen.entries.computed_entries import ComputedStructureEntry
from pymatgen.electronic_structure.bandstructure import BandStructureSymmLine
from pyspark import Row


def parse_composition(material_doc_as_dict: dict) -> Row:
    composition: Composition = material_doc_as_dict.pop("composition")
    formula = composition.formula
    composition_dict = composition.as_dict()
    parsed_composition = {}
    for i, elem_name in enumerate(composition_dict):
        if i == 0:
            parsed_composition["A"] = elem_name
            parsed_composition["A_atoms"] = composition_dict[elem_name]
        elif i == 1:
            parsed_composition["B"] = elem_name
            parsed_composition["B_atoms"] = composition_dict[elem_name]
        elif i == 2:
            parsed_composition["C"] = elem_name
            parsed_composition["C_atoms"] = composition_dict[elem_name]
    parsed_composition["formula"] = formula
    parsed_composition = _update_id(material_doc_as_dict, parsed_composition)
    return Row(**parsed_composition)


def parse_crystal_system(material_doc_as_dict: dict) -> Row:
    crystal_system = material_doc_as_dict.pop("crystal_system")
    _crystal_system: CrystalSystem = crystal_system.pop("crystal_system")
    crystal_system["crystal_system"] = _crystal_system.value
    del crystal_system["version"]
    crystal_system = _update_id(material_doc_as_dict, crystal_system)
    return Row(**crystal_system)


def parse_structure(material_doc_as_dict: dict) -> Row:
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
    structure: Structure = material_doc_as_dict.pop("structure")
    formula = structure.formula
    volume = structure.volume
    density = structure.density
    structure_dict = structure.as_dict()
    sites = structure_dict.pop("sites")
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
    parsed_structures = _update_id(material_doc_as_dict, parsed_structures)
    return Row(**parsed_structures)


def parse_entries(material_doc_as_dict: dict) -> Row:
    entries = material_doc_as_dict.pop("entries")
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
    parsed_entry = _update_id(material_doc_as_dict, parsed_entry)
    return Row(**parsed_entry)


def parse_materials_general_info(material_doc_as_dict: dict) -> Row:
    general_info = {
        "formula": material_doc_as_dict["formula_pretty"],
        "nsites": material_doc_as_dict["nsites"],
        "nelements": material_doc_as_dict["nelements"],
        "chemsys": material_doc_as_dict["chemsys"],
        "volume": material_doc_as_dict["volume"],
        "density": material_doc_as_dict["density"],
        "density_atomic": material_doc_as_dict["density_atomic"]
    }
    general_info = _update_id(material_doc_as_dict, general_info)
    return Row(**general_info)


def parse_magnetism_info(magnetism_data_as_dict: dict) -> Row:
    parsed_magnetism_info = {
        "formula": magnetism_data_as_dict["formula_pretty"],
        "is_magnetic": magnetism_data_as_dict["is_magnetic"],
        "exchange_symmetry": magnetism_data_as_dict["exchange_symmetry"],
        "num_magnetic_sites": magnetism_data_as_dict["num_magnetic_sites"],
        "num_unique_magnetic_sites": magnetism_data_as_dict["num_unique_magnetic_sites"],
        "total_magnetization": magnetism_data_as_dict["total_magnetization"],
        "total_magnetization_normalized_vol": magnetism_data_as_dict["total_magnetization_normalized_vol"],
        "total_magnetization_normalized_formula_units": magnetism_data_as_dict["total_magnetization_normalized_formula_units"],
    }
    parsed_magnetism_info = _update_id(magnetism_data_as_dict, parsed_magnetism_info)
    return Row(**parsed_magnetism_info)


def parse_general_thermo_info(thermo_data_as_dict: dict) -> Row:
    parsed_thermo_info = {
        'formula': thermo_data_as_dict["formula_pretty"],
        'uncorrected_energy_per_atom': thermo_data_as_dict['uncorrected_energy_per_atom'],
        'energy_per_atom': thermo_data_as_dict['energy_per_atom'],
        'energy_uncertainy_per_atom': thermo_data_as_dict['energy_uncertainy_per_atom'],
        'formation_energy_per_atom': thermo_data_as_dict['formation_energy_per_atom'],
        'energy_above_hull': thermo_data_as_dict['energy_above_hull'],
        'is_stable': thermo_data_as_dict['is_stable'],
        'equilibrium_reaction_energy_per_atom': thermo_data_as_dict['equilibrium_reaction_energy_per_atom'],
        'decomposition_enthalpy': thermo_data_as_dict['decomposition_enthalpy'],
    }
    parsed_thermo_info = _update_id(thermo_data_as_dict, parsed_thermo_info)
    return Row(**parsed_thermo_info)


def parse_decomposition_enthalpy_materials(decomposition_enthalpy_materials: dict) -> dict:
    ...


def parse_band_structure(band_structure: BandStructureSymmLine) -> Row:
    parsed_band_structure = {
        'formula': band_structure.structure.reduced_formula,
        'num_of_bands': band_structure.nb_bands,
        'band_gap': band_structure.get_band_gap(),
        'fermi_level': band_structure.efermi,
        'spin_polarized': band_structure.is_spin_polarized,
        'metal': band_structure.is_metal,
    }
    return Row(**parsed_band_structure)


def _update_id(material_data: dict, data: dict):
    material_id: MPID = material_data["material_id"].string
    id_ = material_id.replace("mp-", "").strip()
    data["id"] = id_
    return data

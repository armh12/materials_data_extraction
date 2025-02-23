from pymatgen.electronic_structure.bandstructure import BandStructureSymmLine


def parse_band_structure(band_structure: BandStructureSymmLine) -> dict:
    parsed_band_structure = {
        'formula': band_structure.structure.reduced_formula,
        'num_of_bands': band_structure.nb_bands,
        'band_gap': band_structure.get_band_gap(),
        'fermi_level': band_structure.efermi,
        'spin_polarized': band_structure.is_spin_polarized,
        'metal': band_structure.is_metal,

    }
    return parsed_band_structure

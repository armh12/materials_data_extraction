import pandas as pd
import pytest
from pyspark.sql.dataframe import DataFrame

from materials_project_etl.transform.docs_repository import MaterialDataRepository, MagnetismDataRepository, \
    ThermoDataRepository
from materials_project_etl.transform.properties_repository import PropertiesRepository

pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 1000)


@pytest.fixture
def material_transformer(spark, docs_client) -> MaterialDataRepository:
    return MaterialDataRepository(docs_client, spark)


def test_get_composition(material_transformer, material_ids):
    comp_df = material_transformer.get_composition(material_ids)
    assert isinstance(comp_df, DataFrame)
    print("Composition\n", comp_df.toPandas())


def test_get_crystal_system(material_transformer, material_ids):
    crystal_system_df = material_transformer.get_crystal_system(material_ids)
    assert isinstance(crystal_system_df, DataFrame)
    print("Crystal System\n", crystal_system_df.toPandas())


def test_get_structure(material_transformer, material_ids):
    structure_df = material_transformer.get_structure(material_ids)
    assert isinstance(structure_df, DataFrame)
    print("Structure\n", structure_df.toPandas())


def test_get_entries(material_transformer, material_ids):
    entries_df = material_transformer.get_entries(material_ids)
    assert isinstance(entries_df, DataFrame)
    print("Entries\n", entries_df.toPandas())


def test_get_general_material_info(material_transformer, material_ids):
    general_material_info_df = material_transformer.get_general_material_info(material_ids)
    assert isinstance(general_material_info_df, DataFrame)
    print("General Material Info\n", general_material_info_df.toPandas())


@pytest.fixture
def magnetism_transformer(docs_client, spark) -> MagnetismDataRepository:
    return MagnetismDataRepository(docs_client, spark)


def test_get_magnetism_info(magnetism_transformer, material_ids):
    magnetism_data_df = magnetism_transformer.get_magnetism_info(material_ids)
    assert isinstance(magnetism_data_df, DataFrame)
    print("Magnetism Data\n", magnetism_data_df.toPandas())


@pytest.fixture
def thermo_transformer(docs_client, spark) -> ThermoDataRepository:
    return ThermoDataRepository(docs_client, spark)


def test_get_thermo_info(thermo_transformer, material_ids):
    thermo_data_df = thermo_transformer.get_general_thermo_data(material_ids)
    assert isinstance(thermo_data_df, DataFrame)
    print("Thermo Data\n", thermo_data_df.toPandas())


@pytest.fixture
def properties_transformer(properties_client, spark) -> PropertiesRepository:
    return PropertiesRepository(properties_client, spark)


def test_get_band_structure(properties_transformer, material_ids):
    band_structure_df = properties_transformer.get_band_structure(material_ids)
    assert isinstance(band_structure_df, DataFrame)
    print("Band Structure\n", band_structure_df.toPandas())

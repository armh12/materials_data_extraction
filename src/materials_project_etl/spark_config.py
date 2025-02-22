import os
from pyspark.sql import SparkSession

DEVELOPMENT_MODE = os.environ.get("DEVELOPMENT_MODE") == "true"

spark_logging_level = "WARN" if DEVELOPMENT_MODE else "INFO"


def get_local_spark_session(num_of_cores: int = 4) -> SparkSession:
    spark_session = (SparkSession.builder
                     .appName("MaterialsProjectETL")
                     .master(f"local[{num_of_cores}]")
                     .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
                     .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
                     .getOrCreate())
    spark_session.sparkContext.setLogLevel(spark_logging_level)
    return spark_session


def get_cluster_spark_session(num_of_cores: int = 4) -> SparkSession:
    ...


get_local_spark_session()

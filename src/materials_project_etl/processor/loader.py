import os
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession
import pyspark.sql as ps


class AbstractLoader(ABC):
    def __init__(self,
                 spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def download_dataframe(self, filepath: str) -> ps.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def update_dataframe(self, base_df: ps.DataFrame, new_df: ps.DataFrame) -> ps.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def upload_dataframe(self, df: ps.DataFrame, filepath: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def update_and_upload_dataframe(self, df: ps.DataFrame, filepath: str) -> None:
        raise NotImplementedError()


class LocalLoader(AbstractLoader):
    def __init__(self,
                 spark: SparkSession,
                 root_dir: str = ""):
        super().__init__(spark)
        self.root_dir = root_dir

    def download_dataframe(self, filepath: str) -> ps.DataFrame:
        df = self.spark.read.parquet(filepath)
        return df

    def update_dataframe(self, base_df: ps.DataFrame, new_df: ps.DataFrame) -> ps.DataFrame:
        df = base_df.union(new_df)
        return df

    def upload_dataframe(self, df: ps.DataFrame, filepath: str) -> None:
        df.write.parquet(os.path.join(self.root_dir, filepath), mode='overwrite')

    def update_and_upload_dataframe(self, df: ps.DataFrame, filepath: str) -> None:
        if os.path.exists(filepath):
            base_df = self.spark.read.parquet(os.path.join(self.root_dir, filepath))
            df = self.update_dataframe(base_df, df)
        self.upload_dataframe(df, os.path.join(self.root_dir, filepath))


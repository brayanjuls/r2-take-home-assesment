from datetime import datetime
from typing import NamedTuple
from ingestion.adapter.pg import PostgresConnector
from ingestion.adapter.file_system import FileSystemConnector


class SourceConfig(NamedTuple):
    root_path: str
    batch_date: str
    file_name: str
    file_date_format: str
    columns: dict


class TargetConfig(NamedTuple):
    table_name: str
    db: str
    host: str
    port: str
    user: str
    password: str


class MKExtractor:
    """
        This class represent the extractions for a specific marketplace and source
    """

    def __init__(self, pg_connector: PostgresConnector, fs_connector: FileSystemConnector,
                 source_config: SourceConfig, target_config: TargetConfig):
        self.pg_connector = pg_connector
        self.fs_connector = fs_connector
        self.source_config = source_config
        self.target_config = target_config

    def extract(self, batch_date: str, file_date_format: str, file_name: str, columns: dict):
        file_paths = self.fs_connector.get_file_path(batch_date,
                                                     file_date_format,
                                                     file_name)
        names = [column_pair.split(":")[0] for column_pair in columns]
        df = self.fs_connector.read_csv(file_paths, names)
        return df

    def general_formatting(self, df, batch_date):
        df["batch_date"] = datetime.strptime(batch_date, "%Y-%m-%d").date()
        df["processing_date"] = datetime.now().date()
        return df

    def load(self, df, table_name):
        self.pg_connector.write_to_postgres(df, table_name)
        return True

    def extract_load(self):
        sales_df = self.extract(self.source_config.batch_date,
                                self.source_config.file_date_format,
                                self.source_config.file_name,
                                self.source_config.columns)
        formatted_df = self.general_formatting(sales_df, self.source_config.batch_date)
        return self.load(formatted_df, self.target_config.table_name)

"""Connector and methods accessing postgres database"""
from pandas import DataFrame
from sqlalchemy import create_engine
import logging


class PostgresConnector:
    """
        Class for interacting with postgresSQL
    """

    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        db = create_engine(self.conn_string)
        self.conn = db.connect()
        self.logger = logging.getLogger(__name__)

    def write_to_postgres(self, df: DataFrame, table_name):
        if df.empty:
            self.logger.info('The dataframe is empty! No file will be written!')
            return None
        df.to_sql(table_name, con=self.conn, if_exists='append', index=False, chunksize=1000)

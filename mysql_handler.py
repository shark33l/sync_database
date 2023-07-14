from sqlalchemy import create_engine, update, delete
from sqlalchemy import MetaData
from sqlalchemy.dialects import mysql as mysqlal
import utils
import pandas as pd

logger = utils.create_logger(__name__)
main_configs = utils.get_config("main_config.json")


# mySQL Connector Class
class MySQLConnector:

    # Chunk size to query
    chunk_size = main_configs.get("chunk_size")

    # Constructor
    def __init__(self, host, user, password, database, table, port=3306):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.table: str = table
        self.tableObject = None
        self.conn_engine = self.__create_conn_engine()

    # Create mySQL connection
    def __create_conn_engine(self):
        # Create connection string using DB Configs
        conn_string = f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'

        # Create connection pools
        try:
            # Create Engine
            engine = create_engine(conn_string, echo=False)

            # Get table object of relevant table and set
            meta = MetaData()
            meta.reflect(bind=engine)
            self.tableObject = meta.tables[self.table]

            engine_connect = engine.connect().execution_options(stream_results=True)
            return engine_connect
        except Exception as exc:
            logger.exception("Error while creating mySQL connection pools.")
            raise exc

    # Query table for all rows
    def query_table(self):
        try:
            query_result_chunks = pd.read_sql_table(
                table_name=self.table,
                con=self.conn_engine,
                chunksize=self.chunk_size
            )

            return query_result_chunks
        except Exception as exc:
            logger.exception(f"Error when querying for table - {self.database}.{self.table}")
            raise exc


class SyncMySQLConnector(MySQLConnector):
    # Constructor
    def __init__(self, host, user, password, database, table, join_key, port=3306):
        super().__init__(host, user, password, database, table, port)
        self.join_key = join_key


# Inherits from MySQLConnector
class DestinationMySQLConnector(SyncMySQLConnector):
    # Constructor
    def __init__(self, host, user, password, database, table, join_key, primary_key, port=3306):
        super().__init__(host, user, password, database, table, join_key, port)
        self.primary_key = primary_key

    # Update Table with relevant data in Dataframe
    def update_row(self, update_parameters, key_name, key_value):
        # Create Update statement
        update_stmt = update(self.tableObject).where(self.tableObject.c[key_name] == key_value).values(update_parameters)

        # Execute statement
        self.conn_engine.execute(update_stmt)
        self.conn_engine.commit()

    # Delete row
    def delete_row(self, key_name, key_value):
        # Create delete statement
        delete_stmt = delete(self.tableObject).where(self.tableObject.c[key_name] == key_value)

        # Execute statement
        self.conn_engine.execute(delete_stmt)
        self.conn_engine.commit()


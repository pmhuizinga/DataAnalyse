import pandas as pd
import pyodbc
import logging

# setup logging
logger = logging.getLogger(__name__)  # initialize logger
logger.handlers = []
c_handler = logging.StreamHandler()  # Create handlers
c_format = logging.Formatter('%(levelname)s - %(message)s')
c_handler.setFormatter(c_format)  # Create formatters and add it to handlers
logger.addHandler(c_handler)  # Add handlers to the logger
logger.setLevel(logging.DEBUG)


class DataAnalyses:
    def __init__(self):
        self.version = '0.0.1'
        self.author = 'Paul Huizinga'

    def connect_sql(self, server, database):
        """
        create connection string for SQL Server
        :param server: servername
        :param database: databasename
        :return: connetion string
        """
        driver = 'SQL Server'
        trusted_connection = 'yes'
        conn_string = 'Driver={};Server={};Database={};Trusted_Connection={};'

        conn = pyodbc.connect(conn_string.format(driver, server, database, trusted_connection))

        logger.debug(conn_string)

        return conn

    def connect_postgress(self, server, database):
        conn_string = 'DRIVER={PostgreSQL Unicode};DATABASE=postgres;UID=postgres;PWD=whatever;SERVER=localhost;PORT=5432;'

        logger.debug(conn_string)

        return conn_string

    def select_distinct(self, schemaname, tablename, fieldname):
        """
        create sql string for selecting distinct records
        :param schemaname: schema name
        :param tablename:  table name
        :param fieldname: field name
        :return: TSQL string
        """
        sql_base = 'SELECT DISTINCT {} FROM {}.{}'
        sql_string = sql_base.format(fieldname, schemaname, tablename)

        logger.debug(sql_string)

        return sql_string

    def sql_execute(self, conn, sql):
        df = pd.read_sql(sql, conn)
        return df

    # def check_referential_integrity(self, master, reference):
    #     master = (set(master.iloc[:, 0]))
    #     reference = (set(reference.iloc[:, 0]))
    #     return [[x for x in master if x not in reference]]

    def check_referential_integrity_new(self, master_con, master_schema, master_table, master_field, ref_con,
                                        ref_schema, ref_table, ref_field):
        master = self.sql_execute(master_con, self.select_distinct(master_schema, master_table, master_field))
        ref = self.sql_execute(ref_con, self.select_distinct(ref_schema, ref_table, ref_field))
        master = (set(master.iloc[:, 0]))
        reference = (set(ref.iloc[:, 0]))
        out = [x for x in master if x not in reference]

        return out

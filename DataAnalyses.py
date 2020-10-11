import pandas as pd
import pyodbc
import logging_settings

logger = logging_settings.setup_logger()

class DataAnalyses:
    def __init__(self):
        self.version = '0.0.1'
        self.author = 'Paul Huizinga'


    def database_connect(self, driver=None, server=None, database=None, uid=None, pwd=None, port=None):
        """
        create connection string for SQL Server
        :param driver: database driver, mandatory
        :param server: servername, mandatory
        :param database: databasename, mandatory
        :param uid: user id, optional
        :param pwd: user password, optional
        :param port: port, optional
        :return: connetion string
        """
        trusted_connection = 'yes'

        if driver == None or server == None or database == None:
            logger.debug("database connection details missing")
            return

        conn_string = 'Driver={};Server={};Database={};Trusted_Connection={}'.format(driver, server, database,
                                                                                     trusted_connection)

        if uid != None:
            UID = ';UID=' + str(uid)
            conn_string = conn_string + UID
        if pwd != None:
            PWD = ';PWD=' + str(pwd)
            conn_string = conn_string + PWD
        if port != None:
            PORT = ';PORT=' + str(port)
            conn_string = conn_string + PORT

        logger.debug(conn_string)

        conn = pyodbc.connect(conn_string)

        return conn


    def select_all(self, conn, tablename):
        """

        :param tablename:
        :return:
        """
        sql_base = 'SELECT * FROM [{}]'
        sql_string = sql_base.format(tablename)
        logger.debug(sql_string)
        df = self.sql_execute(conn, sql_string)

        return df


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


    def select_information_schema(self, conn, schemaname):
        #sql_base = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{}'"
        sql_base = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME LIKE 'T_MASTER_%' OR TABLE_NAME LIKE 'T_REF_%'"
        sql_string = sql_base.format(schemaname)

        logger.debug(sql_string)

        df = self.sql_execute(conn, sql_string)
        tables = df.TABLE_NAME.unique()

        return df, tables


    def sql_execute(self, conn, sql):
        df = pd.read_sql(sql, conn)
        return df


    def check_referential_integrity_new(self, master_con, master_schema, master_table, master_field, ref_con,
                                        ref_schema, ref_table, ref_field):
        master = self.sql_execute(master_con, self.select_distinct(master_schema, master_table, master_field))
        ref = self.sql_execute(ref_con, self.select_distinct(ref_schema, ref_table, ref_field))
        master = (set(master.iloc[:, 0]))
        reference = (set(ref.iloc[:, 0]))
        out = [x for x in master if x not in reference]

        return out


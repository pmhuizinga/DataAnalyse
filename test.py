import DataAnalyses
from statistics import BasicStats
import config
import pandas as pd

# read config
driver = config.driver
server = config.server
test_sql = config.ref_test_sql.split(',')
db = config.db

# connect
da = DataAnalyses.DataAnalyses()
db_conn = da.database_connect(driver, server, db)

# test referential integrity code
# ref_int_new = da.check_referential_integrity_new(db_conn, test_sql[0], test_sql[1], test_sql[2], db_conn, test_sql[3],
#                                                  test_sql[4], test_sql[5])
# print('ids found in {} which are not found in {}:'.format(test_sql[4], test_sql[1]), ref_int_new)

# read sql server information schema
df_information_schema, tables = da.select_information_schema(db_conn, 'dbo')

tables = ['t_ref_portfolio_code']
# get table statistics
df_stat = pd.DataFrame()
for table in tables:
    df = da.select_all(db_conn, table)
    df2, lststat = (BasicStats(table, df))
    df_stat = pd.concat([df_stat, df2])

# df = da.select_all(db_conn, 't_ref_portfolio_code')
# dfstat, lststat = BasicStats('t_ref_portfolio_code', df)

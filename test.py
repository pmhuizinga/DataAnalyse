import DataAnalyses as DA
import config

server = config.server
db = config.db
da = DA.DataAnalyses()
sql_paul_home = da.connect_sql(server, db)
ref_int_new = da.check_referential_integrity_new(sql_paul_home, 'dbo', 'TestB', 'TestAID', sql_paul_home, 'dbo', 'TestA', 'ID')

print('ids found in TestB which are not found in TestA:', ref_int_new)
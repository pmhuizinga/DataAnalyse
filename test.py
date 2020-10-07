import DataAnalyses as DA
import config

server = config.server
db = config.db
da = DA.DataAnalyses()
db_conn = da.connect_sql(server, db)
ref_int_new = da.check_referential_integrity_new(master_con=db_conn, 'dbo', 'T_MASTER_SEC', 'CADIS_ID', db_conn, 'dbo', 'T_MASTER_POS', 'CADIS_ID')

print('ids found in TestB which are not found in TestA:', ref_int_new)
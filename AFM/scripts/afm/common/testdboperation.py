from common import DBOperation

# Vertica
sql = 'create table if not exists wb_test (id int)'
dw_ver = DBOperation.DWAccess()
# dw_ver.execute(sql)

sql = 'select * from dim1'
dw_ver.query_with_result(sql)
x = dw_ver.query_cursor(sql)

# x = dw_ver.query_scalar(sql)
# print(x)


# SqlServer
sql = 'select * from afm_rules'
app_conn = DBOperation.APPAccess()
x = app_conn.query_with_result(sql)
print(x)

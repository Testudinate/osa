
from common.DBOperation import *

dw_connection = DWAccess()

all_columns_list = []
sql = "select column_name from columns where table_name='anl_fact_osm_incidents' and table_schema='PEPSI_AHOLD_LH'"
# columnArray = dw_connection.query_with_result(sql)
# print(columnArray)

# [all_columns_list.append("\"" + column['column_name'] + "\"") for column in columnArray]
# for column in columnArray:
#     print(column['column_name'],type(column))
#     # all_columns = all_columns.append(column['column_name'])   # append时不需要赋值语句
#     all_columns.append(column['column_name'])

# print(all_columns_list)

# all_columns = ",".join(all_columns_list)

# print(all_columns)
# | % {'a."' + $_.column_name + '"'}
# insertAlertColumns = columnArray - join(",")
# insertAlertColumns = insertAlertColumns - replace 'a."UPC"', 'c.UPC'
# insertAlertColumns = insertAlertColumns - replace 'a."STOREID"', 'd.STORE_ID as STOREID'
# insertAlertColumns = insertAlertColumns - replace 'a."MAJOR_CATEGORY_NO"', 'c.OSM_MAJOR_CATEGORY_NO as MAJOR_CATEGORY_NO'
# insertAlertColumns = insertAlertColumns - replace 'a."MAJOR_CATEGORY"', 'c.OSM_MAJOR_CATEGORY as MAJOR_CATEGORY'

d1 = {'Ben':10, 'xx':100}
print(d1.keys())    # dict_keys(['Ben', 'xx'])
print(d1.items())

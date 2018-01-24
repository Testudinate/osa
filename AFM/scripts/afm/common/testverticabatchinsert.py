from common import DBOperation

# Vertica does NOT support multi rows inserting in one SQL
# Vertica
dw_conn = DBOperation.DWAccess()._dw_conn
app_conn = DBOperation.APPAccess()._app_conn

# test query_cursor
# dw_cur = dw_conn.cursor()
# sql = "select * from OSA_AHOLD_BEN.RSI_DIM_VENDOR_copy limit 0"
# # dw_cur.execute(sql)
# # column_set = dw_cur.fetchall()
# dw_conn1 = DBOperation.DWAccess()
# column_set = dw_conn1.query_cursor(sql)

cur = app_conn.cursor()
sql = 'select VENDOR_KEY, VENDOR_NAME, VENDOR_SNAME from rsi_dim_vendor'
f = lambda x: "'" + str(x) + "'"

# SqlServer to Vertica
# 1, load data row by row. very inefficient.
cur.execute(sql)
rows = cur.fetchall()  # fetchmany()
print(type(rows))
print(rows)
for row in rows:
    pass
    # print(type(row))    # <class 'pyodbc.Row'>
    # print(row)    # (574, "Welch's", 'WELCHS')
columns = [column[0].upper() for column in cur.description]
print(columns)  # ['VENDOR_KEY', 'VENDOR_NAME', 'VENDOR_SNAME']
results = []

# dw_conn.execute("insert into OSA_AHOLD_BEN.RSI_DIM_VENDOR_copy values(%d, %s, %s)" % (-999, 'Antacids', 'ANTLAX'))
data_list = []
for row in rows:
    # results.append(OrderedDict(zip(columns, row)))
    results.append(dict(zip(columns, row)))
    # print(row, repr(row[1]))        # (-999, 'Antacids & Laxatives', 'ANTLAX') 'Antacids & Laxatives'
    # print(list(row), repr(list(row)[1]))        # [-999, 'Antacids & Laxatives', 'ANTLAX'] 'Antacids & Laxatives'
    # print(row[0], row[1], row[2])
    # data = (f(row[0]), f(row[1]), f(row[2]))
    # data = (row[0], row[1], row[2])
    # data = (repr(row[0]), repr(row[1]), repr(row[2]))
    # print(data)
    # dw_conn.execute("insert into OSA_AHOLD_BEN.RSI_DIM_VENDOR_copy values(%s, %s, %s)" % data)
    # data_list.append(data)

print(results)
# print('insert into OSA_AHOLD_BEN.RSI_DIM_VENDOR_copy values({0}, {1}, {2})'.format(*data_list)) # only first 3 elements in list
# print('insert into OSA_AHOLD_BEN.RSI_DIM_VENDOR_copy values{0}'.format(tuple(data_list)))
# print('insert into OSA_AHOLD_BEN.RSI_DIM_VENDOR_copy values({0[0]})'.format(data_list))
# dw_conn.execute("insert into OSA_AHOLD_BEN.RSI_DIM_VENDOR_copy values({0}, {1}, {2})".format(*data_list))

# print(rows)
# for row in rows:
#     print(row)
#     print(f(row['VENDOR_KEY']), f(row['VENDOR_NAME']), f(row['VENDOR_SNAME']))
#     dw_conn.execute("insert into OSA_AHOLD_BEN.RSI_DIM_VENDOR_copy values({0}, {1}, {2})"
#                    .format( f(row['VENDOR_KEY']), f(row['VENDOR_NAME']), f(row['VENDOR_SNAME'])))

# 2, Pandas
# https://stackoverflow.com/questions/36028759/how-to-open-and-convert-sqlite-database-to-pandas-dataframe
# https://github.com/jamescasbon/vertica-sqlalchemy
# to_sql function only supports sqlalchemy.
import pandas as pd
import sqlalchemy as sa
import urllib

# 1, read data from DB
data = pd.DataFrame(rows)       # rows are tuple type
# print(data)             # 0           [-999, Antacids & Laxatives, ANTLAX]
data = pd.DataFrame.from_records(rows)  # there is no column name
# print(data)             # 0     -999             Antacids & Laxatives  ANTLAX
data = pd.DataFrame.from_records(rows, columns=columns) # specify the column name
# print(data)             # 0     -999             Antacids & Laxatives  ANTLAX

# Read SQL query or database table into a DataFrame.
df = pd.read_sql(sql, app_conn)
# print(type(df), df)               # 0          -999             Antacids & Laxatives       ANTLAX
# Read SQL query into a DataFrame
# pd.read_sql_query()
# Read SQL database table into a DataFrame
# pd.read_sql_table()


# 2, write data into DB
# https://stackoverflow.com/questions/23832487/connecting-to-vertica-database-using-sqlalchemy
# i think writing dbadmin: in engine = create_engine('vertica+pyodbc://dbadmin:@verticadsn') overwrites the UID and PWD in the .odbc.ini file.
# engine = create_engine('vertica+pyodbc://@verticadsn') works just fine
dw_conn_sa = sa.create_engine('vertica+pyodbc://@mydsn')
# dw_conn_sa = sa.create_engine(sa.engine.url.URL(
#     drivername='vertica+pyodbc',
#     username='ben.wu',
#     password='****',
#     host='QAVERTICANXG.ENG.RSICORP.LOCAL',
#     database='fusion',
# ))

# below part is working fine. it is slow in local PC, but it is much faster in VM.
sql = 'select VENDOR_KEY, VENDOR_NAME, VENDOR_SNAME from OSA_AHOLD_BEN.rsi_dim_vendor'
# dw_conn_sa = sa.create_engine("vertica+vertica_python://ben.wu:****@QAVERTICANXG.ENG.RSICORP.LOCAL:5432/fusion")
df = pd.read_sql(sql, dw_conn_sa)
# print(type(df), df)
# df.to_sql('RSI_DIM_VENDOR_copy', dw_conn_sa, schema='OSA_AHOLD_BEN', if_exists='append', index=False, chunksize=1000)
# df.to_sql('RSI_DIM_VENDOR_copy', dw_conn_sa, schema='OSA_AHOLD_BEN', if_exists='append', index=False)


# 3, unload data into csv/other format file, then run copy command to load data into Vertica
# http://blog.csdn.net/pfm685757/article/details/47806469
import csv

# myFile = open('vendor.csv', 'w')
myFile = open('vendor.csv', 'w', encoding='utf8', newline='')
with myFile:
    spamwriter = csv.writer(myFile, delimiter='\034', quotechar='"')
    # spamwriter.writerow(['Spam'] * 5 + ['Baked Beans'])   # testing writerow
    # spamwriter.writerow(['Spam', 'Lovely Spam', 'Wonderful Spam'])    # testing writerow
    # csv.DictWriter()
    spamwriter.writerows(rows)

with open('vendor.csv', 'r', newline='') as csvFile:
    # csvFile.readline()
    # csvFile.read()
    # csvFile.readlines()
    spamreader = csv.reader(csvFile, delimiter='\034', quotechar='"')
    for row in spamreader:
        print(row)

# quotechar='|'  #  " is the default character
# quoting=csv.QUOTE_MINIMAL # only quote fields which contains the special characters.
# quoting=csv.QUOTE_ALL # quote all fields.
with open('eggs.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    spamwriter.writerow(['Spam'] * 5 + ['Baked Beans'] + ['nospace'])
    spamwriter.writerow(['Spam', 'Lovely Spam', 'Wonderful Spam', 'withoutspace'])

# COPY OSA_AHOLD_BEN.rsi_dim_vendor_copy (firstcolumn, secondcolumn) FROM STDIN DELIMITER ',' ENCLOSED BY '"';



# 4, BatchInsert/Copy without loading into data file.
# https://stackoverflow.com/questions/32658926/bulk-insert-into-vertica-using-python
# https://pypi.python.org/pypi/vertica-python/0.7.3
import vertica_python

conn_info = {'host': 'QAVERTICANXG.ENG.RSICORP.LOCAL',
'port': 5433,
'user': 'ben.wu',
'password': '****',
'database': 'fusion'}
connection = vertica_python.connect(**conn_info)
cur = connection.cursor()

with open('vendor.csv', 'r', newline='') as csvFile:
    print(type(csvFile), csvFile)
    # csvFile.readline()
    # csvFile.read()
    # csvFile.readlines()
    # spamreader = csv.reader(csvFile, delimiter='\034', quotechar='"')
    # cur.copy("COPY OSA_AHOLD_BEN.rsi_dim_vendor_copy (VENDOR_KEY, VENDOR_NAME, VENDOR_SNAME) from stdin DELIMITER '\034'", csvFile)

# has to have a \n
data1 = "99999, Diet, DIETD\n" \
        "999, Pet Delhaize, DHZPET\n" \
        "9997, Beverage SS, BEVSS"
# doesn't work. meaning there has to be a read() method. like read a file/csv file etc.  data1 seems like a data file. data2 seems not(remove () ).
data2 = "(991, Diet, DIETD)\n" \
        "(992, Pet Delhaize, DHZPET)\n" \
        "(993, Beverage SS, BEVSS)"
# cur.copy("COPY OSA_AHOLD_BEN.rsi_dim_vendor_copy (VENDOR_KEY, VENDOR_NAME, VENDOR_SNAME) from stdin DELIMITER ','", data1)  # works
cur.copy("COPY OSA_AHOLD_BEN.rsi_dim_vendor_copy (VENDOR_KEY, VENDOR_NAME, VENDOR_SNAME) from stdin DELIMITER ','", data2)  # doesn't work


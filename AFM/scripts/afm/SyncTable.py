import pandas as pd
import sqlalchemy as sa
from common import DBOperation


class SyncTable(object):
    # def __init__(self, source_table, target_table, context):
    def __init__(self, source_conn, target_conn, context):
        self._source_conn = source_conn
        self._target_conn = target_conn
        # self._target_conn = DBOperation.DWAccess()
        # self._source_conn = DBOperation.APPAccess()
        # self._source_table = source_table
        # self._target_table = target_table
        self._context = context
        self._schema_name = self._context["SCHEMA_NAME"]
        self._dw_alchemy_conn = self._get_sql_alchemy_conn()

    def sync_table(self, source_table, target_table):
        self._sync_data_with_pandas(source_table, target_table)

    def _get_sql_alchemy_conn(self):
        return sa.create_engine('vertica+pyodbc://@mydsn')

    def _get_dw_table_columns(self, dw_table):
        sql = "select * from {schema_name}.{dw_table} limit 0"\
            .format(schema_name=self._schema_name, dw_table=dw_table)
        columns_cursor = self._target_conn.query_cursor(sql)
        columns = ', '.join(column[0].upper() for column in columns_cursor.description)
        return columns

    def _sync_data_with_pandas(self, source_table, target_table):
        _insert_columns = self._get_dw_table_columns(target_table)
        # print(_insert_columns)

        source_sql = "SELECT {insert_columns} FROM {table_name}"\
            .format(insert_columns=_insert_columns, table_name=source_table)
        print(source_sql)

        df = pd.read_sql(source_sql, self._source_conn.get_connection())
        # print(df)
        df.to_sql(target_table, self._dw_alchemy_conn, schema=self._schema_name, if_exists='replace', index=False)


if __name__ == '__main__':
    sync1 = SyncTable('RSI_DIM_VENDOR', 'RSI_DIM_VENDOR_copy', {'SCHEMA_NAME': 'OSA_AHOLD_BEN'})
    sync1.sync_table()

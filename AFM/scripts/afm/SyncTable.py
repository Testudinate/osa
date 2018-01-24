import pandas as pd
import sqlalchemy as sa
import csv
from common import DBOperation


class SyncTable(object):
    # def __init__(self, source_conn, target_conn, source_table, target_table, context):
    def __init__(self):
        # self._source_conn = source_conn
        # self._target_conn = target_conn
        self._target_conn = DBOperation.DWAccess()
        self._source_conn = DBOperation.APPAccess()
        self._source_table = "RSI_DIM_VENDOR"
        self._target_table = "RSI_DIM_VENDOR_COPY"
        # self._context = context
        # self._schema_name = self._context["SCHEMA_NAME"]
        self._schema_name = 'OSA_AHOLD_BEN'
        self._dw_alchemy_conn = self._get_sql_alchemy_conn()

    def sync_table(self):
        self._sync_data_with_pandas()

    def _get_sql_alchemy_conn(self):
        return sa.create_engine('vertica+pyodbc://@mydsn')

    def _get_dw_table_columns(self):
        sql = "select * from {schema_name}.{dw_table} limit 0"\
            .format(schema_name=self._schema_name, dw_table=self._target_table)
        columns_cursor = self._target_conn.query_cursor(sql)
        columns = ', '.join(column[0].upper() for column in columns_cursor.description)
        return columns

    def _sync_data_with_pandas(self):
        _insert_columns = self._get_dw_table_columns()
        print(_insert_columns)

        source_sql = "SELECT {insert_columns} FROM {table_name}"\
            .format(insert_columns=_insert_columns, table_name=self._source_table)
        print(source_sql)

        df = pd.read_sql(source_sql, self._source_conn._app_conn)
        df.to_sql(self._target_table, self._dw_alchemy_conn, schema=self._schema_name, if_exists='append', index=False)


if __name__ == '__main__':
    sync1 = SyncTable()
    sync1.sync_table()

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
        self._vendor_key = self._context["VENDOR_KEY"]
        self._retailer_key = self._context["RETAILER_KEY"]
        self._schema_name = self._context["SCHEMA_NAME"]
        self._dw_alchemy_conn = sa.create_engine('vertica+pyodbc://@mydsn')

    def sync_table_with_sql(self, source_sql, target_table):
        df = pd.read_sql(source_sql, self._source_conn.get_connection())
        # print(df)
        df.to_sql(target_table, self._dw_alchemy_conn, schema=self._schema_name, if_exists='append', index=False)

    def sync_table(self, source_table, target_table, filters, check_vr=0):
        """
        sync data from source table to target table
        first: getting columns from source table. Then generate the sql based on columns and filters if have.
        last: insert data into target table based on SQL using pandas.to_sql method.
        Assuming source table and target table are having the same structure.

        :param source_table:
        :param target_table:
        :param filters:     if any filters required.
        :param check_vr:    [True/False] if vendor_key and retailer_key required in target table.
        :return:
        """
        self._sync_data_with_pandas(source_table, target_table, filters, check_vr)

    def _get_source_table_columns(self, source_table):
        sql = "select top 0 * from {source_table} "\
            .format(schema_name=self._schema_name, source_table=source_table)
        print(sql)
        columns_cursor = self._source_conn.query_cursor(sql)
        columns = ', '.join(column[0].upper() for column in columns_cursor.description)
        print(columns)
        return columns

    def _sync_data_with_pandas(self, source_table, target_table, filters, check_vr):
        _insert_columns = self._get_source_table_columns(source_table)
        _vendor_retailer = ''
        if check_vr:
            if "VENDOR_KEY" not in _insert_columns:
                _vendor_retailer = '{0} as VENDOR_KEY, '.format(self._vendor_key)
            if "RETAILER_KEY" not in _insert_columns:
                _vendor_retailer += '{0} as RETAILER_KEY, '.format(self._retailer_key)

        _insert_columns = _vendor_retailer + _insert_columns
        print(_insert_columns)
        source_sql = "SELECT {insert_columns} " \
                     "FROM {table_name} {FILTER} "\
            .format(insert_columns=_insert_columns,
                    table_name=source_table,
                    FILTER=filters)
        print(source_sql)

        df = pd.read_sql(source_sql, self._source_conn.get_connection())
        # print(df)
        df.to_sql(target_table, self._dw_alchemy_conn, schema=self._schema_name, if_exists='append', index=False)


if __name__ == '__main__':
    sync1 = SyncTable('RSI_DIM_VENDOR', 'RSI_DIM_VENDOR_copy', {'SCHEMA_NAME': 'OSA_AHOLD_BEN'})
    sync1.sync_table()

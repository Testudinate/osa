
import pyodbc as py


class DWAccess(object):

    def __init__(self):
        self._host = 'QAVERTICANXG.ENG.RSICORP.LOCAL'
        self._port = 5433
        self._user = 'ben.wu'
        self._pwd = '***'
        self._db = 'fusion'
        self._dw_conn = self.get_connection()

    def get_connection(self):
        return py.connect("DRIVER={{Vertica}};SERVER={0};DATABASE={1};UID={2};PWD={3}"
                          .format(self._host, self._db, self._user, self._pwd))

    def query_with_result(self, sql):
        # conn = self.get_connection()
        try:
            cur = self._dw_conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()   #fetchmany()
            # for row in rows:
            #     print(row)
            columns = [column[0].upper() for column in cur.description]
            results = []
            for row in rows:
                # results.append(OrderedDict(zip(columns, row)))
                # yield dict(zip(columns, row))
                results.append(dict(zip(columns, row)))
            return results
        except py.ProgrammingError as e:
            print('failed to execute sql: {}.\n error:{}'.format(sql, e))
            raise
        finally:
            del cur

    def query_cursor(self, sql):
        # conn = self.get_connection()
        try:
            cur = self._dw_conn.cursor()
            cur.execute(sql)
            return cur
        except py.ProgrammingError as e:
            print('failed to execute sql: {}.\n error:{}'.format(sql, e))
            raise
        finally:
            del cur

    def query_scalar(self, sql):
        # conn = self.get_connection()
        try:
            cur = self._dw_conn.cursor()
            cur.execute(sql)
            value = cur.fetchone()
            return value
        except py.ProgrammingError as e:
            print('failed to execute sql: {}.\n error:{}'.format(sql, e))
            raise
        finally:
            del cur

    def execute(self, sql):
        # conn = self.get_connection()
        try:
            cur = self._dw_conn.cursor()
            cur.execute(sql)
            self._dw_conn.commit()
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql,e))
            raise
        finally:
            del cur

    def close_conn(self):
        self._dw_conn.close()

class APPAccess(object):
    def __init__(self):
        self._host = r'10.172.36.31\RV2'
        self._port = 1433
        self._user_name = 'ben.wu'
        self._pwd = '!QAZ2wsx#EDDC'
        self._db_name = 'OSA_AHOLD_BEN'
        self._app_conn = self.get_connection()

    def get_connection(self):
        return py.connect("DRIVER={{SQL Server}}; SERVER={0}; DATABASE={1}; UID={2}; PWD={3}"
                          .format(self._host, self._db_name,
                                  self._user_name, self._pwd))

    def query_with_result(self, sql):
        # conn = self.get_connection()
        try:
            cur = self._app_conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()   #fetchmany()
            # for row in rows:
            #     print(row)
            columns = [column[0].upper() for column in cur.description]
            results = []
            for row in rows:
                # results.append(OrderedDict(zip(columns, row)))
                results.append(dict(zip(columns, row)))
            return results
        except py.ProgrammingError as e:
            print('failed to execute sql: {}.\n error:{}'.format(sql, e))
            raise
        finally:
            del cur

    def query_cursor(self, sql):
        # conn = self.get_connection()
        try:
            cur = self._app_conn.cursor()
            cur.execute(sql)
            return cur
        except py.ProgrammingError as e:
            print('failed to execute sql: {}.\n error:{}'.format(sql, e))
            raise
        finally:
            del cur

    def query_scalar(self, sql):
        # conn = self.get_connection()
        try:
            cur = self._app_conn.cursor()
            cur.execute(sql)
            value = cur.fetchone()
            return value
        except py.ProgrammingError as e:
            print('failed to execute sql: {}.\n error:{}'.format(sql, e))
            raise
        finally:
            del cur

    def execute(self, sql):
        # conn = self.get_connection()
        try:
            cur = self._app_conn.cursor()
            cur.execute(sql)
            self._app_conn.commit()
        except py.ProgrammingError as e:
            print("failed to execute {}, error: {}".format(sql,e))
            raise
        finally:
            del cur

    def close_conn(self):
        self._app_conn.close()

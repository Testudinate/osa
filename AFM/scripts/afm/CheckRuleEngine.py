from SyncTable import SyncTable


class CheckRuleEngine(object):

    def __init__(self, dw_conn, app_conn, context):
        self._dw_connection = dw_conn
        self._app_connection = app_conn
        self._context = context
        self._vendor_key = context["VENDOR_KEY"]
        self._retailer_key = context["RETAILER_KEY"]
        self._schema_name = context["SCHEMA_NAME"]
        self._suffix = "_" + context["SUFFIX"]
        self._sync_data = SyncTable(source_conn=self._app_connection,
                                    target_conn=self._dw_connection,
                                    context=self._context)

    def check_rule_engine(self):
        """
        Preparing rule engine tables ANL_RULE_ENGINE_STAGE_RULES{suffix} for control processing.
        :return:

        There is no need to check rule engine any longer from PowerShell script.
        Added 3 more columns(Param1, Param2, Param3) in table ANL_RULE_ENGINE_META_RULES to setup the dependency
        UI can check this table when users are saving configuration.
        """
        self.__process()

        self.sync_rule_engine_tables()

    # TODO : need to use temp table to store synced data from SqlServer instead of staging table.
    # TODO : Should move below sync part after checking if AFM needs to be run/rerun.
    def sync_rule_engine_tables(self):
        # _source_table = _target_table = 'ANL_RULE_ENGINE_SUB_LEVEL_FILTER'
        # self._sync_data.sync_table(_source_table, _target_table, ' where 1=1 ', 1)
        # _source_table , _target_table = 'ANL_RULE_ENGINE_UPC_STORE_LIST', 'ANL_RULE_ENGINE_UPC_STORE_LIST'
        # self._sync_data.sync_table(_source_table, _target_table, ' where 1=1 ', 1)

        # getting associated rule_set_id of the given vendor & retailer to sync below 2 tables.
        sql = "SELECT rule_set_id FROM " \
              "(SELECT *,ROW_NUMBER() " \
              "          OVER(PARTITION BY rule_set_name order by rule_set_id desc) AS idx " \
              " FROM ANL_RULE_ENGINE_RULE_SET where vendor_key = {0} and retailer_key = {1}) tmp " \
              "WHERE idx=1 AND enabled in ('T','Y')".format(self._vendor_key, self._retailer_key)
        print(sql)
        rule_sets = self._app_connection.query_with_result(sql)
        rule_set_str = ','.join(str(rule_set["RULE_SET_ID"]) for rule_set in rule_sets)
        print(rule_set_str)

        # sync table ANL_RULE_ENGINE_SUB_LEVEL_FILTER from SqlServer to Vertica
        sql = "DROP TABLE IF EXISTS {schemaName}.ANL_RULE_ENGINE_SUB_LEVEL_FILTER{suffix}; " \
              "CREATE TABLE IF NOT EXISTS {schemaName}.ANL_RULE_ENGINE_SUB_LEVEL_FILTER{suffix} as " \
              "SELECT * FROM {schemaName}.ANL_RULE_ENGINE_SUB_LEVEL_FILTER_TEMPLATE WHERE 1=0 "\
            .format(schemaName=self._schema_name,
                    suffix=self._suffix)
        print(sql)
        self._dw_connection.execute(sql)

        source_sql = "SELECT DISTINCT RULE_ID,RULE_SET_ID,METRICS_VALUE,PARAMETER2,PARAMETER3, " \
                     "       SUB_LEVEL_VALUE,SUB_LEVEL_CATEGORY " \
                     "FROM ANL_RULE_ENGINE_SUB_LEVEL_FILTER " \
                     "WHERE RULE_SET_ID in ({ruleSetIDs})"\
            .format(ruleSetIDs=rule_set_str,
                    vendor_key=self._vendor_key,
                    retailer_key=self._retailer_key)
        print(source_sql)
        self._sync_data.sync_table_with_sql(source_sql, 'ANL_RULE_ENGINE_SUB_LEVEL_FILTER{0}'.format(self._suffix))

        # sync table ANL_RULE_ENGINE_UPC_STORE_LIST from SqlServer to Vertica
        sql = "DROP TABLE IF EXISTS {schemaName}.ANL_RULE_ENGINE_UPC_STORE_LIST{suffix}; " \
              "CREATE TABLE IF NOT EXISTS {schemaName}.ANL_RULE_ENGINE_UPC_STORE_LIST{suffix} AS " \
              "SELECT * FROM {schemaName}.ANL_RULE_ENGINE_UPC_STORE_LIST_TEMPLATE WHERE 1=0 "\
            .format(schemaName=self._schema_name,
                    suffix=self._suffix)
        print(sql)
        self._dw_connection.execute(sql)

        source_sql = """SELECT FILE_ID, UPC, STOREID
        FROM ANL_RULE_ENGINE_UPC_STORE_LIST lst
        INNER JOIN ANL_RULE_ENGINE_RULE_SET rs
        ON (lst.FILE_ID = rs.ITEM_SCOPE OR lst.FILE_ID = rs.STORE_SCOPE )
        AND rs.RULE_SET_ID IN ({ruleSetIDs})
        UNION
        SELECT DISTINCT lst.FILE_ID, lst.UPC, lst.STOREID
        FROM ANL_RULE_ENGINE_UPC_STORE_LIST lst
        INNER JOIN ANL_RULE_ENGINE_RULES r
        ON lst.FILE_ID = r.PARAMETER1 AND (r.RULE_ID = 27 OR r.RULE_ID = 28 OR r.RULE_ID = 29)
        AND r.RULE_SET_ID IN ({ruleSetIDs})"""\
            .format(ruleSetIDs=rule_set_str)
        print(source_sql)
        self._sync_data.sync_table_with_sql(source_sql, 'ANL_RULE_ENGINE_UPC_STORE_LIST{0}'.format(self._suffix))

        # rows = self._dw_connection.query_with_result("SELECT * FROM {0}.ANL_RULE_ENGINE_UPC_STORE_LIST{1}"
        #                                              .format(self._schema_name, self._suffix))
        # print(rows)

    def __process(self):
        print("\n-------------- Calling CheckRuleEngine class --------------")

        # dw version
        # sql="DROP TABLE IF EXISTS #TMP_ANL_RULE_ENGINE_STAGE_RULES;" \
        #     "CREATE TABLE #TMP_ANL_RULE_ENGINE_STAGE_RULES as " \
        #     "SELECT /*+label(GX_OSM_RULE_ENGINE)*/ a.*,b.PROVIDER_SUB_TYPE," \
        #     "       b.METRICS_ORDER AS PROCESSING_ORDER, b.DEPEND_ON_PREVIOUS_RULE "\
        #     "FROM {schemaName}.ANL_RULE_ENGINE_RULES a  "\
        #     "INNER JOIN {schemaName}.ANL_RULE_ENGINE_META_RULES b " \
        #     "ON a.RULE_ID=b.RULE_ID AND a.enabled in ('Y','T') "\
        #     "INNER JOIN {schemaName}.ANL_RULE_ENGINE_RULE_SET c " \
        #     "ON a.RULE_SET_ID=c.RULE_SET_ID AND c.ENABLED in ('Y','T') " \
        #     "WHERE c.vendor_key = {VENDOR_KEY} AND c.retailer_key = {RETAILER_KEY};"\
        #     .format(schemaName=self._schema_name,
        #             suffix=self._suffix,
        #             VENDOR_KEY=self._vendor_key,
        #             RETAILER_KEY=self._retailer_key)
        # self._dw_connection.execute(sql)
        # print(sql)

        # SqlServer
        sql = "IF (OBJECT_ID('tempdb..#TMP_ANL_RULE_ENGINE_STAGE_RULES') IS NOT NULL)" \
              "    DROP TABLE #TMP_ANL_RULE_ENGINE_STAGE_RULES " \
              "SELECT a.*,b.PROVIDER_SUB_TYPE, " \
              "       b.METRICS_ORDER AS PROCESSING_ORDER, b.DEPEND_ON_PREVIOUS_RULE " \
              "INTO #TMP_ANL_RULE_ENGINE_STAGE_RULES " \
              "FROM ANL_RULE_ENGINE_RULES a  " \
              "INNER JOIN ANL_RULE_ENGINE_META_RULES b " \
              "ON a.RULE_ID=b.RULE_ID AND a.enabled in ('Y','T') "\
              "INNER JOIN ANL_RULE_ENGINE_RULE_SET c " \
              "ON a.RULE_SET_ID=c.RULE_SET_ID AND c.ENABLED in ('Y','T') " \
              "WHERE c.vendor_key = {VENDOR_KEY} AND c.retailer_key = {RETAILER_KEY};"\
            .format(VENDOR_KEY=self._vendor_key,
                    RETAILER_KEY=self._retailer_key)
        print(sql)
        self._app_connection.execute(sql)

        print("-------------- Calling CheckRuleEngine class end --------------\n")

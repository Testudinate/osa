
# from DBOperation import *


class GenRuleEngineStageData(object):

    def __init__(self, dw_conn, app_conn, context):
        # self.dw_connection = DWAccess()
        self._dw_connection = dw_conn
        self._app_connection = app_conn
        self._context = context
        self._schema_name = self._context["SCHEMA_NAME"]
        self._vendor_key = self._context["VENDOR_KEY"]
        self._retailer_key = self._context["RETAILER_KEY"]
        self._retailer_name = self._context["RETAILER_NAME"]
        self._seq_num = self._context["SEQ_NUM"]
        self._max_initial_day = self._context["PERIOD_KEY"]

    def gen_stage_table(self):
        """
        Preparing table ANL_RULE_ENGINE_STAGE_FACT from Source table ANL_FACT_OSM_INCIDENTS table.
        Exiting if there is no data to be processed
        :return: no returned value
        """
        self.__process()

    def __process(self):
        print("\n-------------- Calling GenRuleEngineStageData class --------------")

        # no need to get period_key from anl_fact_osm_incidents table directly.
        # instead of using table ANL_META_RAW_ALERTS_SEQ which populated by upstreaming Service <Alert Generation>
        # sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ CASE WHEN MAX(Period_Key) IS NULL THEN 19000101 " \
        #       "       ELSE MAX(Period_Key) END AS max_initial_day " \
        #       "FROM {schemaName}.anl_fact_osm_incidents WHERE vendor_key = {VENDOR_KEY}"\
        #     .format(schemaName=self._schema_name,
        #             VENDOR_KEY=self._vendor_key)
        # _max_initial_day = self._dw_connection.query_scalar(sql)[0]
        # print(_max_initial_day)

        sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ 'WHEN a.interventionkey='||interventionkey::VARCHAR(100)||' " \
              "       THEN '|| rankCalculation||' ' AS value " \
              "FROM {SCHEMA_NAME}.ANL_DIM_OSM_INTERVENTIONCLASSIFICATION " \
              "WHERE rankCalculation IS NOT NULL".format(SCHEMA_NAME=self._schema_name)
        _intervention_key_case_when_sql = ' CASE '
        _intervention_key_result = self._dw_connection.query_with_result(sql)
        for row in _intervention_key_result:
            _intervention_key_case_when_sql += row['VALUE']

        _intervention_key_case_when_sql += " END AS rank_value_after_calculation"

        # NXG-18204/NCM-2372 : updated few column source instead of using a.*
        # all columns of table anl_fact_osm_incidents
        all_columns_list = []
        sql = "SELECT column_name FROM columns WHERE table_name='anl_fact_osm_incidents' " \
              "AND table_schema='{SCHEMA_NAME}'".format(SCHEMA_NAME=self._schema_name)
        column_array = self._dw_connection.query_with_result(sql)
        [all_columns_list.append("a.\"" + _column['COLUMN_NAME'] + "\"") for _column in column_array]
        insert_alert_columns = ",".join(all_columns_list)
        insert_alert_columns = insert_alert_columns.replace('a."UPC"', 'c.UPC')
        insert_alert_columns = insert_alert_columns.replace('a."STOREID"', 'd.STORE_ID as STOREID')
        insert_alert_columns = insert_alert_columns.replace('a."MAJOR_CATEGORY_NO"',
                                                            'c.OSM_MAJOR_CATEGORY_NO as MAJOR_CATEGORY_NO')
        insert_alert_columns = insert_alert_columns.replace('a."MAJOR_CATEGORY"',
                                                            'c.OSM_MAJOR_CATEGORY as MAJOR_CATEGORY')
        print(insert_alert_columns)

        # <2016-11-16 by Hong>
        # Remove IF-ELSE block as OSM_ITEM_NBR always exists in OLAP_ITEM_OSM
        sql_pre_table = "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_PRE ON COMMIT PRESERVE ROWS AS " \
                        "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ {insertAlertColumns}, AlertType, b.AlertSubType, " \
                        "       b.Priority, c.osm_item_nbr AS item_nbr, {interventionKeyCaseWhenSQL} " \
                        "FROM {schemaName}.anl_fact_osm_incidents a " \
                        "INNER JOIN {schemaName}.ANL_DIM_OSM_INTERVENTIONCLASSIFICATION b " \
                        "ON a.InterventionKey = b.InterventionKey " \
                        "INNER JOIN {schemaName}.olap_item_osm c " \
                        "ON a.vendor_key = c.vendor_key AND a.item_key = c.item_key " \
                        "INNER JOIN {schemaName}.olap_store d " \
                        "ON a.retailer_key = d.retailer_key AND a.store_key = d.store_key " \
                        "WHERE Period_Key = {maxinitialday} AND a.InterventionKey <> 1" \
                        "AND a.vendor_key = {vendorKey} and seq_num = {seq_num} ;"\
            .format(insertAlertColumns=insert_alert_columns,
                    interventionKeyCaseWhenSQL=_intervention_key_case_when_sql,
                    schemaName=self._schema_name,
                    maxinitialday=self._max_initial_day,
                    vendorKey=self._vendor_key,
                    seq_num=self._seq_num)

        self._dw_connection.execute("DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_PRE")
        self._dw_connection.execute(sql_pre_table)
        # tmp_result = self.dw_connection.query_with_result("select * from ANL_RULE_ENGINE_STAGE_FACT_PRE")
        sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ COUNT(*) FROM ANL_RULE_ENGINE_STAGE_FACT_PRE"
        __row_cnt = self._dw_connection.query_scalar(sql)[0]
        if __row_cnt == 0:
            print("There is no data to be processed for period_key:{0} & seq_num{1}."
                  "Please check below SQL:\n{2}".format(self._max_initial_day,
                                                        self._seq_num,
                                                        sql_pre_table))
            exit(1)

        print("There are %d in table of ANL_RULE_ENGINE_STAGE_FACT_PRE" % __row_cnt)
        sql = "WITH temp AS (SELECT column_name FROM columns " \
              "WHERE table_schema = 'v_temp_schema' AND table_name = 'ANL_RULE_ENGINE_STAGE_FACT_PRE') " \
              "SELECT /*+ label(GX_OSM_RULE_ENGINE) */ 1 needToRename " \
              "WHERE 1 = (SELECT count(*) FROM temp WHERE column_name = 'MAJOR_CATEGORY') " \
              "AND 0 = (SELECT count(*) FROM temp WHERE column_name = 'OSM_MAJOR_CATEGORY');"
        _need_to_rename = self._dw_connection.query_scalar(sql)[0]

        if _need_to_rename == 1:
            sql = "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ *,MAJOR_CATEGORY as OSM_MAJOR_CATEGORY " \
                  "FROM ANL_RULE_ENGINE_STAGE_FACT_PRE"
        else:
            sql = "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ * AS FROM ANL_RULE_ENGINE_STAGE_FACT_PRE"

        self._dw_connection.execute(sql)

        sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ COUNT(*) FROM ANL_RULE_ENGINE_STAGE_FACT"
        print("There are %d in table of ANL_RULE_ENGINE_STAGE_FACT" % (self._dw_connection.query_scalar(sql)[0]))

        # for advantage feedback, only consider the alert whose upc/store are in the upcmapping/storemapping list
        sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ " \
              "       CASE WHEN MAX(period_key) IS NULL THEN -1 ELSE MAX(period_key) END " \
              "FROM {schemaName}.ANL_FACT_FEEDBACK WHERE type='U' AND vendor_key = {VENDOR_KEY};"\
            .format(schemaName=self._schema_name,
                    VENDOR_KEY=self._vendor_key)
        loadid_for_upc_mapping = self._dw_connection.query_scalar(sql)[0]
        print("loadIDForUPCMapping is: ", loadid_for_upc_mapping)

        sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ " \
              "       CASE WHEN MAX(period_key) IS NULL THEN -1 ELSE MAX(period_key) END " \
              "FROM {schemaName}.ANL_FACT_FEEDBACK WHERE type='S' AND vendor_key = {VENDOR_KEY};"\
            .format(schemaName=self._schema_name,
                    VENDOR_KEY=self._vendor_key)
        load_id_for_store_mapping = self._dw_connection.query_scalar(sql)[0]
        print("loadIDForstoreMapping is: ", load_id_for_store_mapping)

        # TODO : also need to confirm below logic
        if loadid_for_upc_mapping != -1 and load_id_for_store_mapping != -1:
            print("Processing advantage feedback")
            if self._retailer_name.lower() == 'target'.lower():
                print('target retailer:')
                sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TEMP1"
                self._dw_connection.execute(sql)

                sql = "CREATE LOCAL TEMPORARY TABLE ANL_RULE_ENGINE_STAGE_FACT_TEMP1 ON COMMIT PRESERVE ROWS AS " \
                      "SELECT  alerts.*  " \
                      "FROM (SELECT a.*,b.TGT_UPC " \
                      "      FROM (SELECT *, 	CASE WHEN alertupc IS NULL OR alertupc = '' THEN upc " \
                      "                		ELSE alertupc END true_UPC " \
                      "    	     FROM ANL_RULE_ENGINE_STAGE_FACT) a " \
                      "	  JOIN (select * from {schemaName}.OLAP_ITEM) b " \
                      "	  ON a.true_UPC = b.UPC " \
                      "	  WHERE b.TGT_UPC is not null ) alerts , " \
                      "(SELECT DISTINCT Merchandiser_UPC AS UPC " \
                      " FROM {schemaName}.ANL_FACT_FEEDBACK " \
                      " WHERE period_key = {loadIDForUPCMapping} AND vendor_key = {VENDOR_KEY}) upc , " \
                      "(SELECT DISTINCT Store_Id " \
                      "	FROM {schemaName}.ANL_FACT_FEEDBACK " \
                      "	WHERE period_key = {loadIDForstoreMapping} AND vendor_key = {VENDOR_KEY}) store " \
                      "WHERE alerts.storeid = store.store_id " \
                      "AND right(repeat('0',20)||alerts.TGT_UPC,20) = right(repeat('0',20)||upc.UPC,20)"\
                    .format(schemaName=self._schema_name,
                            loadIDForstoreMapping=load_id_for_store_mapping,
                            loadIDForUPCMapping=loadid_for_upc_mapping,
                            VENDOR_KEY=self._vendor_key)
                self._dw_connection.execute(sql)
            else:
                print('default retailer:')
                sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TEMP1"
                self._dw_connection.execute(sql)
                sql = "CREATE LOCAL TEMPORARY TABLE ANL_RULE_ENGINE_STAGE_FACT_TEMP1 ON COMMIT PRESERVE ROWS AS " \
                      "SELECT alerts.* " \
                      "FROM ( SELECT *, CASE WHEN alertupc IS NULL OR alertupc = '' " \
                      "                 THEN upc ELSE alertupc END true_UPC " \
                      "       FROM ANL_RULE_ENGINE_STAGE_FACT ) alerts , " \
                      "     ( SELECT DISTINCT right(repeat('0',20)||Inner_UPC,20) as upc " \
                      "       FROM {schemaName}.ANL_FACT_FEEDBACK " \
                      "       WHERE period_key = {loadIDForUPCMapping} AND vendor_key = {VENDOR_KEY}) upc , " \
                      "     ( SELECT DISTINCT Store_Id " \
                      "       FROM {schemaName}.ANL_FACT_FEEDBACK " \
                      "       WHERE period_key = {loadIDForstoreMapping} AND vendor_key = {VENDOR_KEY}) store " \
                      "WHERE alerts.storeid = store.store_id AND right(repeat('0',20)||alerts.true_UPC,20) = upc.UPC"\
                    .format(schemaName=self._schema_name,
                            loadIDForUPCMapping=loadid_for_upc_mapping,
                            loadIDForstoreMapping=load_id_for_store_mapping,
                            VENDOR_KEY=self._vendor_key)
                self._dw_connection.execute(sql)

            sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT"
            self._dw_connection.execute(sql)

            sql = "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ * FROM ANL_RULE_ENGINE_STAGE_FACT_TEMP1"
            self._dw_connection.execute(sql)

        print("-------------- Calling GenRuleEngineStageData class ended--------------\n")

if __name__ == '__main__':
    gen_stage = GenRuleEngineStageData('', '', 'context')
    gen_stage.gen_stage_table()

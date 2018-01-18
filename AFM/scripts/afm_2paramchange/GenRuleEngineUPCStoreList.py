# from DBOperation import *


class GenRuleEngineUPCStoreList(object):

    def __init__(self, conn, context):
        # self.dw_connection = DWAccess()
        self._dw_connection = conn
        self._context = context
        self._schema_name = context["SCHEMA_NAME"]

    def process(self):
        print("\n-------------- Calling GenRuleEngineUPCStoreList function --------------")
        sql = "CREATE LOCAL TEMP TABLE anl_rule_engine_stage_rule_list_temp1 ON COMMIT PRESERVE ROWS AS " \
              "SELECT /*+label(GX_OSM_RULE_ENGINE)*/ FILE_ID, ATTRIBUTE_NAME, attribute_value, upc, STOREID " \
              "FROM (SELECT a.FILE_ID, " \
              "             CASE WHEN a.UPC = '-' THEN 'STORE_ID' WHEN a.storeid = '-' THEN 'UPC' " \
              "                  ELSE 'STOREID||''-''||UPC' END attribute_name,  " \
              "             CASE WHEN a.UPC = '-' THEN c.store_id WHEN a.storeid = '-' THEN b.upc " \
              "                  ELSE c.store_id || '-' || b.upc END attribute_value, a.upc, a.storeid " \
              "      FROM {schemaName}.ANL_RULE_ENGINE_UPC_STORE_LIST a " \
              "      LEFT JOIN {schemaName}.olap_item_osm b  " \
              "	     ON (CASE WHEN ISNUMFORMAT(a.UPC) = 'true' THEN CAST(CAST(a.UPC as DECIMAL(20, 0)) AS VARCHAR(100)) ELSE a.UPC END) =  " \
              "	        (CASE WHEN ISNUMFORMAT(b.UPC) = 'true' THEN CAST(CAST(b.UPC as DECIMAL(20, 0)) AS VARCHAR(100)) ELSE b.UPC END) " \
              "      LEFT JOIN {schemaName}.olap_STORE c  " \
              "	     ON (CASE WHEN ISNUMFORMAT(a.storeid) = 'true' THEN CAST(CAST(a.storeid as DECIMAL(20, 0)) AS VARCHAR(100)) ELSE a.storeid END) =  " \
              "	        (CASE WHEN ISNUMFORMAT(c.store_id) = 'true' THEN  CAST(CAST(c.store_id as DECIMAL(20, 0)) AS VARCHAR(100)) ELSE c.store_id END) " \
              ") x;".format(schemaName = self._schema_name)
        self._dw_connection.execute(sql)

        sql = "SELECT /*+label(GX_OSM_RULE_ENGINE)*/ COUNT(*) FROM anl_rule_engine_stage_rule_list_temp1"
        print("There are %d rows in table of anl_rule_engine_stage_rule_list_temp1" % (self._dw_connection.query_scalar(sql)[0]))

        sql = "TRUNCATE TABLE {schemaName}.anl_rule_engine_stage_rule_list; " \
              "INSERT INTO {schemaName}.anl_rule_engine_stage_rule_list " \
              "SELECT /*+label(GX_OSM_RULE_ENGINE)*/  FILE_ID::VARCHAR(10), " \
              "       ATTRIBUTE_NAME::VARCHAR(512), attribute_value::VARCHAR(512) AS value " \
              "FROM anl_rule_engine_stage_rule_list_temp1 " \
              "WHERE attribute_value IS NOT NULL".format(schemaName = self._schema_name)
        self._dw_connection.execute(sql)

        sql = "SELECT /*+label(GX_OSM_RULE_ENGINE)*/ COUNT(*) " \
              "FROM {schemaName}.anl_rule_engine_stage_rule_list".format(schemaName=self._schema_name)
        print("There are %d rows in table of %s.anl_rule_engine_stage_rule_list" % (self._dw_connection.query_scalar(sql)[0], self._schema_name))

        sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_RULE_LIST_ERROR; " \
              "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_RULE_LIST_ERROR ON COMMIT PRESERVE ROWS AS " \
              "SELECT /*+label(GX_OSM_RULE_ENGINE)*/ FILE_ID, upc, storeid " \
              "FROM anl_rule_engine_stage_rule_list_temp1 " \
              "WHERE attribute_value IS NULL"
        self._dw_connection.execute(sql)

        sql = "SELECT /*+label(GX_OSM_RULE_ENGINE)*/ COUNT(*) FROM ANL_RULE_ENGINE_STAGE_RULE_LIST_ERROR"
        error_row_count = self._dw_connection.query_scalar(sql)[0]
        print("There are %d rows in the table of ANL_RULE_ENGINE_STAGE_RULE_LIST_ERROR" % error_row_count)
        if error_row_count != 0:
            # Write-Log $sqlConn "rule engine" 99999 'some upc/store can not find matching mater data, please check the table of ANL_RULE_ENGINE_STAGE_RULE_LIST_ERROR' $warning
            print('Some upc/store can not find matching mater data, please check the table of ANL_RULE_ENGINE_STAGE_RULE_LIST_ERROR - WARNING')

        print("-------------- Calling GenRuleEngineUPCStoreList class ended --------------\n")

if __name__ == '__main__':
    gen_stage = GenRuleEngineUPCStoreList('PEPSI_AHOLD_MB')
    gen_stage.process()
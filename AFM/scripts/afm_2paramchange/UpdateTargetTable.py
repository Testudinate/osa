# UpdateTargetTable  $verticaConn $sqlConn $schemaName


class UpdateTargetTable(object):

    def __init__(self, conn, context):
        self._dw_connection = conn
        self._context = context
        self._schema_name = context["SCHEMA_NAME"]

    def process(self, metrics_reject_reason):
        print("\n-------------- Calling UpdateTargetTable class --------------")
        sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_TEMP_FACT_TARGET_1"
        self._dw_connection.execute(sql)
        if metrics_reject_reason == '':
            sql = "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_TEMP_FACT_TARGET_1 ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ a.vendor_key, a.retailer_key, a.id, " \
                  "       CASE WHEN b.reject_reasons IS NULL OR b.reject_reasons = '' THEN a.reject_reasons " \
                  "       ELSE CASE WHEN a.reject_reasons IS NULL OR a.reject_reasons='' THEN '' " \
                  "       ELSE a.reject_reasons || ',' END || b.reject_reasons END AS reject_reasons " \
                  "FROM ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET a " \
                  "INNER JOIN ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP b " \
                  "ON a.id = b.id AND a.vendor_key = b.vendor_key AND a.retailer_key = b.retailer_key"
        else:
            sql = "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_TEMP_FACT_TARGET_1 ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ a.vendor_key, a.retailer_key, a.id, " \
                  "       CASE WHEN b.vendor_key IS NULL THEN a.reject_reasons " \
                  "       ELSE CASE WHEN a.reject_reasons IS NULL OR a.reject_reasons='' THEN '' " \
                  "       ELSE a.reject_reasons || ',' END || '{metricsRejectReason}' END AS reject_reasons " \
                  "FROM ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET a " \
                  "LEFT JOIN ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP b " \
                  "ON a.id = b.id AND a.vendor_key = b.vendor_key AND a.retailer_key = b.retailer_key".format(metricsRejectReason=metrics_reject_reason)
        print(sql)
        self._dw_connection.execute(sql)

        # rename table from RSI_ANL_RULE_ENGINE_TEMP_FACT_TARGET_1 to RSI_ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET
        sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET;" \
              "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET ON COMMIT PRESERVE ROWS AS " \
              "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ * FROM ANL_RULE_ENGINE_TEMP_FACT_TARGET_1"
        print(sql)
        self._dw_connection.execute(sql)

        print("-------------- Calling UpdateTargetTable class end --------------\n")

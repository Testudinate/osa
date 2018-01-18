
#UpdateAlertTable $verticaConn $sqlConn $schemaName $siloDBName


class UpdateAlertTable(object):

    def __init__(self, conn, context):
        self._dw_connection = conn
        self._context = context
        self._schema_name = context["SCHEMA_NAME"]
        self._vendor_key = context["VENDOR_KEY"]
        self._suffix = "_" + context["SUFFIX"]

    def process(self):
        print("\n --------- Calling UpdateAlertTable class -----------")

        sql = "DROP TABLE IF EXISTS {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL{suffix};" \
              "CREATE TABLE {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL{suffix} AS " \
              "SELECT /*+ label(GX_OSM_AFM) */ id, vendor_key, retailer_key, " \
              "       reject_reasons::VARCHAR(1000) AS reject_reasons, owner::VARCHAR(50) AS owner " \
              "FROM ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL " \
              "ORDER BY Vendor_key, retailer_key, id UNSEGMENTED ALL NODES;".format(schemaName=self._schema_name,
                                                                                    suffix=self._suffix)
        self._dw_connection.execute(sql)

        sql = "SELECT ANALYZE_STATISTICS('{schemaName}.ANL_FACT_OSM_INCIDENTS'); " \
              "SELECT ANALYZE_STATISTICS('{schemaName}.ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL{suffix}');"\
            .format(schemaName=self._schema_name,
                    suffix=self._suffix)
        self._dw_connection.execute(sql)

        sql = "UPDATE /*+ direct, label(GX_OSM_AFM) */ {schemaName}.ANL_FACT_OSM_INCIDENTS i " \
              "SET RejectReasons = ft.reject_reasons, " \
              "    owner = ft.OWNER," \
              "    IssuanceId = (CASE WHEN ft.OWNER IS NOT NULL AND ft.OWNER <> '' THEN 0 ELSE IssuanceId end) " \
              "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL{suffix} ft " \
              "WHERE ft.vendor_key=i.Vendor_key AND ft.retailer_key=i.Retailer_Key AND ft.id=i.IncidentID " \
              "AND i.vendor_key = {VENDOR_KEY};".format(schemaName=self._schema_name,
                                                        VENDOR_KEY=self._vendor_key,
                                                        suffix=self._suffix)
        self._dw_connection.execute(sql)

        # all alert tables are having the same columns with table ANL_FACT_ALERT_TEMPLATE
        sql = "SELECT column_name FROM columns WHERE table_name='ANL_FACT_OSM_INCIDENTS' " \
              "AND table_schema='{schemaName}'".format(schemaName=self._schema_name)
        _column_array = self._dw_connection.query_with_result(sql)
        insert_alert_columns = ','.join(["i.\"" + _column['COLUMN_NAME'] + "\"" for _column in _column_array])
        print(insert_alert_columns)
        insert_alert_columns = insert_alert_columns.replace('i."IssuanceId"', "(CASE WHEN ft.OWNER IS NOT NULL AND ft.OWNER <> '' THEN 0 ELSE IssuanceId END) AS IssuanceId")
        insert_alert_columns = insert_alert_columns.replace('i."OWNER"', 'NVL(ft.OWNER,i.Owner) AS Owner')
        insert_alert_columns = insert_alert_columns.replace('i."RejectReasons"', 'NVL(ft.reject_reasons,i.RejectReasons) AS RejectReasons')
        print(insert_alert_columns)

        _vendor_key = 55
        # suffix = customer.suffix
        _table_name='ANL_FACT_ALERT_' + str(_vendor_key)
        sql = "SELECT /*+label(GX_OSM_AFM)*/ COUNT(*) cnt FROM tables " \
              "WHERE table_name='{tableName}' AND table_schema='{schemaName}'".format(tableName=_table_name, schemaName=self._schema_name)
        _table_exists = self._dw_connection.query_scalar(sql)[0]

        sql = "SELECT COUNT(*) cnt FROM ANL_RULE_ENGINE_STAGE_META_ALERTS_SET WHERE vendor_key={vendorKey}".format(vendorKey=_vendor_key)
        # _meta_exists=self._dw_connection.query_scalar(sql)[0]

        # if _table_exists == 1 and _meta_exists != 0:
        if _table_exists == 1:
            sql = "SELECT * FROM ANL_RULE_ENGINE_STAGE_META_ALERTS_SET WHERE vendor_key={vendorKey}".format(vendorKey=_vendor_key)
            _alert_set = self._dw_connection.query_with_result(sql)[0]
            _seq_num = _alert_set['SEQ_NUM']
            _retailer_key = _alert_set['RETAILER_KEY']
            _period_key = _alert_set['ALERT_DAY_KEY']

        _seq_num = 1
        _retailer_key = 55
        _period_key = 20170801
        print(_seq_num, _retailer_key, _period_key)

        # forcedBackupTable = "${tableName}_back_raw_alert"
        #  if ($vendorKey -eq 5088){
        # 	$sql="select count(*) From INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA='dbo' and TABLE_NAME='$forcedBackupTable'"
        # 	 if ($sqlConn.queryscalar($sql) -eq 1){
        # 		Write-Log $sqlConn "rule engine" 99999 "table of $forcedBackupTable already exists, fix the issue by dropping the table" "warning"
        # 		$sqlConn.execute("drop table $forcedBackupTable")
        # 	 }
        # 	$sql="select * into $forcedBackupTable from $tableName where 0=1;
        # 		insert into $forcedBackupTable select * from $tableName where period_key=$periodKey "
        # 	$sqlConn.execute($sql)
        #  }
        # sql="delete from $tableName where period_key=$periodKey"
        # sqlConn.execute($sql)

        # sql="SELECT /*+ label(GX_OSM_AFM) */ * from $schemaName.$tableName where period_key=$periodKey and seq_num=$seqNum and InterventionKey<>1"
        # NXG-17438 : move above time-consuming update out to step11 in PublishAlert job. and change $sql as below.
        # NXG-18098: only sync alerts where issuanceid = 0
        sql = """SELECT /*+ label(gx_osm_afm) */ {insertalertcolumns}
            FROM {schemaname}.{tablename} i
            LEFT JOIN {schemaname}.anl_rule_engine_stage_fact_target_final ft
            ON ft.vendor_key=i.vendor_key AND ft.retailer_key=i.retailer_key AND ft.id=i.alert_id
            WHERE i.period_key={periodkey} AND i.seq_num={seqnum} AND interventionkey<>1
            AND (CASE WHEN ft.owner IS NOT NULL AND ft.owner <> '' THEN 0 ELSE issuanceid END) = 0"""\
            .format(insertalertcolumns=insert_alert_columns, schemaname=self._schema_name,
                    tablename=_table_name, periodkey=_period_key, seqnum=_seq_num)
        print(sql)

        print("---------Calling UpdateAlertTable class ended ----------\n")

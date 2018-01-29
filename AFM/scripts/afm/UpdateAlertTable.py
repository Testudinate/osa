
#UpdateAlertTable $verticaConn $sqlConn $schemaName $siloDBName


class UpdateAlertTable(object):

    def __init__(self, conn, context):
        self._dw_connection = conn
        self._context = context
        self._schema_name = context["SCHEMA_NAME"]
        self._vendor_key = context["VENDOR_KEY"]
        self._suffix = "_" + context["SUFFIX"]
        self._seq_num = self._context["SEQ_NUM"]
        self._period_key = self._context["PERIOD_KEY"]

    def update_alert_table(self):
        """
        We are separating raw alerts and issued alerts in different tables.
        This is to insert/update the issued alerts into target table ANL_FACT_ALERT
        :return: no returned value
        """
        self.__process()

    def __process(self):
        print("\n --------- Calling UpdateAlertTable class -----------")

        sql = "DROP TABLE IF EXISTS {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL{suffix};" \
              "CREATE TABLE {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL{suffix} AS " \
              "SELECT /*+ label(GX_OSM_AFM) */ id, vendor_key, retailer_key, " \
              "       reject_reasons::VARCHAR(1000) AS reject_reasons, owner::VARCHAR(50) AS owner " \
              "FROM ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL " \
              "ORDER BY Vendor_key, retailer_key, id UNSEGMENTED ALL NODES;".format(schemaName=self._schema_name,
                                                                                    suffix=self._suffix)
        print(sql)
        self._dw_connection.execute(sql)

        sql = "SELECT ANALYZE_STATISTICS('{schemaName}.ANL_FACT_OSM_INCIDENTS'); " \
              "SELECT ANALYZE_STATISTICS('{schemaName}.ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL{suffix}');"\
            .format(schemaName=self._schema_name,
                    suffix=self._suffix)
        print(sql)
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
        print(sql)
        # self._dw_connection.execute(sql)

        _stage_table_name = 'ANL_FACT_ALERT_SWAP_STAGING{suffix}'.format(suffix=self._suffix)
        _target_table_name = 'ANL_FACT_ALERT'
        # all alert tables are having the same columns with table ANL_FACT_ALERT_TEMPLATE
        sql = "SELECT column_name FROM columns WHERE table_name='ANL_FACT_ALERT' " \
              "AND table_schema='{schemaName}'".format(schemaName=self._schema_name)
        _column_array = self._dw_connection.query_with_result(sql)
        insert_columns = ','.join(["i.\"" + _column['COLUMN_NAME'] + "\"" for _column in _column_array])
        print(insert_columns)
        insert_columns = insert_columns.replace('i."IssuanceId"',
                                                "(CASE WHEN ft.OWNER IS NOT NULL AND ft.OWNER <> '' THEN 0 ELSE IssuanceId END) AS IssuanceId")
        insert_columns = insert_columns.replace('i."OWNER"', 'NVL(ft.OWNER,i.Owner) AS Owner')
        insert_columns = insert_columns.replace('i."RejectReasons"',
                                                'NVL(ft.reject_reasons,i.RejectReasons) AS RejectReasons')
        insert_columns = insert_columns.replace('i."ALERT_ID"', 'IncidentID as ALERT_ID')
        insert_columns = insert_columns.replace('i."NUMBER_OF_FACINGS"', 'NULL AS NUMBER_OF_FACINGS')
        insert_columns = insert_columns.replace('i."POG_DESCRIPTION"', 'NULL AS POG_DESCRIPTION')
        insert_columns = insert_columns.replace('i."POG_LOCATION"', 'NULL AS POG_LOCATION')
        insert_columns = insert_columns.replace('i."FIRST_PUBLISH_DATE_PERIOD_KEY"',
                                                "to_number(to_char(FirstPublishDate,'YYYYMMDD')) AS FIRST_PUBLISH_DATE_PERIOD_KEY")
        insert_columns = insert_columns.replace('i."LAST_PUBLISH_DATE_PERIOD_KEY"',
                                                "to_number(to_char(LastPublishDate,'YYYYMMDD')) AS LAST_PUBLISH_DATE_PERIOD_KEY")
        print(insert_columns)

        sql = "SELECT /*+label(GX_OSM_AFM)*/ COUNT(*) cnt FROM tables " \
              "WHERE table_name='{tableName}' AND table_schema='{schemaName}'"\
            .format(tableName=_target_table_name, schemaName=self._schema_name)
        # _table_exists = self._dw_connection.query_scalar(sql)[0]

        # if _table_exists == 1 and _meta_exists != 0:
        # TODO : same as next TODO list
        # BUG :
        # if _table_exists == 1:
        #     pass
            # sql = "SELECT * FROM ANL_META_RAW_ALERTS_SEQ WHERE vendor_key={vendor_key} and retailer_key = {retailer_key}"\
            #     .format(vendorKey=self._vendor_key)
            # _alert_set = self._app_connection.query_with_result(sql)[0]
            # _seq_num = _alert_set['SEQ_NUM']
            # _retailer_key = _alert_set['RETAILER_KEY']
            # _period_key = _alert_set['ALERT_DAY_KEY']

        print(self._seq_num, self._period_key)

        # TODO : to confirm below Forced logic
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

        # Table partitions are not supported for temporary tables.
        # So create a staging table with same structure to store previous days of data for the same vendor.
        # since we are not going to touch target table directly instead of using swap_partition.
        sql = "DROP TABLE IF EXISTS {schema_name}.{stage_table}; " \
              "CREATE TABLE {schema_name}.{stage_table} LIKE {schema_name}.{target_table}"\
            .format(schema_name=self._schema_name,
                    stage_table=_stage_table_name,
                    target_table=_target_table_name)
        print(sql)
        self._dw_connection.execute(sql)

        # move previous days of data into temp table.
        sql = "SELECT MOVE_PARTITIONS_TO_TABLE(" \
              "'{schema_name}.{target_table}', " \
              "{vendor_key}, " \
              "{vendor_key}, " \
              "'{schema_name}.{stage_table}')"\
            .format(schema_name=self._schema_name,
                    target_table=_target_table_name,
                    vendor_key=self._vendor_key,
                    stage_table=_stage_table_name)
        print(sql)
        self._dw_connection.execute(sql)

        # delete issued alerts on the same day if re-run AFM
        sql = "DELETE FROM {schema_name}.{table_name} " \
              "WHERE vendor_key = {vendor_key} AND period_key = {period_key} "\
            .format(schema_name=self._schema_name,
                    table_name=_stage_table_name,
                    vendor_key=self._vendor_key,
                    period_key=self._period_key)
        print(sql)
        self._dw_connection.execute(sql)

        # get the issued alerts
        # sql="SELECT /*+ label(GX_OSM_AFM) */ * from $schemaName.$tableName where period_key=$periodKey and seq_num=$seqNum and InterventionKey<>1"
        # NXG-17438 : move above time-consuming update out to step11 in PublishAlert job. and change $sql as below.
        # NXG-18098: only sync alerts where issuanceid = 0
        # TODO : we won't get data from source table directly if there is already a staging table. will verify.
        # consider using ANL_RULE_ENGINE_STAGE_FACT instead of ANL_FACT_OSM_INCIDENTS
        sql = """INSERT INTO {schemaName}.{stage_table}
            SELECT /*+ label(gx_osm_afm) */ {target_table_columns}
            FROM ANL_RULE_ENGINE_STAGE_FACT i
            LEFT JOIN {schemaName}.anl_rule_engine_stage_fact_target_final{suffix} ft
            ON ft.vendor_key=i.vendor_key AND ft.retailer_key=i.retailer_key AND ft.id=i.incidentid
            WHERE i.period_key={period_key} AND i.seq_num={seq_num} AND interventionkey<>1
            AND i.vendor_key = {vendor_key}
            AND (CASE WHEN ft.owner IS NOT NULL AND ft.owner <> '' THEN 0 ELSE issuanceid END) = 0"""\
            .format(schemaName=self._schema_name,
                    stage_table=_stage_table_name,
                    target_table_columns=insert_columns,
                    period_key=self._period_key,
                    seq_num=self._seq_num,
                    vendor_key=self._vendor_key,
                    suffix=self._suffix)
        print(sql)
        self._dw_connection.execute(sql)

        # swap partitions back into target table
        sql = "SELECT SWAP_PARTITIONS_BETWEEN_TABLES( " \
              "'{schema_name}.{table_name}', " \
              "{vendor_key}, " \
              "{vendor_key}, " \
              "'{schema_name}.{target_table}')"\
            .format(schema_name=self._schema_name,
                    target_table=_target_table_name,
                    vendor_key=self._vendor_key,
                    table_name=_stage_table_name)
        print(sql)
        self._dw_connection.execute(sql)
        print("---------Calling UpdateAlertTable class ended ----------\n")

# Run-RuleEngineSpecialRule - verticaConn $verticaConn - sqlConn $sqlConn - hubConn $hubConn - schemaName $schemaName - hubID $hubID
# - ruleSetId $ruleSetId - ruleID $ruleID - providerSubType $providerSubType - silo_type $silo_typeOfSiloRunningAFM - siloID $siloID
# - interventionKeyList $interventionKeyList
import datetime

from GetSQLSubLevelFilter import *
from UpdateTargetTable import *
from RunFeedback import *
from removeDuplication import *


class RunRuleEngineSpecialRule(object):
    def __init__(self, conn, context):
        # self._dw_connection = dbo.DWAccess()
        self._dw_connection = conn
        self._context = context
        self._schema_name = context["SCHEMA_NAME"]
        self._suffix = "_" + context["SUFFIX"]
        self._afm_silo_type = self._context['SILO_TYPE']
        # self._hub_id = hub_id
        self._run_feedback = None
        self._remove_duplication = RemoveDuplication(self._dw_connection, self._context)
        self._get_sub_level_filter = GetSQLSubLevelFilter(self._dw_connection, self._context)
        self._update_target = UpdateTargetTable(self._dw_connection,self._context)

    def special_rule_processing(self, rule_set_id, rule_id, provider_sub_type, intervention_key_list):
        """
        Processing All rules EXCEPT METRICS_TYPE = "Value Filter"

        :param rule_set_id: rule_set_id from table ANL_RULE_ENGINE_RULE_SET
        :param rule_id: ANL_RULE_ENGINE_META_RULES
        :param provider_sub_type:  ANL_RULE_ENGINE_META_RULES.provider_sub_type
        :param intervention_key_list:
        :return:    no returned value
        """
        self.__process(rule_set_id, rule_id, provider_sub_type, intervention_key_list)

    def __process(self, rule_set_id, rule_id, provider_sub_type, intervention_key_list):
        print("\n-------------- Calling RunRuleEngineSpecialRule class --------------")
        # param($verticaConn,$sqlConn,$hubconn,{schemaName},$hubID,{ruleSetId},$ruleID,$providerSubType,{interventionKeyList},$silo_type,$siloID)

        if self._afm_silo_type == 'SVR':
            pass
            # . "$PSScriptRoot\RunFeedbackSVR.ps1"

        if self._afm_silo_type == 'WM':
            pass
            # . "PSScriptRoot\RunFeedbackWM.ps1"

        if self._afm_silo_type == 'ALERT':
            pass
            # . "$PSScriptRoot\RunFeedbackAlert.ps1"

        self._dw_connection.execute("drop table if exists {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_3".format(schemaName = self._schema_name))
        sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ CASE WHEN parameter1 IS NULL THEN '' ELSE parameter1 END AS parameter1, " \
              "CASE WHEN parameter2 IS NULL THEN '' ELSE parameter2 END AS parameter2, " \
              "CASE WHEN parameter3 IS NULL THEN '' ELSE parameter3 END AS parameter3, " \
              "CASE WHEN SUB_LEVEL_METRICS IS NULL THEN '' ELSE SUB_LEVEL_METRICS END AS SUB_LEVEL_METRICS_new " \
              "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_RULES{suffix} WHERE rule_set_id = {ruleSetId} AND rule_id = {ruleID}" \
            .format(schemaName=self._schema_name, ruleSetId=rule_set_id, ruleID=rule_id, suffix=self._suffix)
        _rule_row = self._dw_connection.query_with_result(sql)[0]
        print(_rule_row, type(_rule_row))
        _parameter1 = _rule_row['PARAMETER1']
        _parameter2 = _rule_row['PARAMETER2']
        _parameter3 = _rule_row['PARAMETER3']
        _sub_level_metrics = _rule_row['SUB_LEVEL_METRICS_NEW']

        sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ * FROM {schemaName}.ANL_RULE_ENGINE_META_RULES " \
              "WHERE rule_id = {ruleID}".format(schemaName=self._schema_name, ruleID=rule_id)
        _meta_rule_row = self._dw_connection.query_with_result(sql)[0]
        # print(_meta_rule_row, type(_meta_rule_row))
        _metrics_type = _meta_rule_row['METRICS_TYPE']
        _metrics_name = _meta_rule_row['METRICS_NAME']
        _metrics_reject_reason = _meta_rule_row['METRICS_REJECT_REASON']
        _filter_name = _meta_rule_row['FILTER_NAME']

        sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ * FROM {schemaName}.ANL_RULE_ENGINE_RULE_SET " \
              "WHERE RULE_SET_ID = {ruleSetId}".format(schemaName=self._schema_name, ruleSetId=rule_set_id)
        _rule_set_rows = self._dw_connection.query_with_result(sql)[0]
        # print(_rule_set_rows, type(_rule_set_rows))
        # _silo_id = _rule_set_rows['SILO_ID']
        _data_provider_name = _rule_set_rows['DATA_PROVIDER_NAME']

        sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ * FROM {schemaName}.ANL_RULE_ENGINE_META_DATA_PROVIDERS " \
              "WHERE data_provider_name = '{dataProviderName}'".format(schemaName=self._schema_name,
                                                                       dataProviderName=_data_provider_name)
        _provider_row = self._dw_connection.query_with_result(sql)[0]
        # print(_provider_row, type(_provider_row))
        _provider_pk_column = _provider_row['PROVIDER_BASE_TABLE_PK_COLUMN']

        if _metrics_type == 'store-alertType filter':
            sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP; " \
                  "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ a.vendor_key vendor_key, a.retailer_key retailer_key, a.IncidentID as id " \
                  "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix} a, " \
                  "{schemaName}.ANL_DIM_OSM_INTERVENTIONCLASSIFICATION b " \
                  "WHERE a.InterventionKey=b.InterventionKey AND COALESCE(a.RSI_ALERT_TYPE,0) & b.AlertIntegerType=0 " \
                  "AND b.InterventionKey in ({interventionKeyList})".format(schemaName=self._schema_name,
                                                                            interventionKeyList=intervention_key_list,
                                                                            suffix=self._suffix)
            self._dw_connection.execute(sql)
            self._update_target.update_target_table(_metrics_reject_reason)

        # _metrics_type = 'fEEdback'
        if (_metrics_type.lower() == 'Product Filter'.lower() or _metrics_type.lower() == 'Store Filter'.lower()
            or _metrics_type.lower() == 'Store-Product Filter'.lower() or _metrics_type.lower() == 'Feedback'.lower()):
            _file_id = _parameter1
            self._dw_connection.execute("DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP")
            if _metrics_type.lower() == "Product Filter".lower():
                _join_column = 'UPC'
            if _metrics_type.lower() == "Store Filter".lower():
                _join_column = 'Storeid'
            if _metrics_type.lower() == "Store-Product Filter".lower():
                _join_column = "Storeid||'-'||upc"
            if _parameter2.lower() == 'inclusive list'.lower():
                _in_not_in = ' NOT IN '
            if _parameter2.lower() == 'exclusive list'.lower():
                _in_not_in = ' IN '
            if _metrics_type.lower() == 'feedback'.lower():
                _file_id = 'feedback'
                _in_not_in = ' IN '
                _join_column = "CAST(vendor_key AS VARCHAR(20)) ||'-' || CAST(retailer_key AS VARCHAR(20)) || '-' ||storeid || '-' || upc || '-' || COALESCE(sublevel.SUB_LEVEL_VALUE,'default')"
                # Run-Feedback $verticaConn $sqlConn $hubConn {schemaName} $hubID -inaccurateAlertsDys $parameter1
                # -notInPlanogramDays $parameter2 -trueAlertDays $parameter3 -ruleSetId {ruleSetId} -ruleID $ruleID -siloID $siloID
                self._run_feedback = RunFeedback(self._dw_connection, self._context, _parameter1,
                                                 _parameter2, _parameter3, rule_set_id, rule_id)
                self._run_feedback.processing_feedback()
                sql = "INSERT /*+ DIRECT, label(GX_OSM_RULE_ENGINE)*/ INTO {schemaName}.ANL_RULE_ENGINE_STAGE_RULE_LIST " \
                      "SELECT 'feedback','vendorkey-retailerkey-Store-UPC', " \
                      "CAST(vendorkey AS VARCHAR(20)) ||'-' || CAST(retailerkey AS VARCHAR(20)) || '-' ||StoreUPC " \
                      "FROM {schemaName}.ANL_RULE_ENGINE_AFM_FEEDBACK_STAGE{suffix}"\
                    .format(schemaName=self._schema_name,
                            suffix=self._suffix)
                print(sql)
                self._dw_connection.execute(sql)

            print(_join_column)
            # NXG-17764: in case "Alert Type" is not enabled.
            sql = "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP ON COMMIT preserve ROWS as " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ vendor_key,retailer_key,{providerBaseTablePkColumn} as id " \
                  "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix} ruleset " \
                  "LEFT JOIN {schemaName}.ANL_RULE_ENGINE_SUB_LEVEL_FILTER sublevel " \
                  "ON ruleset.AlertSubType = sublevel.SUB_LEVEL_VALUE AND sublevel.rule_set_id = {ruleSetId} " \
                  "AND sublevel.rule_id = {ruleID} AND SUB_LEVEL_CATEGORY = 'Alert Type' " \
                  "WHERE {joinColumn} {inNotIn} (SELECT value FROM {schemaName}.ANL_RULE_ENGINE_STAGE_RULE_LIST WHERE file_id='{fileID}') " \
                  "AND interventionkey IN ({interventionKeyList});".format(providerBaseTablePkColumn=_provider_pk_column,
                                                                           schemaName=self._schema_name,
                                                                           ruleID=rule_id,
                                                                           ruleSetId=rule_set_id,
                                                                           joinColumn=_join_column,
                                                                           inNotIn=_in_not_in,
                                                                           fileID=_file_id,
                                                                           interventionKeyList=intervention_key_list,
                                                                           suffix=self._suffix)
            print(sql)
            self._dw_connection.execute(sql)

            sql = "DELETE /*+ DIRECT, label(GX_OSM_RULE_ENGINE)*/ " \
                  "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_RULE_LIST " \
                  "WHERE file_id='feedback'".format(schemaName=self._schema_name)
            print(sql)
            self._dw_connection.execute(sql)
            self._update_target.update_target_table(_metrics_reject_reason)

        # _metrics_type = 'Flagged Item'
        # _parameter1 = 10
        if (_metrics_type.lower() == 'Flagged Item'.lower()) or (_metrics_type.lower() == 'Flagged Store'.lower()):
            if _metrics_type.lower() == 'Flagged Item'.lower():
                _aggregate_column = 'STOREID'
                _sub_aggregate_column = 'UPC'
            if _metrics_type.lower() == 'Flagged Store'.lower():
                _aggregate_column = 'UPC'
                _sub_aggregate_column = 'STOREID'
            if intervention_key_list != '':
                flag_intervention_key_list = " WHERE InterventionKey IN ({interventionKeyList})".format(interventionKeyList=intervention_key_list)

            sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_TEMP_FACT_1; " \
                  "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_TEMP_FACT_1 ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ vendor_key,retailer_key,InterventionKey,{providerBaseTablePkColumn} as id, " \
                  "{aggregateColumn} AS aggregate_column, {subAggregateColumn} AS sub_aggregate_column " \
                  "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix} {interventionKeyList}" \
                .format(providerBaseTablePkColumn=_provider_pk_column,
                        aggregateColumn=_aggregate_column,
                        subAggregateColumn=_sub_aggregate_column,
                        schemaName=self._schema_name,
                        interventionKeyList=flag_intervention_key_list,
                        suffix=self._suffix)
            print(sql)
            self._dw_connection.execute(sql)

            sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_TEMP_FACT_2;" \
                  "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_TEMP_FACT_2 ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ retailer_key, vendor_key, " \
                  "       COUNT(DISTINCT aggregate_column) distinct_count " \
                  "FROM ANL_RULE_ENGINE_TEMP_FACT_1 GROUP BY retailer_key, vendor_key"
            print(sql)
            self._dw_connection.execute(sql)

            sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_TEMP_FACT_3; " \
                  "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_TEMP_FACT_3 ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ a.retailer_key, a.vendor_key, a.sub_aggregate_column, " \
                  "       1.0 * 100 * COUNT(*) / MAX(b.distinct_count) ratio " \
                  "FROM ANL_RULE_ENGINE_TEMP_FACT_1 a " \
                  "INNER JOIN ANL_RULE_ENGINE_TEMP_FACT_2 b " \
                  "ON a.retailer_key = b.retailer_key AND a.vendor_key = b.vendor_key {interventionKeyList} " \
                  "GROUP BY a.retailer_key, a.vendor_key, a.sub_aggregate_column"\
                .format(interventionKeyList=flag_intervention_key_list)
            print(sql)
            self._dw_connection.execute(sql)

            sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP; " \
                  "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ a.vendor_key,a.retailer_key,{providerBaseTablePkColumn} AS id " \
                  "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix} a " \
                  "INNER JOIN ANL_RULE_ENGINE_TEMP_FACT_3 b " \
                  "ON {subAggregateColumn}=b.sub_aggregate_column AND ratio > CAST({parameter1} AS FLOAT) " \
                  "AND a.vendor_key=b.vendor_key AND a.retailer_key=b.retailer_key {interventionKeyList}" \
                .format(providerBaseTablePkColumn=_provider_pk_column,
                        schemaName=self._schema_name,
                        subAggregateColumn=_sub_aggregate_column,
                        parameter1=_parameter1,
                        interventionKeyList=flag_intervention_key_list,
                        suffix=self._suffix)
            print(sql)
            self._dw_connection.execute(sql)
            self._update_target.update_target_table(_metrics_reject_reason)

        if _metrics_type.lower() == 'minimum departments'.lower():
            sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_TEMP_FACT_1; " \
                  "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_TEMP_FACT_1 ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ retailer_key,vendor_key,OSM_MAJOR_CATEGORY " \
                  "FROM ( SELECT retailer_key,vendor_key,OSM_MAJOR_CATEGORY, " \
                  "             ROW_NUMBER() OVER (PARTITION BY retailer_key,vendor_key ORDER BY totalExpectedLostSalesAmount DESC) idx " \
                  "       FROM ( SELECT a.retailer_key,a.vendor_key,OSM_MAJOR_CATEGORY, " \
                  "                     SUM(ExpectedLostSalesAmount) totalExpectedLostSalesAmount " \
                  "              FROM  {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix} a " \
                  "              INNER JOIN ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET b " \
                  "              ON a.{providerBaseTablePkColumn} = b.id AND a.vendor_key = b.vendor_key AND a.retailer_key = b.retailer_key " \
                  "              AND a.InterventionKey IN ({interventionKeyList}) " \
                  "              AND (b.reject_reasons IS NULL OR b.reject_reasons ='' " \
                  "                   OR b.reject_reasons LIKE '%clear reject reason caused by pick up rule)' " \
                  "                   OR b.reject_reasons LIKE '%clear reject reason caused by pick up rule' " \
                  "                 ) GROUP BY OSM_MAJOR_CATEGORY,a.retailer_key,a.vendor_key" \
                  "             ) x " \
                  ")y WHERE idx <={parameter1}".format(schemaName=self._schema_name,
                                                       providerBaseTablePkColumn=_provider_pk_column,
                                                       interventionKeyList=intervention_key_list,
                                                       parameter1=_parameter1,
                                                       suffix=self._suffix)
            print(sql)
            self._dw_connection.execute(sql)

            sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP; " \
                  "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ a.retailer_key,a.vendor_key,a.incidentid AS id " \
                  "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET a " \
                  "LEFT JOIN ANL_RULE_ENGINE_TEMP_FACT_1 b " \
                  "ON a.retailer_key=b.retailer_key AND a.vendor_key=b.vendor_key " \
                  "AND COALESCE(a.OSM_MAJOR_CATEGORY,'')=COALESCE(b.OSM_MAJOR_CATEGORY,'') " \
                  "WHERE b.vendor_key IS NULL".format(schemaName=self._schema_name)
            print(sql)
            self._dw_connection.execute(sql)
            self._update_target.update_target_table(_metrics_reject_reason)

        # _filter_name = 'already issued'
        # _parameter3 = "MARKET_CLUSTER LIKE '%Supercenter%'"
        if _filter_name.lower() == 'already issued':
            _sql_max_period = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ COALESCE(MAX(period_key),20010101) " \
                           "FROM {schemaName}.ANL_FACT_OSM_INCIDENTS;".format(schemaName=self._schema_name)
            _period_key_for_alert = self._dw_connection.query_scalar(_sql_max_period)[0]
            # # $date = [datetime]::ParseExact("$periodKeyForAlert", "yyyyMMdd", $null)
            _date = datetime.datetime.strptime(str(_period_key_for_alert), '%Y%m%d')
            print("Max period key in 'already issued' rule is %s" % _period_key_for_alert)
            # _parameter1 = 'weekly'
            # _parameter2 = 'Friday'
            if _parameter1.lower() == 'weekly'.lower():
                while _date.strftime('%A') != _parameter2:
                    _date = _date - datetime.timedelta(days=1)
                _max_period_key_2check = _date.strftime('%Y%m%d')
                _min_period_key_2check = (_date - datetime.timedelta(days=6)).strftime('%Y%m%d')
                sql_period_clause = " pre.period_key BETWEEN {minPeriodKey2Check} AND {maxPeriodKey2Check} "\
                    .format(minPeriodKey2Check=_min_period_key_2check,
                            maxPeriodKey2Check=_max_period_key_2check)
            else:
                _period_key_2check = (_date - datetime.timedelta(days=1)).strftime('%Y%m%d')
                sql_period_clause = " pre.period_key = {periodKey2Check} ".format(periodKey2Check=_period_key_2check)

            print("Period (range) to check for already issued rule is: %s" % sql_period_clause)

            sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ COALESCE(MAX(period_key),20010101) FROM ANL_RULE_ENGINE_STAGE_FACT;"
            period_key_of_current_batch = self._dw_connection.query_scalar(sql)[0]
            sql= "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP; " \
                 "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP ON COMMIT PRESERVE ROWS AS " \
                 "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ current.vendor_key vendor_key, " \
                 "      current.retailer_key retailer_key, current.IncidentID as id " \
                 "FROM {schemaName}.olap_store s " \
                 "INNER JOIN {schemaName}.ANL_FACT_OSM_INCIDENTS current " \
                 "ON s.retailer_key = current.retailer_key AND s.store_key = current.store_key AND s.{parameter3} " \
                 "INNER JOIN {schemaName}.ANL_FACT_OSM_INCIDENTS pre " \
                 "ON current.itemnumber = pre.itemnumber AND current.storeid = pre.storeid " \
                 "AND pre.issuanceid = 0 AND {sqlPeriodClause} AND current.period_key = {periodKeyOfCurrentBatch}"\
                .format(schemaName=self._schema_name,
                        sqlPeriodClause= sql_period_clause,
                        periodKeyOfCurrentBatch=period_key_of_current_batch,
                        parameter3=_parameter3)
            print(sql)
            self._dw_connection.execute(sql)
            self._update_target.update_target_table(_metrics_reject_reason)

        # rule_id = 55
        # _parameter1 = 10
        # IZS alert count per store
        if rule_id == 55:
            sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ InterventionKey " \
                  "FROM {schemaName}.ANL_DIM_OSM_INTERVENTIONCLASSIFICATION WHERE AlertType='IZS'".format(schemaName=self._schema_name)
            _izs_intervention_key_list = ""
            for row in self._dw_connection.query_with_result(sql):
                _izs_intervention_key_list = _izs_intervention_key_list + str(row['INTERVENTIONKEY']) + ","
            print(_izs_intervention_key_list)
            _izs_intervention_key_list = _izs_intervention_key_list[:-1]
            print(_izs_intervention_key_list)

            sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP; " \
                  "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ retailer_key,vendor_key,id " \
                  "FROM ( SELECT a.retailer_key,a.vendor_key,b.id," \
                  "              ROW_NUMBER() OVER (PARTITION BY a.storeid ORDER BY a.rank_value_after_calculation DESC NULLS LAST) idx " \
                  "       FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET a " \
                  "       INNER JOIN ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET b " \
                  "       ON a.{providerBaseTablePkColumn}=b.id AND a.vendor_key = b.vendor_key " \
                  "       AND a.retailer_key = b.retailer_key " \
                  "       AND a.InterventionKey IN ({izsInterventionKeyList}) " \
                  "       AND (b.reject_reasons IS NULL OR b.reject_reasons ='' " \
                  "            OR b.reject_reasons like '%clear reject reason caused by pick up rule)' " \
                  "            OR b.reject_reasons like '%clear reject reason caused by pick up rule') " \
                  ") y WHERE y.idx > {parameter1}".format(schemaName=self._schema_name,
                                                          providerBaseTablePkColumn=_provider_pk_column,
                                                          izsInterventionKeyList=_izs_intervention_key_list,
                                                          parameter1=_parameter1)
            print(sql)
            self._dw_connection.execute(sql)
            self._update_target.update_target_table(_metrics_reject_reason)

        # rule_id = 56
        # _parameter2 = 11
        # Phantom alert count Dept count per store
        if rule_id == 56:
            sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ InterventionKey FROM " \
                  "{schemaName}.ANL_DIM_OSM_INTERVENTIONCLASSIFICATION " \
                  "WHERE AlertType='Phantom'".format(schemaName=self._schema_name)
            _phantom_intervention_key_list = ""
            for row in self._dw_connection.query_with_result(sql):
                _phantom_intervention_key_list = _phantom_intervention_key_list + str(row['INTERVENTIONKEY']) + ","
            print(_phantom_intervention_key_list)
            _phantom_intervention_key_list = _phantom_intervention_key_list[:-1]
            print(_phantom_intervention_key_list)

            sql = "DROP TABLE IF EXISTS {schemaName}.ANL_RULE_ENGINE_STAGE_PHANTOM_ALERT_DEPT; " \
                  "CREATE TABLE {schemaName}.ANL_RULE_ENGINE_STAGE_PHANTOM_ALERT_DEPT as " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ a.retailer_key, a.vendor_key, b.id, a.MAJOR_CATEGORY, " \
                  "       a.store_key, a.rank_value_after_calculation " \
                  "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET a " \
                  "INNER JOIN ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET b " \
                  "ON a.{providerBaseTablePkColumn}=b.id and a.vendor_key = b.vendor_key " \
                  "AND a.retailer_key = b.retailer_key AND a.InterventionKey in ({phantomInterventionKeyList}) " \
                  "AND (b.reject_reasons is null or b.reject_reasons ='' " \
                  "     OR b.reject_reasons LIKE '%clear reject reason caused by pick up rule)' " \
                  "     OR b.reject_reasons LIKE '%clear reject reason caused by pick up rule') " \
                  "SEGMENTED BY HASH(ID) ALL NODES".format(schemaName=self._schema_name,
                                                           providerBaseTablePkColumn=_provider_pk_column,
                                                           phantomInterventionKeyList=_phantom_intervention_key_list)
            print(sql)
            self._dw_connection.execute(sql)

            sql_depts = "SELECT * " \
                        "FROM ( SELECT major_category, retailer_key, store_key," \
                        "              ROW_NUMBER() OVER (PARTITION BY store_key ORDER BY cnt DESC) idx " \
                        "       FROM ( SELECT major_category, retailer_key, store_key, COUNT(*) cnt " \
                        "              FROM {schemaName}.ANL_RULE_ENGINE_STAGE_PHANTOM_ALERT_DEPT " \
                        "              GROUP BY major_category, store_key, retailer_key) x" \
                        ") y WHERE idx <= {parameter2}".format(schemaName=self._schema_name, parameter2=_parameter2)
            print(sql_depts)

            sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP; " \
                  "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP ON COMMIT PRESERVE ROWS AS " \
                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ a.retailer_key,a.vendor_key,a.id " \
                  "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_PHANTOM_ALERT_DEPT a " \
                  "LEFT JOIN (SELECT retailer_key, vendor_key, id " \
                  "           FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_key ORDER BY rank_value_after_calculation DESC NULLS LAST) idx " \
                  "                  FROM (SELECT b.* FROM ({sqlDepts}) a " \
                  "                        INNER JOIN {schemaName}.ANL_RULE_ENGINE_STAGE_PHANTOM_ALERT_DEPT b " \
                  "                        ON a.retailer_key = b.retailer_key AND a.major_category = b.major_category " \
                  "                        AND a.store_key = b.store_key) x" \
                  "           ) y WHERE idx <= {parameter1}) b " \
                  "ON a.retailer_key = b.retailer_key AND a.vendor_key = b.vendor_key AND a.id = b.id " \
                  "WHERE b.id IS NULL".format(sqlDepts=sql_depts, schemaName=self._schema_name, parameter1=_parameter1)
            print(sql)
            self._dw_connection.execute(sql)
            self._update_target.update_target_table(_metrics_reject_reason)

        # NXG-18098: handle duplicated store-item data.
        if _metrics_type.lower() == 'Duplicates Removal'.lower():
            # why we apply remove-duplication here.
            # Because we need to make sure we handled the duplicated store-item before processing "Volume Limit" rule.
            self._remove_duplication.remove_duplication()
            # $alreadyRemovedDuplication = $true

        # rule_id = 32
        # _parameter2 = 'STORE_KEY'
        # _parameter3 = 'UPC'
        # _metrics_type = 'Volume Limit'
        if _metrics_type.lower() == 'Volume Limit'.lower():
            # _filter_name = 'Number of Alerts/Store'
            if _filter_name.lower() == 'Number of Alerts/Store'.lower():
                sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_TEMP_FACT_3; " \
                      "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_TEMP_FACT_3 " \
                      "(sub_level VARCHAR(512), value INT) ON COMMIT PRESERVE ROWS"
                self._dw_connection.execute(sql)
                if _sub_level_metrics == '':
                    _temp_column = " 'default' AS sub_level, "
                    _sql_temp = "CASE WHEN idx> {parameter1} THEN 'reject for alert rank' ELSE '1' END " \
                                "AS \"RULE:{providerSubType} {filterName}\" ".format(parameter1=_parameter1,
                                                                                     providerSubType=provider_sub_type,
                                                                                     filterName=_filter_name)
                else:
                    _temp_column = " {subLevelMetrics} as sub_level, ".format(subLevelMetrics=_sub_level_metrics)
                    _sql_temp = self._get_sub_level_filter.get_sql(provider_sub_type, rule_id, rule_set_id, 'sub_level',
                                                                   'idx > parameter1', _metrics_reject_reason, _filter_name)
                print(_sql_temp)
                sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_TEMP_FACT_1; " \
                      "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_TEMP_FACT_1 ON COMMIT PRESERVE ROWS AS " \
                      "SELECT a.retailer_key,a.vendor_key,b.id,a.{parameter2} group_column, " \
                      "       {tempColumn} a.{parameter3} order_column " \
                      "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET a " \
                      "INNER JOIN ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET b " \
                      "ON a.{providerBaseTablePkColumn}=b.id AND a.vendor_key = b.vendor_key " \
                      "AND a.retailer_key = b.retailer_key AND a.InterventionKey IN ({interventionKeyList}) " \
                      "AND (b.reject_reasons IS NULL OR b.reject_reasons ='' " \
                      "     OR b.reject_reasons LIKE '%clear reject reason caused by pick up rule)' " \
                      "     OR b.reject_reasons LIKE '%clear reject reason caused by pick up rule');"\
                    .format(parameter2=_parameter2, tempColumn=_temp_column,
                            parameter3=_parameter3, schemaName=self._schema_name,
                            providerBaseTablePkColumn=_provider_pk_column,
                            interventionKeyList=intervention_key_list)
                print(sql)
                self._dw_connection.execute(sql)

                sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP; " \
                      "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP ON COMMIT preserve ROWS as " \
                      "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ retailer_key,vendor_key,id " \
                      "FROM (SELECT retailer_key,vendor_key,id,group_column,sub_level,idx ,{sqltemp} " \
                      "       FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY group_column,sub_level ORDER BY order_column DESC) idx " \
                      "              FROM ANL_RULE_ENGINE_TEMP_FACT_1) x " \
                      ")y WHERE \"RULE:{providerSubType} {filterName}\" <> '1';" \
                    .format(sqltemp=_sql_temp, providerSubType=provider_sub_type, filterName=_filter_name)
                print(sql)
                self._dw_connection.execute(sql)
                self._update_target.update_target_table(_metrics_reject_reason)

        print("-------------- Calling RunRuleEngineSpecialRule class end--------------\n")

if __name__ == '__main__':
    special = RunRuleEngineSpecialRule('PEPSI_AHOLD_MB', 'HUB_FUNCTION_MB')
    # special.process(rule_set_id, rule_id, provider_sub_type, silo_type, silo_id, intervention_key_list, hub_id):
    special.update_target_table('200132', 32, '*', 'S', 'PEPSI_AHOLD_MB', '1,2,3,4,5,6')

#!/usr/bin/python
# -*- coding: UTF-8 -*-

from CheckRuleEngine import *
from GenRuleEngineStageData import *
from GenRuleEngineUPCStoreList import *
from GetInterventionkeyList import *
from RunRuleEngineSpecialRule import *
from UpdateAlertTable import *
from UpdateTargetTable import *
# from common.DBOperation import *
from removeDuplication import *


class RunRuleEngine(object):

    def __init__(self, dw_conn, app_conn, context):
        # self._dw_connection = DWAccess()
        self._dw_connection = dw_conn
        self._app_connection = app_conn
        self._context = context
        self._vendor_key = self._context['VENDOR_KEY']
        self._retailer_key = self._context['RETAILER_KEY']
        self._schema_name = self._context['SCHEMA_NAME']
        self._suffix = "_" + self._context['SUFFIX']
        self.already_removed_duplication = False
        # self._log = Log() # to be coded
        # self._config = Config() # to be coded
        # self._SQLEngine = SQLEngine() # to be coded
        self._check_rule_engine = None
        # self._gen_stage_data = GenRuleEngineStageData(self._schema_name, self._silo_db_name, self._hub_id)
        self._gen_stage_data = None
        self._get_intervention_keys = GetInterventionKeyList(self._dw_connection, self._context)
        self._run_special_rule = RunRuleEngineSpecialRule(self._dw_connection, self._app_connection, self._context)
        self._get_sublevel_filter = GetSQLSubLevelFilter(self._dw_connection, self._app_connection, self._context)
        self._UpdateTargetTable = UpdateTargetTable(self._dw_connection, self._context)
        self._remove_duplication = RemoveDuplication(self._dw_connection, self._context)
        self._update_alert_table = UpdateAlertTable(self._dw_connection, self._context)
        self._gen_upc_store_list = GenRuleEngineUPCStoreList(self._dw_connection, self._context)

    def rule_process(self):
        try:
            print("Calling RunRuleEngine...")
            sql = "IF (OBJECT_ID('tempdb..#TMP_ANL_RULE_ENGINE_STAGE_COMMON_1') IS NOT NULL)" \
                  "    DROP TABLE #TMP_ANL_RULE_ENGINE_STAGE_COMMON_1 " \
                  "SELECT 'AFM' AS PROVIDER_NAME, a.DATA_PROVIDER_NAME, " \
                  "       a.rule_set_name, a.vendor_key, a.retailer_key, a.owner, " \
                  "       b.priority as owner_priority, 'S' AS type " \
                  "INTO #TMP_ANL_RULE_ENGINE_STAGE_COMMON_1 " \
                  "FROM (SELECT * FROM   " \
                  "        (SELECT *, ROW_NUMBER() OVER (PARTITION BY vendor_key, retailer_key, owner, rule_set_name " \
                  "                   ORDER BY created_date DESC) idx  " \
                  "			  FROM   ANL_RULE_ENGINE_RULE_SET " \
                  "          WHERE vendor_key = {VENDOR_KEY} AND retailer_key = {RETAILER_KEY}" \
                  "			) x WHERE idx = 1 " \
                  ") a " \
                  "INNER JOIN ANL_META_VR_OWNER_MAPPING b " \
                  "ON a.vendor_key = b.vendor_key AND a.retailer_key = b.retailer_key " \
                  "AND a.owner = b.owner;".format(VENDOR_KEY=self._vendor_key,
                                                  RETAILER_KEY=self._retailer_key)
            print(sql)
            self._app_connection.execute(sql)
            sql = "SELECT DISTINCT PROVIDER_NAME,DATA_PROVIDER_NAME FROM #TMP_ANL_RULE_ENGINE_STAGE_COMMON_1;"
            providers = self._app_connection.query_with_result(sql)

            for provider in providers:
                _provider_name = provider['PROVIDER_NAME']
                _data_provider_name = provider['DATA_PROVIDER_NAME']
                # "Write-Log $sqlConn "rule engine" 99999 "working on provider $providerName" $inProgress"
                print("Working on provider %s - in progress" % _provider_name)
                sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET;	" \
                      "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET " \
                      "( vendor_key INT,    " \
                      "retailer_key INT,    " \
                      "id BIGINT,           " \
                      "reject_reasons VARCHAR(4000),   " \
                      "OWNER VARCHAR(100),   " \
                      "is_alert_portal_rule_set CHAR(1) " \
                      ") ON COMMIT PRESERVE ROWS;"
                self._dw_connection.execute(sql)

                # TODO : no idea if below 2 tables are required anymore.
                # ANL_RULE_ENGINE_META_PROVIDERS & ANL_RULE_ENGINE_META_DATA_PROVIDERS
                # engine provider pre process
                sql = "SELECT CASE WHEN PROVIDER_PRE_PROCESSING_SP IS NULL THEN '' " \
                      "       ELSE PROVIDER_PRE_PROCESSING_SP END AS functionName " \
                      "FROM ANL_RULE_ENGINE_META_PROVIDERS " \
                      "WHERE PROVIDER_NAME ='{PROVIDER_NAME}' "\
                    .format(PROVIDER_NAME=_provider_name)
                _function_name = self._app_connection.query_scalar(sql)[0]
                print("PROVIDER_PRE_PROCESSING_SP is :", _function_name)
                self._check_rule_engine = eval("{FUNC_NAME}(self._dw_connection, self._app_connection, self._context)"
                                               .format(FUNC_NAME=_function_name.replace('-', '')))
                # self._check_rule_engine = CheckRuleEngine(self._dw_connection, self._context)
                self._check_rule_engine.check_rule_engine()

                sql = "SELECT CASE WHEN PROVIDER_PRE_PROCESSING_SP IS NULL THEN '' " \
                      "       ELSE PROVIDER_PRE_PROCESSING_SP END AS FUNCTION_NAME, " \
                      "       PROVIDER_POST_PROCESSING_SP,PROVIDER_BASE_TABLE_PK_COLUMN " \
                      "FROM ANL_RULE_ENGINE_META_DATA_PROVIDERS " \
                      "WHERE DATA_PROVIDER_NAME = '{DATA_PROVIDER_NAME}' "\
                    .format(DATA_PROVIDER_NAME=_data_provider_name)
                print(sql)
                row = self._app_connection.query_with_result(sql)[0]
                _function_name = row['FUNCTION_NAME']
                _provider_base_table_PK_column = row['PROVIDER_BASE_TABLE_PK_COLUMN']
                _data_provider_post_processing_function = row['PROVIDER_POST_PROCESSING_SP']
                print(_function_name, _provider_base_table_PK_column, _data_provider_post_processing_function)
                # self._gen_stage_data = GenRuleEngineStageData(self._dw_connection, self._app_connection, self._context)
                self._gen_stage_data = eval("{FUNC_NAME}(self._dw_connection, self._app_connection, self._context)"
                                            .format(FUNC_NAME=_function_name.replace('-', '')))
                self._gen_stage_data.gen_stage_table()

                self._gen_upc_store_list.gen_stage_rule_list_table()

                sql = "SELECT  rule_set_name,owner " \
                      "FROM #TMP_ANL_RULE_ENGINE_STAGE_COMMON_1 " \
                      "WHERE PROVIDER_NAME = '{PROVIDER_NAME}' AND data_provider_name = '{DATA_PROVIDER_NAME}' " \
                      "ORDER BY owner_priority ASC;"\
                    .format(PROVIDER_NAME=_provider_name, DATA_PROVIDER_NAME=_data_provider_name)
                rule_sets = self._app_connection.query_with_result(sql)
                print(rule_sets, type(rule_sets))
                for rule_set in rule_sets:
                    already_removed_duplication = False
                    _rule_set_name = rule_set['RULE_SET_NAME']
                    _owner = rule_set['OWNER']
                    sql = "SELECT silo_id, type " \
                          "FROM #TMP_ANL_RULE_ENGINE_STAGE_COMMON_1 " \
                          "WHERE RULE_SET_NAME = '{ruleSetName}' ".format(ruleSetName=_rule_set_name)
                    # _row = self._app_connection.query_with_result(sql)[0]
                    # SVR silo id, could be multi ones.different from parameter silo_id
                    # _silo_id = _row['SILO_ID']
                    # _silo_type = _row['TYPE']
                    # print("silo_id & silo_type & owner are: ", _silo_id, _silo_type, _owner)

                    sql = "SELECT rule_set_id," \
                          "  CASE WHEN ITEM_SCOPE is null THEN '' ELSE ITEM_SCOPE END ITEM_SCOPE," \
                          "  CASE WHEN STORE_SCOPE is null THEN '' ELSE STORE_SCOPE end STORE_SCOPE ,TYPES_LIST " \
                          "FROM ANL_RULE_ENGINE_RULE_SET " \
                          "WHERE RULE_SET_NAME = '{ruleSetName}' AND ENABLED in ('T','Y') " \
                          "AND vendor_key = {VENDOR_KEY} AND retailer_key = {RETAILER_KEY}; "\
                        .format(ruleSetName=_rule_set_name,
                                VENDOR_KEY=self._vendor_key,
                                RETAILER_KEY=self._retailer_key)
                    _row = self._app_connection.query_with_result(sql)[0]
                    _rule_set_id = _row['RULE_SET_ID']
                    _item_scope = _row['ITEM_SCOPE']
                    _store_scope = _row['STORE_SCOPE']
                    _type_list = _row['TYPES_LIST']
                    print(_rule_set_id, _item_scope, _store_scope, _type_list)
                    msg="Working on rule set id rule_set_id whose name is %s - in progress" % _rule_set_name
                    print(msg)
                    # Write-Log $sqlConn "rule engine" 99999 $msg $inProgress

                    sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ COUNT(*) " \
                          "FROM (SELECT * FROM ANL_RULE_ENGINE_STAGE_FACT LIMIT 1) x;"
                    _row_exists = self._dw_connection.query_scalar(sql)[0]
                    if _row_exists == 0:
                        msg = "There is no data to be processed for (Vendor: %s Retailer: %s) , " \
                              "Since table of ANL_RULE_ENGINE_STAGE_FACT is empty - INFO" % (self._vendor_key, self._retailer_key)
                        print(msg)
                        # Write-Log $sqlConn "rule engine" 99999 $msg 'INFO'

                    """
                    create a stage table for the rule set, every rule under this rule set will process based on
                    table of RSI_ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix}. If an alert will be processed by multiple rule set,
                    then the owner set by a previous rule set won't be overwritten by a later rule set. e.g if an alert is
                    set the owner as 'Ahold', then an SVR rule set will ignore that alert and won't overwrite it. If multiple
                    rule set for a SVR will process the same alert, the first rule set will has higher priority.
                    """
                    sql = "DROP TABLE IF EXISTS {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix};" \
                          "CREATE TABLE {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix} AS " \
                          "SELECT /*+label(GX_OSM_RULE_ENGINE)*/ a.* FROM ANL_RULE_ENGINE_STAGE_FACT a "\
                        .format(schemaName=self._schema_name, suffix=self._suffix)

                    # commented this out, since we don'e need table CONFIG_CUSTOMERS_FROM_HUB anymore
                    # if _silo_type != 'A':
                    #     sql += "INNER JOIN {schemaName}.CONFIG_CUSTOMERS_FROM_HUB b " \
                    #             "ON a.VENDOR_KEY = b.VENDOR_KEY AND a.RETAILER_KEY = b.RETAILER_KEY " \
                    #             "AND (b.SILO_ID = '{siloID}') ".format(schemaName=self._schema_name, siloID=_silo_id)

                    sql += "LEFT JOIN ANL_RULE_ENGINE_STAGE_FACT_TARGET c " \
                           "ON a.VENDOR_KEY = c.VENDOR_KEY and a.RETAILER_KEY = c.RETAILER_KEY " \
                           "AND a.{providerBaseTablePKColumn}=c.id " \
                           "WHERE 0=0 AND a.VENDOR_KEY = {VENDOR_KEY} and a.RETAILER_KEY = {RETAILER_KEY} "\
                        .format(providerBaseTablePKColumn=_provider_base_table_PK_column,
                                VENDOR_KEY=self._vendor_key,
                                RETAILER_KEY=self._retailer_key)
                    print(sql)

                    # there is no alert silo in OSA SUITE, So commented out below condition.
                    # For alert portal silo, we are only interesting the alert whose DSD_Ind  is 0
                    # if _silo_type == 'A' and _provider_name == 'AFM':
                    #     sql += "AND DSD_IND=0 "

                    if _item_scope != '':
                        sql += " AND UPC IN (SELECT VALUE FROM {schemaName}.ANL_RULE_ENGINE_STAGE_RULE_LIST " \
                                    "WHERE FILE_ID = '{itemScope}') ".format(schemaName=self._schema_name,
                                                                             itemScope=_item_scope)
                        sql_dummy = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ COUNT(*) " \
                                    "FROM (SELECT * FROM {schemaName}.ANL_RULE_ENGINE_STAGE_RULE_LIST " \
                                    "WHERE FILE_ID = '{itemScope}' LIMIT 1) x ".format(schemaName=self._schema_name,
                                                                                       itemScope=_item_scope)
                        _row_exists = self._dw_connection.query_scalar(sql_dummy)[0]
                        if _row_exists == 0:
                            msg = "Rule set of %s has item scope set, but the scope is empty - WARNING" % (_rule_set_name)
                            print(msg)
                            # Write-Log $sqlConn "rule engine" 99999 $msg $warning

                    if _store_scope != '':
                        sql += "AND STOREID IN (SELECT VALUE FROM {schemaName}.ANL_RULE_ENGINE_STAGE_RULE_LIST " \
                                "WHERE FILE_ID = '{storeScope}') ".format(schemaName=self._schema_name,
                                                                          storeScope=_store_scope)
                        sql_dummpy = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ COUNT(*) FROM " \
                                     "(SELECT * FROM {schemaName}.ANL_RULE_ENGINE_STAGE_RULE_LIST " \
                                     " WHERE FILE_ID = '{storeScope}' LIMIT 1) x".format(schemaName=self._schema_name,
                                                                                         storeScope=_store_scope)
                        _row_exists = self._dw_connection.query_scalar(sql_dummpy)[0]
                        if _row_exists == 0:
                            msg = "Rule set of %s has store scope set, but the scope is empty - WARNING" % _rule_set_name
                            print(msg)
                            # Write-Log $sqlConn "rule engine" 99999 $msg $warning
                    print(sql)

                    # print(type_list)
                    _intervention_key_list = self._get_intervention_keys.getting_intervention_key_list(_type_list)
                    print(_intervention_key_list)

                    if _provider_name == 'AFM' and _type_list != '':
                        sql += "AND interventionkey in ({interventionKeyList}) ".format(interventionKeyList=_intervention_key_list)

                    # NXG-18237: no need to re-process duplicated alert.
                    # (e.g. alert silo rules rejected those alerts as duplicated. no need to re-process in SVR silo rules)
                    sql += "AND ((c.owner IS NULL OR c.owner='') " \
                           "      AND NVL(reject_reasons,'') NOT LIKE '%duplicated alert%') UNSEGMENTED ALL NODES;"
                    print("SQL of creating table ANL_RULE_ENGINE_STAGE_FACT_RULE_SET%s is: \n %s" % (self._suffix, sql) )
                    self._dw_connection.execute(sql)

                    _stage_table = "{schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix}"\
                        .format(schemaName=self._schema_name, suffix=self._suffix)
                    sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ COUNT(*) FROM {stage_table} "\
                        .format(stage_table=_stage_table)
                    print("There are %d rows in table of %s" % (self._dw_connection.query_scalar(sql)[0], _stage_table))

                    sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ COUNT(*) cnt " \
                          "FROM (SELECT vendor_key, retailer_key, incidentid, count(*) " \
                          "      FROM {stage_table} group by vendor_key, retailer_key, incidentid " \
                          "      HAVING COUNT(*) > 1) x "\
                        .format(stage_table=_stage_table)
                    _duplicated_alert_cnt = self._dw_connection.query_scalar(sql)[0]
                    if _duplicated_alert_cnt > 0:
                        sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ vendor_key, retailer_key, " \
                              "       incidentid, COUNT(*) cnt " \
                              "FROM {stage_table} GROUP BY vendor_key, retailer_key, incidentid " \
                              "HAVING COUNT(*) > 1 LIMIT 10 ".format(stage_table=_stage_table)
                        result = self._dw_connection.query_with_result(sql)
                        msg = "Duplication Found:"
                        print("There are duplicated alerts, please check alert table, some examples are: %s" % result[0])
                        for row in result:
                            _vendor_key = row['VENDOR_KEY']
                            _retailer_key = row['RETAILER_KEY']
                            _incident_id = row['INCIDENTID']
                            _cnt = row['CNT']
                            _temp_msg = "vendor_key: {vendor_key}, retailer_key: {retailer_key}, " \
                                        "incidentid: {incidentid}, number of duplication: {cnt}`n"\
                                .format(vendor_key=_vendor_key, retailer_key=_retailer_key,
                                        incidentid=_incident_id, cnt=_cnt)
                            print(_temp_msg)
                            msg += _temp_msg
                        # Write-Log $sqlConn "rule engine" 99999 $msg $error
                        # throw $msg
                        print(msg)
                        raise Exception()

                    sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE) */ COUNT(*) " \
                          "FROM (SELECT * FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix} LIMIT 1) x"\
                        .format(schemaName=self._schema_name, suffix=self._suffix)
                    if self._dw_connection.query_scalar(sql)[0] == 0:
                        msg = "There is no data to be processed for (Vendor: {VENDOR_KEY}, Retailer: {RETAILER_KEY}), " \
                              "rule set of {ruleSetName} " \
                              "Since table of {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix} is empty - WARNING"\
                            .format(VENDOR_KEY=self._vendor_key,
                                    RETAILER_KEY = self._retailer_key,
                                    ruleSetName=_rule_set_name,
                                    schemaName=self._schema_name,
                                    suffix=self._suffix)
                        # Write-Log $sqlConn "rule engine" 99999 $msg $warning
                        print(msg)

                    sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET; " \
                          "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET " \
                          "ON COMMIT PRESERVE ROWS AS " \
                          "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ vendor_key, retailer_key, " \
                          "       {providerBaseTablePKColumn} id, CAST(NULL AS VARCHAR(4000)) AS reject_reasons," \
                          "       CAST(NULL AS VARCHAR(100)) AS OWNER " \
                          "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix}"\
                        .format(providerBaseTablePKColumn=_provider_base_table_PK_column,
                                schemaName=self._schema_name,
                                suffix=self._suffix)
                    self._dw_connection.execute(sql)
                    print("SQL of creating table ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET is:\n", sql)

                    sql = "SELECT DISTINCT processing_order " \
                          "FROM #TMP_ANL_RULE_ENGINE_STAGE_RULES " \
                          "WHERE rule_set_id ={ruleSetID} ORDER BY processing_order ASC"\
                        .format(ruleSetID=_rule_set_id,
                                suffix=self._suffix)
                    processing_orders = self._app_connection.query_with_result(sql)
                    # print(sql)
                    # print(processing_orders)
                    for processing_order_row in processing_orders:
                        _processing_order = processing_order_row['PROCESSING_ORDER']
                        print("Working on (modified) processing order of %s -- in progress" % _processing_order)
                        _sql_concat = ''
                        _sql_tmp = ''
                        _sql_tmp1 = ''
                        # Write-Log $sqlConn "rule engine" 99999 $msg $inProgress
                        sql="SELECT  FILTER_NAME, METRICS_TYPE, " \
                            "        METRICS_CONDITION, METRICS_REJECT_REASON, a.PARAMETER1, " \
                            "        CASE WHEN a.PARAMETER2 IS NULL THEN '' ELSE a.PARAMETER2 END AS PARAMETER2, " \
                            "        a.PARAMETER3,a.PROVIDER_SUB_TYPE, " \
                            "        CASE WHEN SUB_LEVEL_METRICS IS NULL THEN '' ELSE SUB_LEVEL_METRICS END AS SUB_LEVEL_METRICS, " \
                            "        a.RULE_ID, RULE_SET_ID, RULE_ACTION_TYPE, METRICS_ORDER  " \
                            "FROM #TMP_ANL_RULE_ENGINE_STAGE_RULES a " \
                            "INNER JOIN ANL_RULE_ENGINE_META_RULES b " \
                            "ON a.RULE_ID = b.RULE_ID AND rule_set_id = {ruleSetId} " \
                            "AND processing_order = {processingOrder}".format(ruleSetId=_rule_set_id,
                                                                              processingOrder=_processing_order)
                        rule_rows = self._app_connection.query_with_result(sql)
                        print(rule_rows, type(rule_rows))
                        _metrics_type = ''
                        for rule in rule_rows:
                            print("Working on rule: ", rule)
                            _filter_name = rule['FILTER_NAME']
                            _metrics_type = rule['METRICS_TYPE']
                            _metrics_order = rule['METRICS_ORDER']
                            if _metrics_order > 106 and (not already_removed_duplication):
                                self._remove_duplication.remove_duplication()
                                already_removed_duplication = True

                            # need to make sure metrics condition is inside ()
                            _metrics_condition = rule['METRICS_CONDITION']
                            _metrics_reject_reason = rule['METRICS_REJECT_REASON']
                            _parameter1 = rule['PARAMETER1']
                            _parameter2 = rule['PARAMETER2']
                            _parameter3 = rule['PARAMETER3']
                            _provider_sub_type = rule['PROVIDER_SUB_TYPE']
                            _sub_level_metrics = rule['SUB_LEVEL_METRICS']
                            _rule_id = rule['RULE_ID']
                            _rule_set_id = rule['RULE_SET_ID']
                            _rule_action_type = rule['RULE_ACTION_TYPE']
                            msg = "Working on filter whose name is '{filterName}' - in progress".format(filterName=_filter_name)
                            # Write-Log $sqlConn "rule engine" 99999 $msg $inProgress
                            print(msg)

                            print("_metrics_condition", _metrics_condition, '_metrics_reject_reason', _metrics_reject_reason, '_parameter1', _parameter1,
                                  '_parameter2 : ', _parameter2, "_parameter3 is :", _parameter3,
                                  "_provider_sub_type :", _provider_sub_type, "_sub_level_metrics: ", _sub_level_metrics, '_rule_id :', _rule_id, _rule_set_id)
                            _intervention_key_list = self._get_intervention_keys.getting_intervention_key_list(_provider_sub_type)
                            print(_intervention_key_list)

                            if _provider_sub_type == "*":
                                _sql_tmp1 = ""
                            else:
                                _sql_tmp1 = " InterventionKey IN ({interventionKeyList}) AND "\
                                    .format(interventionKeyList=_intervention_key_list)

                            print(_sql_tmp1)
                            # print(metricsCondition)
                            # print(metrics_type)
                            # print(ruleSetID)
                            # print(ruleID)

                            if _metrics_type.lower() == "Value Filter".lower():
                                if _sub_level_metrics == '':
                                    print("1 _sql_tmp is : ", _metrics_condition)
                                    _sql_tmp = _metrics_condition.lower().replace('parameter1',str(_parameter1))
                                    _sql_tmp = _sql_tmp.replace('parameter2', str(_parameter2))
                                    _sql_tmp = _sql_tmp.replace('parameter3',str(_parameter3))
                                    print("1 _sql_tmp is : ", _sql_tmp)
                                    _sql_tmp = " WHEN {AlertType_place_holder}".format(AlertType_place_holder=_sql_tmp1) + _sql_tmp + \
                                               " THEN '{metricsRejectReason}' ELSE '1'".format(metricsRejectReason=_metrics_reject_reason)
                                    # _sql_tmp = _sql_tmp.replace("AlertType_place_holder", _sql_tmp1)
                                    _sql_tmp = ' CASE ' + _sql_tmp + ' END "RULE:{providerSubType} {filterName}" '\
                                        .format(providerSubType=_provider_sub_type, filterName=_filter_name)

                                    print("2 sqltmp is : ", _sql_tmp)
                                else:
                                    if _rule_id == 3 and _parameter2 != '':
                                        _sql_tmp1 = _sql_tmp1 + _metrics_condition.lower().replace("parameter2", str(_parameter2))
                                    else:
                                        _sql_tmp1 = _sql_tmp1 + _metrics_condition
                                    print("1 _sql_tmp is : ", _sql_tmp1, '_metrics_reject_reason :', _metrics_reject_reason, "_filter_name : ", _filter_name)
                                    _sql_tmp = self._get_sublevel_filter.get_sql(_provider_sub_type, _rule_id, _rule_set_id,
                                                                                 _sub_level_metrics, _sql_tmp1, _metrics_reject_reason, _filter_name)

                                print(_sql_tmp)
                                print("3 final sqltmp is : ", _sql_tmp)

                            else:
                                self._run_special_rule.special_rule_processing(_rule_set_id, _rule_id, _provider_sub_type, _intervention_key_list)

                            msg = "Working on filter whose name is {filterName} - completed".format(filterName=_filter_name)
                            # Write-Log $sqlConn "rule engine" 99999 $msg $completed
                            _sql_concat = _sql_concat + _sql_tmp + ","
                            print("_sql_concat is: ", _sql_concat)

                        if _metrics_type.lower() == "Value Filter".lower():
                            _sql_concat = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_PROCESSING_ORDER; " \
                                          "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_PROCESSING_ORDER ON COMMIT PRESERVE ROWS AS " \
                                          "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ vendor_key,retailer_key," \
                                          " incidentid as id, {sub_sqlconcat} " \
                                          "FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix} "\
                                .format(sub_sqlconcat=_sql_concat[:-1],
                                        schemaName=self._schema_name,
                                        suffix=self._suffix)
                            print(_sql_concat)
                            self._dw_connection.execute(_sql_concat)

                            sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ column_name FROM columns " \
                                  "WHERE table_name='ANL_RULE_ENGINE_STAGE_FACT_TARGET_PROCESSING_ORDER' " \
                                  "AND table_schema='v_temp_schema' AND column_name like 'RULE:%';"
                            _column_set = self._dw_connection.query_with_result(sql)
                            print(_column_set, type(_column_set))

                            _tmp_array = []
                            for column in _column_set:
                                _column_name =column['COLUMN_NAME']
                                _tmp_array.append(" CASE WHEN \"{columnName}\" = '1' THEN '' ELSE \"{columnName}\"||',' END".format(columnName=_column_name))

                            _sql_reject_reason = "||".join(_tmp_array)
                            print("sql reject reason is:", _sql_reject_reason)

                            sql = "DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP; " \
                                  "CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP " \
                                  "ON COMMIT PRESERVE ROWS AS " \
                                  "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ vendor_key,retailer_key,id," \
                                  "SUBSTRING(temp_reject_reasons,0,length(temp_reject_reasons)) as reject_reasons " \
                                  "FROM (SELECT vendor_key,retailer_key,id,{sqlRejectReason} AS temp_reject_reasons " \
                                  "         FROM ANL_RULE_ENGINE_STAGE_FACT_TARGET_PROCESSING_ORDER )x ".format(sqlRejectReason=_sql_reject_reason)
                            print(sql)
                            self._dw_connection.execute(sql)

                            self._UpdateTargetTable.update_target_table('')

                        # end of processing order
                        msg = "Working on (modified) processing order of {processingOrder} - completed"\
                            .format(processingOrder=_processing_order)
                        # Write-Log $sqlConn "rule engine" 99999 $msg $completed
                        print(msg)
                    print("already_removed_duplication is :", already_removed_duplication)
                    if not already_removed_duplication:
                        self._remove_duplication.remove_duplication()
                        already_removed_duplication = True
                    print(already_removed_duplication)
                    # end of rule set
                    # $ruleSetName

                    print("rule set name is:", _rule_set_name)
                    sql = "UPDATE /*+ DIRECT, label(GX_OSM_RULE_ENGINE)*/ ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET " \
                          "SET reject_reasons='{ruleSetName}('||reject_reasons||')' " \
                          "WHERE reject_reasons is not null and reject_reasons <> ''".format(ruleSetName=_rule_set_name)
                    self._dw_connection.execute(sql)

                    # use 'N' instead of using "CASE WHEN '{siloType}'='A' THEN 'Y' ELSE 'N' END". cuz there is no alert silo here.
                    sql = """INSERT /*+ DIRECT, label(GX_OSM_RULE_ENGINE)*/ INTO ANL_RULE_ENGINE_STAGE_FACT_TARGET
                       (vendor_key,retailer_key,id,reject_reasons,owner,is_alert_portal_rule_set)
                        SELECT vendor_key,retailer_key,id,reject_reasons,
                               CASE WHEN reject_reasons IS NULL OR reject_reasons = ''
                               OR reject_reasons LIKE '%clear reject reason caused by pick up rule)' THEN '{owner}' ELSE NULL END,

                               'N'
                         FROM ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET""".format(owner=_owner)
                    print(sql)
                    self._dw_connection.execute(sql)

                    sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ COUNT(*) FROM ANL_RULE_ENGINE_STAGE_FACT_TARGET"
                    print("There are %s rows in table of ANL_RULE_ENGINE_STAGE_FACT_TARGET" % self._dw_connection.query_scalar(sql)[0])

                    msg = "Working on rule set id %s whose name is %s: completed" % (_rule_set_id, _rule_set_name)
                    print(msg)
                    # Write-Log $sqlConn "rule engine" 99999 $msg $completed

                """
                double check: the same alertID shouldn't appear multiple times in the target table.
                One possible reason is that 2 SVR rule set cover the same alert.
                """
                sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ COUNT(*) " \
                      "FROM (SELECT id,vendor_key from ANL_RULE_ENGINE_STAGE_FACT_TARGET " \
                      "      WHERE is_alert_portal_rule_set<>'Y' GROUP BY id,vendor_key HAVING COUNT(*)>1) x"
                _row_count = self._dw_connection.query_scalar(sql)[0]
                if _row_count != 0:
                    msg = "Internal Error:duplication in ANL_RULE_ENGINE_STAGE_FACT_TARGET " \
                          "(We should not have any alerts with multiple owners). " \
                          "Please contact engineering team!!!"
                    print(msg)
                    # Write-Error $msg
                    # Write-Log $sqlConn "rule engine" 99999 $msg $error
                    exit(1)

                # self._dw_connection.execute("drop table if exists PEPSI_AHOLD_MB.ANL_RULE_ENGINE_STAGE_FACT_TARGET11")
                # self._dw_connection.execute("create table PEPSI_AHOLD_MB.ANL_RULE_ENGINE_STAGE_FACT_TARGET11 as select * from ANL_RULE_ENGINE_STAGE_FACT_TARGET")

                # double check: we should not have one alert with multiple owner set
                sql = "SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ COUNT(*) FROM " \
                      "(SELECT COUNT(*) FROM ANL_RULE_ENGINE_STAGE_FACT_TARGET " \
                      "  WHERE (owner is not null or owner='') GROUP BY id,vendor_key HAVING COUNT(*)>1)x"
                row_count = self._dw_connection.query_scalar(sql)[0]
                if row_count != 0:
                    msg = "Internal Error:duplication in ANL_RULE_ENGINE_STAGE_FACT_TARGET " \
                          "(We should not have any alerts with multiple owners). " \
                          "Please contact engineering team!!!"
                    print(msg)
                    # Write-Error $msg
                    # Write-Log $sqlConn "rule engine" 99999 $msg $error
                    exit(1)

                sql = """DROP TABLE IF EXISTS ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL;
                   CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL ON COMMIT PRESERVE ROWS AS
                   SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ x.*,y.owner
                   FROM ( SELECT id,vendor_key,retailer_key, case when reject_reasons ='' THEN NULL ELSE reject_reasons END AS reject_reasons
                          FROM (select id,vendor_key,retailer_key,
                          GROUP_CONCAT(reject_reasons) OVER (PARTITION BY id,vendor_key,retailer_key ORDER BY reject_reasons asc) AS reject_reasons
                        FROM ANL_RULE_ENGINE_STAGE_FACT_TARGET) temp
                   ) x
                   INNER JOIN
                   (SELECT id,vendor_key,retailer_key, GROUP_CONCAT(owner) OVER (PARTITION BY id,vendor_key,retailer_key) AS owner
                   FROM ANL_RULE_ENGINE_STAGE_FACT_TARGET) y
                   ON x.id=y.id AND x.vendor_key=y.vendor_key AND x.retailer_key =y.retailer_key"""
                self._dw_connection.execute(sql)

                # reject duplicated item-store.
                self._remove_duplication.remove_duplication_final()

                if _data_provider_post_processing_function != '':
                    msg = "Working on data provider post process, function name is: %s - in progress" % _data_provider_post_processing_function
                    print(msg)
                    # Write-Log $sqlConn "rule engine" 99999 $msg $inProgress
                    self._update_alert_table.update_alert_table()
                    # Write-Log $sqlConn "rule engine" 99999 $msg $completed

                msg = "Rule engine processing is done"
                print(msg)
                # Write-Log $sqlConn "rule engine" 99999 $msg $completed

        except Exception as e:
            raise
        finally:
            self._dw_connection.close_conn()
            self._app_connection.close_conn()


if __name__ == '__main__':
    # run_rule = RunRuleEngine('ben_osa_retailer','ben_osa_retailer','ben_osa_retailer', 'ENGV2HSDBQA1.ENG.RSICORP.LOCAL\DB5','ALERT')
    # __init__(self, silo_id, schema_name, silo_db_name, silo_server_name, silo_type, suffix):
    run_rule = RunRuleEngine('dw_conn', 'app_conn', 'suffix')
    run_rule.rule_process()

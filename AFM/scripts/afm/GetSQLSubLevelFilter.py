
class GetSQLSubLevelFilter(object):

    def __init__(self, conn, app_conn, context):
        self._dw_connection = conn
        self._app_connection = app_conn
        self._context = context
        self._schema_name = context["SCHEMA_NAME"]
        self._suffix = "_" + context["SUFFIX"]

    def get_sql(self, provider_subtype, rule_id, rule_set_id, sub_level_column, condition, reject_reason, filter_name):
        """
        Return final SQL according to "rule id" and "rule set id" and its condition as following.
        :param provider_subtype:
        :param rule_id:
        :param rule_set_id:
        :param sub_level_column:
        :param condition:
        :param reject_reason:
        :param filter_name:
        :return:
        """
        sql = self.__process(provider_subtype, rule_id, rule_set_id, sub_level_column, condition, reject_reason, filter_name)
        return sql

    def __process(self, provider_subtype, rule_id, rule_set_id, sub_level_column, condition, reject_reason, filter_name):
        print("\n-------------- Calling GetSQLSubLevelFilter class --------------")
        # return SQL for sub level filter

        _sqlToReturn = ""
        sql = "SELECT /*+label(GX_OSM_RULE_ENGINE)*/ * FROM {schemaName}.ANL_RULE_ENGINE_SUB_LEVEL_FILTER " \
              "WHERE rule_id={ruleId} and rule_set_id={ruleSetID} AND SUB_LEVEL_CATEGORY <> 'Alert Type' "\
            .format(schemaName=self._schema_name, ruleId=rule_id, ruleSetID=rule_set_id)
        print(sql)
        _result_sets = self._dw_connection.query_with_result(sql)
        print(_result_sets, type(_result_sets))
        loop_times = 0
        for row in _result_sets:
            print("SQLSubLevel condition is:", condition)
            _new_condition = condition.lower().replace("parameter1", str(row['METRICS_VALUE']))
            _new_condition = _new_condition.replace("parameter2", str(row['PARAMETER2']))
            _new_condition = _new_condition.replace("parameter3", str(row['PARAMETER3']))
            _sub_level_value = row["SUB_LEVEL_VALUE"].replace("'", "''")
            _sqlToReturn += " WHEN {subLevelColumn} = '{subLevelValue}' THEN " \
                            "CASE WHEN {newCondition} THEN '{rejectReason}' ELSE '1' END"\
                .format(subLevelColumn=sub_level_column,
                        newCondition=_new_condition,
                        subLevelValue=_sub_level_value,
                        rejectReason=reject_reason)
            loop_times += 1
        print("Returned SQL is: ", _sqlToReturn)

        sql = "SELECT /*+label(GX_OSM_RULE_ENGINE)*/ * FROM #TMP_ANL_RULE_ENGINE_STAGE_RULES " \
              "WHERE rule_id={ruleId} AND rule_set_id={ruleSetID}"\
            .format(ruleId=rule_id, ruleSetID=rule_set_id, suffix=self._suffix)
        print(sql)
        row = self._app_connection.query_with_result(sql)[0]
        # print(row, type(row))
        print("before :", condition)
        _new_condition = condition.lower().replace("parameter1", str(row['PARAMETER1']))
        _new_condition = _new_condition.replace("parameter2", str(row['PARAMETER2']))
        _new_condition = _new_condition.replace("parameter3", str(row['PARAMETER3']))
        print("after :", _new_condition)
        print("before _sqlToReturn is:", _sqlToReturn)
        if loop_times == 0:
            _sqlToReturn += " CASE WHEN {newCondition} THEN '{rejectReason}' ELSE '1' " \
                            "END \"rule:{providerSubType} {filterName}\" ".format(newCondition=_new_condition,
                                                                                  providerSubType=provider_subtype,
                                                                                  filterName=filter_name, rejectReason=reject_reason)
        else:
            _sqlToReturn += " ELSE CASE WHEN {newCondition} THEN '{rejectReason}' ELSE '1' END "\
                .format(newCondition=_new_condition, rejectReason=reject_reason)
            _sqlToReturn = ' CASE {sqlToReturn} END "rule:{providerSubType} {filterName}" '\
                .format(sqlToReturn=_sqlToReturn, providerSubType=provider_subtype, filterName=filter_name)

        print("after _sqlToReturn is:", _sqlToReturn)

        print("-------------- Calling GetSQLSubLevelFilter class end --------------\n")

        return _sqlToReturn

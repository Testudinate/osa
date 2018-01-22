
class RunFeedback(object):
# param($verticaConn,$sqlConn,$hubConn,$schemaName,$hubID,$inaccurateAlertsDys,$notInPlanogramDays,$trueAlertDays,$ruleSetId,$ruleID,$siloID='')
# param($verticaConn,$sqlConn,$hubConn,$schemaName,$hubID,$inaccurateAlertsDys,$notInPlanogramDays,$trueAlertDays,$ruleSetId,$ruleID,$siloID)
    def __init__(self, conn, app_conn, context, inaccurate_alerts_dys, not_in_planogram_days, true_alert_days, rule_set_id, rule_id):
        self.dw_connection = conn
        self.app_connection = app_conn
        self._context = context
        self._schema_name = context["SCHEMA_NAME"]
        self._suffix = "_" + context["SUFFIX"]
        self._vendor_key = context["VENDOR_KEY"]
        self._inaccurate_alerts_dys = inaccurate_alerts_dys
        self._not_in_planogram_days = not_in_planogram_days
        self._true_alert_days = true_alert_days
        self._rule_set_id = rule_set_id
        self._rule_id = rule_id
        # self._silo_id = silo_id

    def processing_feedback(self):
        """
        Applying feedback rule against all alerts.
        There are 3 types of Alerts: inaccurated Alerts, not in planogram, true alerts.
        Users are able to set the suppression days from UI for above 3 types of alerts.
        AFM will not issue those alerts when they were issued within given days from UI.

        This will also check if "Alert Type" box is enabled or not from UI.
        :return: no return
        """
        self.__process()

    def __process(self):
        print("\n-------------- Calling RunFeedback Class --------------")
        self.dw_connection.execute("DROP TABLE IF EXISTS {schemaName}.ANL_RULE_ENGINE_AFM_FEEDBACK_STAGE{suffix};"
                                   "DROP TABLE IF EXISTS {schemaName}.ANL_RULE_ENGINE_STAGE_SUB_LEVEL_FILTER_1{suffix}"
                                   .format(schemaName=self._schema_name, suffix=self._suffix))

        sql="CREATE TABLE {schemaName}.ANL_RULE_ENGINE_STAGE_SUB_LEVEL_FILTER_1{suffix} " \
            "( SUB_LEVEL_VALUE VARCHAR(512)," \
            "  alertType VARCHAR(512)," \
            "  numdaystosuppress VARCHAR(10)" \
            "); ".format(schemaName=self._schema_name, suffix=self._suffix)
        self.dw_connection.execute(sql)

        sql="INSERT /*+ DIRECT, label(GX_OSM_RULE_ENGINE)*/ INTO {schemaName}.ANL_RULE_ENGINE_STAGE_SUB_LEVEL_FILTER_1{suffix} "\
            "SELECT SUB_LEVEL_VALUE, 'NotTrueAlert' alertType, METRICS_VALUE numdaystosuppress "\
            "FROM {schemaName}.ANL_RULE_ENGINE_SUB_LEVEL_FILTER "\
            "WHERE rule_set_Id = {ruleSetId} AND rule_ID = {ruleID} and SUB_LEVEL_CATEGORY = 'Alert Type' "\
            "UNION ALL "\
            "SELECT  SUB_LEVEL_VALUE, 'NotOnPlanogram', PARAMETER2 "\
            "FROM {schemaName}.ANL_RULE_ENGINE_SUB_LEVEL_FILTER "\
            "WHERE rule_set_Id = {ruleSetId} AND rule_ID = {ruleID} and SUB_LEVEL_CATEGORY = 'Alert Type' "\
            "UNION ALL "\
            "SELECT SUB_LEVEL_VALUE, 'TrueAlert', PARAMETER3 "\
            "FROM {schemaName}.ANL_RULE_ENGINE_SUB_LEVEL_FILTER "\
            "WHERE rule_set_Id = {ruleSetId} AND rule_ID = {ruleID} and SUB_LEVEL_CATEGORY = 'Alert Type' "\
            "UNION ALL "\
            "SELECT 'default', 'NotOnPlanogram', '{notInPlanogramDays}' "\
            "UNION ALL "\
            "SELECT 'default', 'NotTrueAlert', '{inaccurateAlertsDys}' "\
            "UNION ALL "\
            "SELECT 'default', 'TrueAlert', '{trueAlertDays}';".format(schemaName=self._schema_name,
                                                                       ruleSetId=self._rule_set_id,
                                                                       ruleID=self._rule_id,
                                                                       notInPlanogramDays=self._not_in_planogram_days,
                                                                       inaccurateAlertsDys=self._inaccurate_alerts_dys,
                                                                       trueAlertDays=self._true_alert_days,
                                                                       suffix=self._suffix)
        self.dw_connection.execute(sql)

        sql="SELECT COUNT(*) FROM ANL_RULE_ENGINE_SUB_LEVEL_FILTER " \
            "WHERE rule_set_Id = {ruleSetId} AND rule_ID = {ruleID} and SUB_LEVEL_VALUE IS NOT NULL " \
            "AND SUB_LEVEL_CATEGORY = 'Alert Type';".format(schemaName=self._schema_name,
                                                            ruleSetId=self._rule_set_id,
                                                            ruleID=self._rule_id)
        _alert_type_enabled = self.app_connection.query_scalar(sql)[0]
        if _alert_type_enabled != 0:
            sqlForFeedback = \
                "SELECT fdbk.Store_Id, fdbk.vendor_key, fdbk.Inner_UPC, fdbk.STORE_VISIT_DATE, " \
                "       fdbk.FEEDBACK_DESCRIPTION, COALESCE(sublevel.SUB_LEVEL_VALUE,'default') AS OSM_MAJOR_CATEGORY, " \
                "       fdbk.Merchandiser, fdbk.Retailer_Key, " \
                "       ROW_NUMBER() OVER(PARTITION BY dim.AlertSubType, fdbk.Store_Id,fdbk.Inner_UPC ORDER BY STORE_VISIT_DATE desc) rn " \
                "FROM {schemaName}.ANL_FACT_FEEDBACK fdbk " \
                "INNER JOIN {schemaName}.ANL_FACT_OSM_INCIDENTS alert " \
                "ON fdbk.ALERT_ID = alert.IncidentID and fdbk.RETAILER_KEY = alert.RETAILER_KEY " \
                "AND fdbk.VENDOR_KEY = alert.VENDOR_KEY " \
                "INNER JOIN {schemaName}.ANL_DIM_OSM_INTERVENTIONCLASSIFICATION dim " \
                "ON alert.InterventionKey = dim.InterventionKey " \
                "LEFT JOIN {schemaName}.ANL_RULE_ENGINE_SUB_LEVEL_FILTER sublevel " \
                "ON dim.AlertSubType = sublevel.SUB_LEVEL_VALUE and rule_set_id = {ruleSetId} AND rule_id = {ruleID} " \
                "AND SUB_LEVEL_CATEGORY = 'Alert Type' " \
                "WHERE alert.VENDOR_KEY = {VENDOR_KEY}".format(schemaName=self._schema_name,
                                                               ruleSetId=self._rule_set_id,
                                                               ruleID=self._rule_id,
                                                               VENDOR_KEY=self._vendor_key)
        else:
            sqlForFeedback = "SELECT fdbk.Store_Id, fdbk.vendor_key, fdbk.Inner_UPC, fdbk.STORE_VISIT_DATE, fdbk.FEEDBACK_DESCRIPTION, " \
                             "       'default' as OSM_MAJOR_CATEGORY, fdbk.Merchandiser,fdbk.Retailer_Key, " \
                             "       ROW_NUMBER() OVER(PARTITION BY fdbk.Store_Id,fdbk.Inner_UPC ORDER BY fdbk.STORE_VISIT_DATE desc) rn " \
                             "FROM {schemaName}.ANL_FACT_FEEDBACK fdbk " \
                             "WHERE fdbk.source != 'ARIA' or fdbk.source IS NULL".format(schemaName=self._schema_name)

        print(sqlForFeedback)

        sql = "SELECT ANALYZE_STATISTICS('{schemaName}.ANL_RULE_ENGINE_STAGE_SUB_LEVEL_FILTER_1{suffix}'); "\
              "SELECT ANALYZE_STATISTICS('{schemaName}.ANL_DIM_FEEDBACK_ASSUMPTIONS'); "\
              "SELECT ANALYZE_STATISTICS('{schemaName}.ANL_FACT_FEEDBACK');".format(schemaName=self._schema_name, suffix=self._suffix)
        self.dw_connection.execute(sql)

        sql="CREATE TABLE {schemaName}.ANL_RULE_ENGINE_AFM_FEEDBACK_STAGE{suffix} AS " \
            "SELECT /*+label(GX_OSM_RULE_ENGINE)*/ t.StoreUPC StoreUPC, Retailer_Key RetailerKey, Vendor_Key VendorKey "\
            "FROM (SELECT DISTINCT cast(fdbk.Store_Id as varchar(20))|| '-'||cast( fdbk.Inner_UPC as varchar(30)) || '-' || fdbk.OSM_MAJOR_CATEGORY AS StoreUPC, "\
            "        	fdbk.OSM_MAJOR_CATEGORY, fdbk.store_visit_date, fdbk.Feedback_Description, "\
            "        	fdbk.Merchandiser,fdbk.Retailer_Key,fdbk.Vendor_Key "\
            "	       FROM (sqlForFeedback_placeholder) fdbk where rn = 1 ) t "\
            "INNER JOIN {schemaName}.ANL_RULE_ENGINE_STAGE_SUB_LEVEL_FILTER_1{suffix} sub_level " \
            "ON sub_level.SUB_LEVEL_VALUE = t.OSM_MAJOR_CATEGORY "\
            "INNER JOIN (SELECT FeedbackDesc, CASE WHEN NotOnPlanogram = 'Y' THEN 'NotOnPlanogram' "\
            "                                	   WHEN TrueAlert <> 'Y' THEN 'NotTrueAlert' "\
            "                                	   WHEN TrueAlert = 'Y' THEN 'TrueAlert' "\
            "                                  END AS alertType, Merchandiser "\
            "        	    FROM {schemaName}.ANL_DIM_FEEDBACK_ASSUMPTIONS "\
            "           ) assumption " \
            "ON t.Feedback_Description = assumption.FeedbackDesc AND assumption.Merchandiser = t.Merchandiser " \
            "AND assumption.alertType = sub_level.alertType "\
            "WHERE DATEDIFF(day, STORE_VISIT_DATE, GETDATE()) <= numdaystosuppress "\
            .format(schemaName=self._schema_name, suffix=self._suffix)
        sql = sql.replace("sqlForFeedback_placeholder", sqlForFeedback)
        print(sql)
        self.dw_connection.execute(sql)
        print("-------------- Calling RunFeedback class ended --------------\n")

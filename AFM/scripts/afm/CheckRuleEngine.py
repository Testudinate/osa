

class CheckRuleEngine(object):

    def __init__(self, conn, context):
        self._dw_connection = conn
        self._context = context
        self._vendor_key = context["VENDOR_KEY"]
        self._retailer_key = context["RETAILER_KEY"]
        self._schema_name = context["SCHEMA_NAME"]
        self._suffix = "_" + context["SUFFIX"]

    def check_rule_engine(self):
        """
        Preparing rule engine tables ANL_RULE_ENGINE_STAGE_RULES{suffix} for control processing.
        :return:

        There is no need to check rule engine any longer from PowerShell script.
        Added 3 more columns(Param1, Param2, Param3) in table ANL_RULE_ENGINE_META_RULES to setup the dependency
        UI can check this table when users are saving configuration.
        """
        self.__process()

    def __process(self):
        print("\n-------------- Calling CheckRuleEngine class --------------")
        sql="DROP TABLE IF EXISTS {schemaName}.ANL_RULE_ENGINE_STAGE_RULES{suffix};" \
            "CREATE TABLE {schemaName}.ANL_RULE_ENGINE_STAGE_RULES{suffix} as " \
            "SELECT /*+label(GX_OSM_RULE_ENGINE)*/ a.*,b.PROVIDER_SUB_TYPE," \
            "       b.METRICS_ORDER AS PROCESSING_ORDER, b.DEPEND_ON_PREVIOUS_RULE "\
            "FROM {schemaName}.ANL_RULE_ENGINE_RULES a  "\
            "INNER JOIN {schemaName}.ANL_RULE_ENGINE_META_RULES b " \
            "ON a.RULE_ID=b.RULE_ID AND a.enabled in ('Y','T') "\
            "INNER JOIN {schemaName}.ANL_RULE_ENGINE_RULE_SET c " \
            "ON a.RULE_SET_ID=c.RULE_SET_ID AND c.ENABLED in ('Y','T') " \
            "WHERE c.vendor_key = {VENDOR_KEY} AND c.retailer_key = {RETAILER_KEY};"\
            .format(schemaName=self._schema_name,
                    suffix=self._suffix,
                    VENDOR_KEY=self._vendor_key,
                    RETAILER_KEY=self._retailer_key)
        self._dw_connection.execute(sql)
        print(sql)


        print("-------------- Calling CheckRuleEngine class end --------------\n")



class CheckRuleEngine(object):

    def __init__(self, conn, context):
        self._dw_connection = conn
        self._context = context
        self._schema_name = context["SCHEMA_NAME"]
        self._suffix = "_" + context["SUFFIX"]

    def check_rule_engine(self):
        self._process()

    def _process(self):
        print("\n-------------- Calling CheckRuleEngine class --------------")
        sql="DROP TABLE IF EXISTS {schemaName}.ANL_RULE_ENGINE_STAGE_RULES{suffix};" \
            "CREATE TABLE {schemaName}.ANL_RULE_ENGINE_STAGE_RULES{suffix} as " \
            "SELECT /*+label(GX_OSM_RULE_ENGINE)*/ a.*,b.PROVIDER_SUB_TYPE,b.METRICS_ORDER AS PROCESSING_ORDER, b.DEPEND_ON_PREVIOUS_RULE "\
            "FROM {schemaName}.ANL_RULE_ENGINE_RULES a  "\
            "INNER JOIN {schemaName}.ANL_RULE_ENGINE_META_RULES b " \
            "ON a.RULE_ID=b.RULE_ID AND a.enabled in ('Y','T') "\
            "INNER JOIN {schemaName}.ANL_RULE_ENGINE_RULE_SET c " \
            "ON a.RULE_SET_ID=c.RULE_SET_ID AND c.ENABLED in ('Y','T')"\
            .format(schemaName=self._schema_name,
                    suffix=self._suffix)
        self._dw_connection.execute(sql)
        print(sql)
        """
        no need to check rule engine any longer.
        this is being handled in a new table from UI.
        """

        print("-------------- Calling CheckRuleEngine class end --------------\n")

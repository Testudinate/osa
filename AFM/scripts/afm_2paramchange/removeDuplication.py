# removeduplication $verticaConn $schemaName


class RemoveDuplication(object):

    def __init__(self, conn, context):
        self._dw_connection = conn
        self._context = context
        self._schema_name = context["SCHEMA_NAME"]
        self._suffix = "_" + context["SUFFIX"]

    def remove_duplication(self):
        print("\n-------------- Calling removeduplication.remove_duplication --------------")

        print("Run the code to remove duplicated alert whose item_group/store are the same")
        sql = """DROP TABLE IF EXISTS {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_DUPLICATION;
            CREATE TABLE {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_DUPLICATION as
            SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ vendor_key, retailer_key, incidentid as id
            FROM (SELECT a.vendor_key, a.retailer_key, a.incidentid,
                        ROW_NUMBER() OVER (PARTITION BY a.itemnumber, a.store_key ORDER BY a.Priority asc, rank_value_after_calculation desc) idx
                 FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_RULE_SET{suffix} a
                 INNER JOIN ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET b
                 ON a.vendor_key = b.vendor_key
                 AND a.retailer_key=b.retailer_key
                 AND a.incidentid =b.id
                 AND (b.reject_reasons IS NULL
                    OR  b.reject_reasons='')
            ) x WHERE idx > 1""".format(schemaName=self._schema_name, suffix=self._suffix)
        self._dw_connection.execute(sql)

        sql = """UPDATE /*+ DIRECT, label(GX_OSM_RULE_ENGINE)*/ ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET a
            SET reject_reasons = CASE WHEN reject_reasons IS NULL THEN 'duplicated alert'
                                    ELSE reject_reasons||',duplicated alert' END
            FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_DUPLICATION b
            WHERE a.retailer_key = b.retailer_key
            AND a.vendor_key = b.vendor_key AND a.id = b.id""".format(schemaName=self._schema_name)
        self._dw_connection.execute(sql)

        print("-------------- Calling removeduplication.remove_duplication end --------------\n")

    '''
    There could be duplicated item-store after introduced New Alert Type.##
    Adding following new function to handle duplicated item-store in the final table. ##
    case is: there are multi item-stores with different alert type. ##
    Alert silo will pick one of them and rejected rest, And SVR rule will also picked up one. ##
    then we will have 2 rows with same item-store, finally, we need mark one as rejected in final table.##
    "b.OWNER IS not NULL" means being issued. only handle issued rows and pick up only one.
    "b.reject_reasons nulls first" means rows with no reject_reasons rank top.
    '''
    def remove_duplication_final(self):
        print("\n-------------- Calling removeduplication.remove_duplication_final --------------")

        print("Run the code to remove duplicated alert whose item_group/store are the same in the final table")
        sql = """DROP TABLE IF EXISTS {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_DUPLICATION;
            CREATE TABLE {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_DUPLICATION AS
            SELECT /*+ label(GX_OSM_RULE_ENGINE)*/ vendor_key, retailer_key, incidentid as id
            FROM (  SELECT a.vendor_key, a.retailer_key, a.incidentid ,
                        ROW_NUMBER() OVER (PARTITION BY a.itemnumber, a.store_key
                           ORDER BY b.reject_reasons nulls first, a.Priority asc, rank_value_after_calculation DESC) idx
                    FROM ANL_RULE_ENGINE_STAGE_FACT a
                    INNER JOIN ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL b
                    ON a.vendor_key = b.vendor_key
                    AND a.retailer_key=b.retailer_key
                    AND a.incidentid =b.id
                    AND (b.OWNER <> ' ' or NVL(b.OWNER,'') <> '')
            ) x WHERE idx > 1""".format(schemaName=self._schema_name)
        self._dw_connection.execute(sql)

        sql = """UPDATE /*+ DIRECT, label(GX_OSM_RULE_ENGINE)*/ ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL a
            SET reject_reasons = CASE WHEN reject_reasons IS NULL THEN 'duplicated alert final'
                                    ELSE reject_reasons||',duplicated alert final' END,
                owner = null
            FROM {schemaName}.ANL_RULE_ENGINE_STAGE_FACT_DUPLICATION b
            WHERE a.retailer_key= b.retailer_key
            AND a.vendor_key = b.vendor_key AND a.id = b.id""".format(schemaName=self._schema_name)
        print(sql)
        print("\n-------------- Calling removeduplication.remove_duplication_final end--------------")

        self._dw_connection.execute(sql)


if __name__ == '__main__':
    rd = RemoveDuplication('test','test')
    rd.remove_duplication()
    rd.remove_duplication_final()

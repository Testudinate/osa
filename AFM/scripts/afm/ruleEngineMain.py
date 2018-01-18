from RunRuleEngine import RunRuleEngine
from common.DBOperation import DWAccess
from common.DBOperation import APPAccess


class RuleEngineMain(object):
    def __init__(self, vendor_key, retailer_key, silo_type):
        self._vendor_key = vendor_key
        self._retailer_key = retailer_key
        self._dw_connection = DWAccess()
        self._app_connection = APPAccess()
        # self._silo_db_name = silo_db_name
        # self._silo_server_name = silo_server_name
        self._silo_type = silo_type
        self._context = self._getting_context()
        self._run_rule_engine = RunRuleEngine(self._dw_connection, self._context)

    def _getting_retailer_name(self):
        sql = "SELECT retailer_sname FROM rsi_dim_retailer WHERE retailer_key = {RETAILER_KEY}"\
            .format(RETAILER_KEY=self._retailer_key)
        _retailer_sname = self._app_connection.query_scalar(sql)[0]
        return _retailer_sname

    def _getting_vendor_name(self):
        sql = "SELECT vendor_sname FROM rsi_dim_vendor WHERE vendor_key = {VENDOR_KEY}"\
            .format(VENDOR_KEY=self._vendor_key)
        _vendor_sname = self._app_connection.query_scalar(sql)[0]
        return _vendor_sname

    def _check_silo_type(self):
        if self._silo_type == 'WMSSC' or self._silo_type == 'SASSC':
            _silo_type = 'WM'
        else:
            _silo_type = 'SVR'
        return _silo_type

    def _getting_context(self):
        _context = dict(SCHEMA_NAME="OSA_" + self._getting_retailer_name() + "_BEN",
                        VENDOR_KEY=self._vendor_key,
                        RETAILER_KEY=self._retailer_key,
                        VENDOR_NAME=self._getting_vendor_name(),
                        RETAILER_NAME=self._getting_retailer_name(),
                        SUFFIX=self._vendor_key,
                        SILO_TYPE=self._check_silo_type())
        return _context

    def main_process(self):
        self._run_rule_engine.rule_process()

if __name__ == '__main__':
    runRule = RuleEngineMain('55', '267', 'SVR')  # for VENDOR: PEPSI retailer: AHOLD
    # runRule = RuleEngineMain('5', '267', 'SVR')  # for VENDOR: ULEVER retailer: AHOLD
    # runRule = RuleEngineMain('5439', '15', 'SVR')     # for target retailer
    print(runRule._getting_context())
    runRule.main_process()

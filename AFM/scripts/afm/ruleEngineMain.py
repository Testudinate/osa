#!/usr/bin/python3
# coding=utf-8


from RunRuleEngine import RunRuleEngine
from common.DBOperation import DWAccess
from common.DBOperation import APPAccess


class RuleEngineMain(object):
    def __init__(self, vendor_key, retailer_key, silo_type, *, force_rerun=False, period_key=None):
        self._vendor_key = vendor_key
        self._retailer_key = retailer_key
        self._dw_connection = DWAccess()
        self._app_connection = APPAccess()
        # self._silo_db_name = silo_db_name
        # self._silo_server_name = silo_server_name
        self._silo_type = silo_type         # silo type: [WM / SVR]
        self._force_rerun = force_rerun     # rerun AFM even alerts had been issued already. default: 0
        self._period_key = period_key       # check if period_key pass to AFM or not.
        self._context = self._getting_context()
        self._run_rule_engine = RunRuleEngine(self._dw_connection, self._app_connection, self._context)

    # TODO : will use a separate Service to get below parameters instead of doing this in AFM process.
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
        _period_key, _seq_num = self._getting_alerts_info()
        _context = dict(SCHEMA_NAME="OSA_" + self._getting_retailer_name() + "_BEN",
                        VENDOR_KEY=self._vendor_key,
                        RETAILER_KEY=self._retailer_key,
                        VENDOR_NAME=self._getting_vendor_name(),
                        RETAILER_NAME=self._getting_retailer_name(),
                        SUFFIX=self._vendor_key,
                        SILO_TYPE=self._check_silo_type(),
                        FORCE_RERUN=self._force_rerun,
                        PERIOD_KEY=_period_key,
                        SEQ_NUM=_seq_num)
        return _context

    def _getting_alerts_info(self):
        if self._period_key is not None:    # if AG pass period_key, then use this period_key
            # "Alert Generation" will update this meta table ANL_META_RAW_ALERTS_SEQ.
            sql = "SELECT MAX(seq_num) FROM ANL_META_RAW_ALERTS_SEQ " \
                  "WHERE vendor_key = {0} AND retailer_key = {1} AND alert_day = {2}"\
                .format(self._vendor_key, self._retailer_key, self._period_key)
            _seq_num = self._app_connection.query_scalar(sql)[0]
            print(sql)
            if not _seq_num:
                raise ValueError("Please help to check parameter <period_key> "
                                 "and below SQL to see if seq_num is None: %s" % sql)
            return self._period_key, _seq_num

        else:                   # else getting the latest period_key from meta table
            sql = "SELECT alert_day, seq_num FROM( " \
                  "SELECT *, ROW_NUMBER() OVER(PARTITION BY vendor_key, retailer_key " \
                  "          ORDER BY alert_day DESC, seq_num DESC) idx " \
                  "FROM [ANL_META_RAW_ALERTS_SEQ] ) x  " \
                  "WHERE idx = 1 and vendor_key  = {vendor_key} " \
                  "and retailer_key = {retailer_key}".format(vendor_key=self._vendor_key,
                                                             retailer_key=self._retailer_key)
            _period_key = self._app_connection.query_scalar(sql)[0]
            _seq_num = self._app_connection.query_scalar(sql)[1]

            if not _period_key or not _seq_num:
                raise ValueError("Please help to check below SQL to see if alert_day/seq_num are None: %s" % sql)
            print(sql)
            return _period_key, _seq_num

    def main_process(self):
        self._run_rule_engine.rule_process()

if __name__ == '__main__':
    try:
        runRule = RuleEngineMain('5', '267', 'SVR', force_rerun=False)  # for VENDOR: PEPSI retailer: AHOLD
        # runRule = RuleEngineMain('5', '267', 'SVR')  # for VENDOR: ULEVER retailer: AHOLD
        # runRule = RuleEngineMain('5439', '15', 'SVR')     # for target retailer
        print(runRule._context)
        runRule.main_process()
    except BaseException as e:
        raise
    finally:
        pass

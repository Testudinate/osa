from ruleEngineMain import *
runRule = RuleEngineMain('55', '267', 'SVR')  # for VENDOR: PEPSI retailer: AHOLD
print(runRule._getting_context())
runRule.main_process()

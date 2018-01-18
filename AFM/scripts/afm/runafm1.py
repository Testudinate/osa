from ruleEngineMain import *
runRule = RuleEngineMain('5', '267', 'SVR')  # for VENDOR: ULEVER retailer: AHOLD
print(runRule._getting_context())
runRule.main_process()


ruleEngineMain.py
--ruleEngineMain.ps1

RunRuleEngine.py
--RunRuleEngine.ps1

--SyncRuleSetForAlert.ps1
--SyncRuleSetForSVR.ps1

CheckRuleEngine.py
--CheckRuleEngine.ps1

--drop temp/staging table.
DropRuleEngineTables.ps1
dropTable.ps1

GenRuleEngineStageData.py
--GenRuleEngineStageDataForAlert.ps1
--GenRuleEngineStageDataForSVR.ps1

GenRuleEngineUPCStoreList.py
--GenRuleEngineUPCStoreList.ps1

GetIntervnetionkeyList.py
--GetIntervnetionkeyList.ps1

GetSQLSubLevelFilter.py
--GetSQLSubLevelFilter.ps1

removeDuplication.py
--removeDuplication.ps1

RunRuleEngineSpecialRule.py
--RunRuleEngineSpecialRule.ps1

RunFeedback.py
--RunFeedbackAlert.ps1
--RunFeedbackSVR.ps1
--RunFeedbackWM.ps1

UpdateAlertTable.py
UpdateAlertTableForAlert.ps1
UpdateAlertTableForAlertOnly.ps1
UpdateAlertTableForSVR.ps1

UpdateTargetTable.py
--UpdateTargetTable.ps1



checkAFMStatusForSVR.ps1


---------------------------------1, 


Schemas: OSA_AHOLD, OSA_TARGET ...


Run AFM by all owners for the same V-R if they have multi owners.
Need to add owner order in rule_set table.

Read meta/rule engine tables from sqlserver.
ANL_RULE_ENGINE_UPC_STORE_LIST: consider create a staging table / tmp table to sync data from sqlserver

call config service getting configures based on vendor and retailer instead of handling this in AFM side.
core job service will check running service (i.e.: Same V-R job can NOT run more times at the same time)
Sync forced silo will be taken care at item/store service

Phase1: sync feedback data from RDP to OSA schema(Vertica) -- handle in AFM
Phase2: feedback service will handle RDP & Portal.

Scorecard job will sync feedback as well.

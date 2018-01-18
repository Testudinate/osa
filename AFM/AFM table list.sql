##DIM/Fact tables##

#每个V-R对应一个owner。这三个字段组成一个rule_set. 每个rule_set有对应的rules。
#现在production上面Alert_Ahold这个silo, 只有alert silo上面有rule_set，然后处理所有的svr silo的数据。如果说以后没有alert silo。 那么是70个svr silos都公用一个rule_set，然后各自跑各自的afm呢，还是所有的silo循环一起跑呢？
#虽然没有alert silo的概念，那么portal上面看数据是怎么看的？比如现在的alert silo的portal如何解决？
ANL_RULE_ENGINE_RULE_SET (keep this as it is. change siloID to V-R key)
ANL_RULE_ENGINE_RULES (keep this as it is)
ANL_RULE_ENGINE_SUB_LEVEL_FILTER (filled from UI. keep this as it is)
ANL_RULE_ENGINE_UPC_STORE_LIST (keep this as it is)

ANL_RULE_ENGINE_META_DATA_PROVIDERS (keep this as it is for PublishAlert & AlertAutomation)
ANL_RULE_ENGINE_META_PROVIDERS (keep this as it is for all silos)
ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER (this depends on whether alert silo UI needed? if yes, keep this as it is for all silos)
ANL_RULE_ENGINE_META_RULES  (adding 3 more columns to control rule dependencies)

#所有silo上面的表ANL_FACT_OSM_INCIDENTS表结构一致，所以可以放到同一个表里面。
#同样所有silo上面的表ANL_FACT_ALERT_<SUFFIX>结构也是一致的。且这个表是从上面的表来的(colMapping in ps1)。但是表结构有点差异
#新的design中ANL_FACT_OSM_INCIDENTS只会新增数据且是afm的source table, 另外新建一张表存放target数据。中间不做任何的update/delete
#source表中需要seq_num/batch_id. 每次拿最新批次的数据(所以这里可能需要维护一张dim表，类似于ANL_META_ALERTS_SET)。
#新的target表中需要存放最新批次的数据。
ANL_FACT_OSM_INCIDENTS (AlertAutomation Job中用到的表)
ANL_FACT_ALERT_<SUFFIX>  (PublishAlert job里面用到的表, 以后没有这个表，都用表ANL_FACT_OSM_INCIDENTS. 且新增一张表)

ANL_FACT_AFA_<SUFFIX> (如果说feedback表中包含所有的feedback(Portal & RDP), 那么feedback rule中就不需要afa表)
ANL_FACT_FEEDBACK (这个表会有一个专门的service, 到时候应该可以直接读取这个表的数据)
ANL_RULE_ENGINE_STAGE_RULES (CheckRuleEngine.ps1. used for checking rules dependencies and many other places. might keep it as temp table.)
ANL_FACT_ALERT_TEMPLATE (这个template表也不需要了)

olap_store
olap_item_osm
DIM_HUB.ANL_DIM_OSM_INTERVENTIONCLASSIFICATION (keep this as it is)
ANL_DIM_FEEDBACK_ASSUMPTIONS (keep this as it is. 所有Alert silo和对应的SVR silo里面这个表的数据都是一样的，或者不同的alert silo有些新增的数据。)


ANL_META_ALERTS_SET (SqlServer)
ANL_RULE_ENGINE_STAGE_META_ALERTS_SET (SqlServer)


##Staging tables##:
DIM_SILO_FROM_HUB (used 1 time in RunRuleEngine.ps1. from hub table rsi_dim_silo. can change this to temp table)
CONFIG_CUSTOMERS_FROM_HUB (used 1 time in RunRuleEngine.ps1. from hub table metadata_$hubDB.CONFIG_CUSTOMERS. can change this to a temp table)
anl_rule_engine_stage_rule_list (GenRuleEngineUPCStoreList.ps1. staging table used for filter in/out item&store. can keed this as it is. )

ANL_RULE_ENGINE_ALERT_SUB_SILOS (used 1 time in RunFeedbackAlert.ps1. can change this to a temp table or use a parameter instead.)
ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL (temp表的物理化表。结构内容都一样)


##temp tables##:
ANL_RULE_ENGINE_STAGE_COMMON_1 (used 3 times in RunRuleEngine.ps1. only for loop. can keep this as a temp table)
ANL_RULE_ENGINE_STAGE_FACT_PRE (used for resetting ANL_FACT_ALERT_<SUFFIX> table in GenRuleEngineStageDataForAlert.ps1. can keep it as a temp table)
ANL_RULE_ENGINE_STAGE_FACT (data directly comes from ANL_RULE_ENGINE_STAGE_FACT_PRE in GenRuleEngineStageDataForAlert.ps1. )

ANL_RULE_ENGINE_STAGE_FACT_RULE_SET (data comes from ANL_RULE_ENGINE_STAGE_FACT with some filters. 里面存的实际还是fact数据。每次处理rule_set都会drop&create, 处理下一个rule_set)

ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET_TEMP 
ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET (来自于 ANL_RULE_ENGINE_STAGE_FACT_RULE_SET 。存放每一个rule_set处理完的结果，然后drop&create, 处理下一个rule_set。 这个表里面只存放V-R, AlertID, reject_reason, owner)

ANL_RULE_ENGINE_STAGE_FACT_TARGET (存放所有rule_set处理完的结果,树状结构)
ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL (存放所有rule_set处理完的结果, 但是rejectReason和owner是通过group_concat函数聚合后的结果。这个表需要物理化)

anl_rule_engine_stage_rule_list_temp1 (GenRuleEngineUPCStoreList.ps1 )



##SP from SqlServer
EXEC SP$ANL_RULE_ENGINE_AFM_NOTIFICATION
exec SP$RSI_GET_SUB_SILOS '$siloID'
exec SP$RSI_ALERT_SILOS '$siloID'

##DataFlow:
ANL_FACT_OSM_INCIDENTS(SVR) -> ANL_FACT_ALERT_<SUFFIX>(Alert) -> ANL_RULE_ENGINE_STAGE_FACT_PRE -> ANL_RULE_ENGINE_STAGE_FACT -> ANL_RULE_ENGINE_STAGE_FACT_RULE_SET
-> ANL_RULE_ENGINE_STAGE_FACT_TARGET_RULE_SET(Update-TargetTable) -> ANL_RULE_ENGINE_STAGE_FACT_TARGET -> ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL 
-> $schemaName.ANL_RULE_ENGINE_STAGE_FACT_TARGET_FINAL -> updating ANL_FACT_ALERT_<SUFFIX> & sync data to SqlServer for Alert Portal.





finnaly:
common schema:
ANL_RULE_ENGINE_RULE_SET
ANL_RULE_ENGINE_RULES
ANL_RULE_ENGINE_SUB_LEVEL_FILTER
ANL_RULE_ENGINE_UPC_STORE_LIST

ANL_RULE_ENGINE_META_DATA_PROVIDERS
ANL_RULE_ENGINE_META_PROVIDERS
ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER
ANL_RULE_ENGINE_META_RULES

ANL_DIM_FEEDBACK_ASSUMPTIONS
ANL_DIM_OSM_INTERVENTIONCLASSIFICATION

retailer schema:
ANL_FACT_OSM_INCIDENTS (source table)
ANL_FACT_ALERT (target table)



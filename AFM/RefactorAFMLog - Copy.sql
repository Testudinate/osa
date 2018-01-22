
chang log:
--AFM入口
--主函数
ruleEngineMain.py(ruleEngineMain.ps1)

--主角本
--去掉了读SqlServer里面的几个表，用vendor_key和retailer_key代替条件 (rsi_dim_silo & rsi_config_customer)
--去掉了邮件步骤， 之前完全通过sqlserver实现的 
RunRuleEngine.py --RunRuleEngine.ps1

--下面两个脚本不需要了，因为不需要sync rules了
--SyncRuleSetForAlert.ps1
--SyncRuleSetForSVR.ps1

--这里还需要，只不过是建表了，做一些准备工作
CheckRuleEngine.py
--CheckRuleEngine.ps1

--这两个也不需要了
--drop temp/staging table.
--DropRuleEngineTables.ps1
--dropTable.ps1

--把下面两个合到一起了, 基本上是从SVR的脚本里面迁移过来的逻辑
--这里有个advantage feedback, 这个功能只是简单的从相应的插件里面把功能搬过来了。
GenRuleEngineStageData.py
--GenRuleEngineStageDataForAlert.ps1
--GenRuleEngineStageDataForSVR.ps1

--这个基本上没变
GenRuleEngineUPCStoreList.py
--GenRuleEngineUPCStoreList.ps1

--这个基本上没变
GetIntervnetionkeyList.py
--GetIntervnetionkeyList.ps1

--这个基本上没变
GetSQLSubLevelFilter.py
--GetSQLSubLevelFilter.ps1

--这个基本上没变
removeDuplication.py
--removeDuplication.ps1

--这个基本上没变
RunRuleEngineSpecialRule.py
--RunRuleEngineSpecialRule.ps1

--这个基本上从SVR的code迁移过来的
RunFeedback.py
--RunFeedbackAlert.ps1
--RunFeedbackSVR.ps1
--RunFeedbackWM.ps1

--这个基本上从SVR的code迁移过来的
UpdateAlertTable.py
--UpdateAlertTableForAlert.ps1
--UpdateAlertTableForAlertOnly.ps1
--UpdateAlertTableForSVR.ps1

--这个不需要update回去了，只要把issue过的数据insert到一张新表里面即可
UpdateTargetTable.py
--UpdateTargetTable.ps1


--基本改动
所有staging tables都必须加上suffix(暂时用vendor_key).
所有的物理表都不能用truncate, 需要在所有用到的地方加上vendor_key的filter
有些用到的表必须要有vendor_key这个字段，目前只加了几个表
如何封装接口？
是否需要swap_partition? 取决于incidents表是否够大(如果说incidents表只增不减的话，数据量会比较大，建议用staging表swap vendor数据出来在做计算)


--至于fact表如何存储。
比如说 ANL_FACT_OSM_INCIDENTS 表。如果说所有的V-R的表都存放到一张表里面，那么势必要用到分区。如果按照V-R组合来分区的话，差不多有1000个分区。
但是Vertica里面最多只能支持1024个分区。所以说这种方案不可行。
目前讨论下来就按照Retailer来建schema. 这样的话每个表可以用 vendor 来分区。这样就没有分区数量限制的问题了。 


问题：
AFM service相关的问题：
1, 按照retailer来分schema，按照vendor_key来分区。 staging表按用vendor_key结尾，temp表不变？
   那么source表就是 anl_fact_osm_incidents , target表是 anl_fact_alert用来存放issue过了alerts
   
2, 没有了Alert Silo，那么是每个SVR silo都是有各自的alert cycle. 每个V-R根据这个表里面的时候trigger起来？

3, 如何生成表 ANL_META_ALERTS_SET ? 是不是每次生成anl_fact_osm_incidents表的时候 seqnum会随message一起传过来。
   这个时候可以去比较 ANL_META_ALERTS_SET，如果有的话就说明跑过了 没有的话说明还没跑就insert一条数据进去，然后跑afm

4, 是否可以直接用python里面的logging， 这个只能写文件，是否需要把log输出到表里面。如果不需要的话那UI那边是否需要显示log，或者直接读log文件？

5, AFM里面用到了一些SqlServer上面的表： rsi_dim_silo, metadata_$hubDB.CONFIG_CUSTOMERS
   如果说针对每个SVR silo, 那么目前就不需要这些表了
   
6, Alert Portal?

7, 同一个owner可以存在多个rule_set_name/silo_id/V-R, 




1, Python里面如果设计到外部表，如何处理？
A: 可以通过文件的方式把数据导入到Pandas里面，然后通过pandas来处理

2, 如果所有的表都在OSA数据库下，那是不是可以直接通过python来跑sql？
A: 我想应该这样做把，否则全部用pandas能否实现非常复杂的逻辑，比如窗口函数等。

3, 如何处理temp/staging table?
A: temp table可以保留，但是staging table是不是可以在后面加上suffix, 否则多个AFM job同时跑的话会在同一个cluster上面创建多个同样的表。就会报错。
   另外有些中间表可以通过Pandas放到内存里面，不需要创建表。
   那么suffix应该是什么呢？vendor_key & retailer_key?  or service_id
   如果是按照retailer分schema的话那么所有的staging表的suffix就是vendor_key? or vendor_key & service_id ?

4, 有没有可能同一个V-R的同一种job同时在跑？ 
    这个应该是不可以的，当前的silo的模式也不可能同一个job同时跑。 
	所以suffix可以按照vendor_key & retailer_key. 
	如果按照retailer来分schema的话suffix可以只是vendor_key

5,  UI上面的alert silo是否还需要？ 比如Alert_AHOLD silo上面的PublishAlert是按照一个job循环svr silo跑呢？ 还是分70个svr silo并行跑？


---------------------------------1, 
AFM demo Meeting minutes 20180118:

DBs:
APP DB: OSA
DW Schemas: OSA_AHOLD, OSA_TARGET etc.

AFM:
Run AFM by all owners for the same V-R if they have multi owners.
Need to add owner priority in rule set mapping table.
AFM should Read meta/rule engine tables from SqlServer instead of Vertica.
ANL_RULE_ENGINE_UPC_STORE_LIST: Consider creating a staging/tmp table to sync data from SqlServer since this table will join OLAP_ITEM/STORE tables.

Other related services:
Call “Configure Service” getting configures(vendor_name, retailer_name, type, etc.) based on vendor and retailer instead of handling this in AFM side.
“Core job Service” will check running service (i.e.: AFM job can NOT be running when there is already an AFM job running for the same V-R)
Sync forced silo will be taken care at item/store service

Feedback:
Phase1: sync feedback data from RDP to OSA schema(Vertica). --handle in AFM
Phase2: Ultimately “Feedback Service” will handle feedbacks from RDP & Portal.  –Create a CR for tracking this, out of this scope
Scorecard job will also need to sync feedback as well. 


----
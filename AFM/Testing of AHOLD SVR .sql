
用ahold的数据来测试AFM：(silo: ahold, period_key: 20180128)
Copy 所有 alert_ahold 下面的 svr silo 里面的数据到测试库。 alert_ahold silo下面一共有69个svr silos。
1, copy 69 个silo上面的 anl_fact_osm_incidents 表里面的数据到QA环境里面的 osa_ahold_ben schema.
   这里要注意update表里面的几个字段. Owner to NULL, Issuanceid to 1, RejectReasons to NULL. 
2, copy olap_item_osm & olap_store 到上面的QA Vertica环境。
3, 从alert silo 上面copy下面几个表到 osa_ahold_ben SqlServer 数据库
	select * from ALERT_AHOLD.ANL_RULE_ENGINE_META_DATA_PROVIDERS;
	select * from ALERT_AHOLD.ANL_RULE_ENGINE_META_PROVIDERS;
	--select * from ALERT_AHOLD.ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER;
	select * from ALERT_AHOLD.ANL_RULE_ENGINE_META_RULES;

	select * from ALERT_AHOLD.ANL_RULE_ENGINE_RULE_SET;
	select * from ALERT_AHOLD.ANL_RULE_ENGINE_RULES;
	select * from ALERT_AHOLD.ANL_RULE_ENGINE_SUB_LEVEL_FILTER;
	select * from ALERT_AHOLD.ANL_RULE_ENGINE_UPC_STORE_LIST;
4, 稍微修改下上面几个表里面的配置信息根据新的表结构。

5, 并行跑69个SVR vendors 相关的 AFM job
--CD D:\OneDrive\program\OSA_SUITE\AFM\scripts\afm & D: 
CD C:\Ben\python\osa\AFM\scripts\afm
python.exe -c "from ruleEngineMain import *; runRule = RuleEngineMain('113', '267', 'SVR'); runRule.main_process()"
5	--done
6	--done
15	--done
23	--done
33	--done
52	--error. rule 	--这两个失败的原因是因为有多个rule_set, 记录在问题5里面
55	--error. duplicated
110	--done
112	--done
113	--done

6, 比较 QA和PRO上面的 ANL_FACT_ALERT 表的结果


问题记录：
1, ANL_META_VR_OWNER_MAPPING 这个表没有数据，直接结束了，也没有任何提示？
2, OLAP_ITEM_OSM & OLAP_STORE 没有数据也是直接结束了。 也没有任何提示？
3, 拼接case when的时候报错了，是因为之前的 PowerShell 里面 不同的rule id之间是有关联关系的。
   新的 design 里面应该是UI去实现这块逻辑，所以表里面要手动去更新这些值
   ruleid: 13, 14, 16, 17, 18
   参考后面的sql
4, 导数据的时候没有清掉olap_item_osm的数据，导致有重复记录 (item_key)
5, 因为之前的PowerShell是先把4个rule_engine表都同步到vertica里面在进行计算的。
   所以这里务必要保证，所有用到那4个表的地方都加上了之前同步脚本里面的逻辑，外加自己的逻辑
6, 结果不大一致，主要是因为没有 sync anl_fact_feedback table
7, 不要同时分析公用的表, 去掉相关的分析函数
   pyodbc.Error: ('55V04', '[55V04] ERROR 2083:  A Analyze Statistics operation is already in progress on projection OSA_AHOLD_BEN.ANL_DIM_FEEDBACK_ASSUMPTIONS_b0 [txnid 49539596868454305 session v_fusion_node0002-16732:0x20b3102]\n (2083) (SQLExecDirectW)')
8, 因为python是区分大小写的，所以下面的写法没法成立。
   if _metrics_type == 'store-alertType filter': 改成
   if _metrics_type.lower() == 'store-alertType filter'.lower(): 
9, <Volume Limit>: ALERT_AHOLD_Ahold(AlertRank) NULLS FIRST for default. change this to NULLS LAST?
10, ALERT SILO will apply filter "DSD = 0" but NOT for SVR silo.
11, RSI_ALERT_TYPE: there is no RSI_ALERT_TYPE in SVR Silo. 
11, ALERT_AHOLD_Ahold(Feedback). 确实这条 rule 导致有差异，但是rule本身的逻辑是没有问题的。 
    我查了几个 alert_id 在 anl_fact_feedback 表和 anl_fact_afa_suffix 表。 都没有找到feedback, 但是pro上面是被feedback rejected掉了。比较奇怪

----preparing data for step 1,2------
SELECT count(*) FROM ALT_UNILEVER_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128;     --87202
SELECT count(*) FROM ALT_PHARMAVITE_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128;   --1631
SELECT count(*) FROM ALT_COKE_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128;         --124289
SELECT count(*) FROM ALT_GENERAL_ELECTRIC_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128;     --4113
SELECT count(*) FROM ALT_HERSHEY_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128;      --131091
SELECT count(*) FROM ALT_JOHNSONANDJOHNSON_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128;    --16581 OK
SELECT count(*) FROM ALT_PEPSICO_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128;      --255473
SELECT count(*) FROM ALT_SC_JOHNSON_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128;   --35949
SELECT count(*) FROM ALT_CAMPBELL_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128;     --79093
SELECT count(*) FROM ALT_MAYBELLINE_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128;   --7531

--@export on;
--@export set filename="D:\CHOBANI_TARGET_Chronic_OOS_180Days.csv" CsvColumnDelimiter="|" ShowNullAs="";
--or

vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -At -F '|' -c "SELECT * FROM ALT_UNILEVER_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128" -o "ALT_UNILEVER_AHOLD.txt"
vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "SELECT * FROM ALT_PHARMAVITE_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128" -Atq -P footer=off -F '|' -o "ALT_PHARMAVITE_AHOLD.txt"
vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "SELECT * FROM ALT_COKE_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128" -Atq -P footer=off -F '|' -o "ALT_COKE_AHOLD.txt"
vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "SELECT * FROM ALT_GENERAL_ELECTRIC_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128" -Atq -P footer=off -F '|' -o "ALT_GENERAL_ELECTRIC_AHOLD.txt"
vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "SELECT * FROM ALT_HERSHEY_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128" -Atq -P footer=off -F '|' -o "ALT_HERSHEY_AHOLD.txt"
vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "SELECT * FROM ALT_JOHNSONANDJOHNSON_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128" -Atq -P footer=off -F '|' -o "ALT_JOHNSONANDJOHNSON_AHOLD.txt"
vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "SELECT * FROM ALT_PEPSICO_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128" -Atq -P footer=off -F '|' -o "ALT_PEPSICO_AHOLD.txt"
vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "SELECT * FROM ALT_SC_JOHNSON_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128" -Atq -P footer=off -F '|' -o "ALT_SC_JOHNSON_AHOLD.txt"
vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "SELECT * FROM ALT_CAMPBELL_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128" -Atq -P footer=off -F '|' -o "ALT_CAMPBELL_AHOLD.txt"
vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "SELECT * FROM ALT_MAYBELLINE_AHOLD.ANL_FACT_OSM_INCIDENTS WHERE period_key = 20180128" -Atq -P footer=off -F '|' -o "ALT_MAYBELLINE_AHOLD.txt"


vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "SELECT * FROM ALERT_AHOLD.OLAP_ITEM_OSM" -Atq -P footer=off -F '|' -o "OLAP_ITEM_OSM.txt"
vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "SELECT * FROM ALERT_AHOLD.OLAP_STORE" -Atq -P footer=off -F '|' -o "OLAP_STORE.txt"
vsql -U ben.wu -d fusion -h PRODVERTICANXG7.PROD.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "SELECT * FROM ALERT_AHOLD.ANL_FACT_FEEDBACK" -Atq -P footer=off -F '|' -o "ANL_FACT_FEEDBACK.txt"
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.OLAP_ITEM_OSM_import FROM local 'OLAP_ITEM_OSM.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.OLAP_STORE_import FROM local 'OLAP_STORE.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.ANL_FACT_FEEDBACK_import FROM local 'ANL_FACT_FEEDBACK.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"


--loading
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS_import FROM local 'ALT_UNILEVER_AHOLD.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS_import FROM local 'ALT_PHARMAVITE_AHOLD.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS_import FROM local 'ALT_COKE_AHOLD.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS_import FROM local 'ALT_GENERAL_ELECTRIC_AHOLD.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS_import FROM local 'ALT_HERSHEY_AHOLD.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS_import FROM local 'ALT_JOHNSONANDJOHNSON_AHOLD.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS_import FROM local 'ALT_PEPSICO_AHOLD.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS_import FROM local 'ALT_SC_JOHNSON_AHOLD.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS_import FROM local 'ALT_CAMPBELL_AHOLD.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"
vsql -U ben.wu -d fusion -h QAVERTICANXG.ENG.RSICORP.LOCAL -w Bat.Pit.Pan-444 -c "COPY OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS_import FROM local 'ALT_MAYBELLINE_AHOLD.txt' skip 0 delimiter E'|' trailing nullcols rejectmax 1000 REJECTED DATA 'rej.log' EXCEPTIONS 'err.log' --enable-connection-load-balance DIRECT no escape;"


---------loading anl_fact_feedback-------
--insert into osa_ahold_ben.anl_fact_feedback
select -1 as EVENT_KEY, alert_fdbk.RETAILER_KEY, alert_fdbk.VENDOR_KEY, cast(STORE_VISITED_PERIOD_KEY::varchar as timestamp) as STORE_VISIT_DATE, 
PERIOD_KEY, 'A' TYPE, NULL, ALERT_ID, NULL as ALERT_TYPE, NULL as MERCHANDISER_STORE_NUMBER, store.STORE_ID, NULL as MERCHANDISER_UPC, 
item.UPC as INNER_UPC, 'Ahold' as MERCHANDISER, STORE_REP, NULL as SOURCE, NULL as BEGIN_STATUS, NULL as ACTION, FEEDBACK_DESCRIPTION , 
NULL as FEEDBACK_HOTLINEREPORTDATE, NULL as FEEDBACK_ISININVENTORY, NULL as UPC_STATUS, NULL as MSI
from osa_ahold_ben.anl_fact_feedback_alert alert_fdbk
inner join osa_ahold_ben.olap_store store
on alert_fdbk.store_key = store.store_key and alert_fdbk.RETAILER_KEY=store.RETAILER_KEY
inner join osa_ahold_ben.olap_item_osm item
on alert_fdbk.item_key = item.item_key and alert_fdbk.vendor_key=item.vendor_key


-----------------------------------update rule parameters----------------------
CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_RULES  ON COMMIT preserve ROWS as 
SELECT /*+label(GX_OSM_RULE_ENGINE)*/ a.*,b.PROVIDER_SUB_TYPE,b.METRICS_ORDER AS PROCESSING_ORDER, b.DEPEND_ON_PREVIOUS_RULE 
FROM ALERT_AHOLD.ANL_RULE_ENGINE_RULES a 
inner join ALERT_AHOLD.ANL_RULE_ENGINE_META_RULES b on a.RULE_ID=b.RULE_ID and a.enabled in ('Y','T')
inner join ALERT_AHOLD.ANL_RULE_ENGINE_RULE_SET c on a.RULE_SET_ID=c.RULE_SET_ID and c.ENABLED in ('Y','T')
--select * from ANL_RULE_ENGINE_STAGE_RULES

CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_RULES_TEMP  ON COMMIT preserve ROWS AS 
select /*+label(GX_OSM_RULE_ENGINE)*/ a.RULE_ID,a.RULE_SET_ID,b.PARAMETER1 as PARAMETER2   
FROM ANL_RULE_ENGINE_STAGE_RULES a
INNER JOIN ANL_RULE_ENGINE_STAGE_RULES b ON a.RULE_SET_ID = b.rule_set_id AND 
(
	(a.RULE_ID = 13 AND b.RULE_ID = 14) 
	OR (a.RULE_ID = 14 AND b.RULE_ID = 13)
	OR (a.RULE_ID = 15 AND b.RULE_ID = 16)
	OR (a.RULE_ID = 16 AND b.RULE_ID = 15)
	OR (a.RULE_ID = 17 AND b.RULE_ID = 14)
	OR (a.RULE_ID = 18 AND b.RULE_ID = 16)
)
--select * from ANL_RULE_ENGINE_STAGE_RULES_TEMP

CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_RULES_TEMP_1  ON COMMIT preserve ROWS AS 
select /*+label(GX_OSM_RULE_ENGINE)*/ a.RULE_ID ,a.RULE_SET_ID , a.SUB_LEVEL_METRICS ,a.PARAMETER1 ,
		case when b.PARAMETER2 is null then a.PARAMETER2 else b.PARAMETER2 end PARAMETER2 ,a.PARAMETER3, 
        a.ENABLED, a.PROVIDER_SUB_TYPE, a.PROCESSING_ORDER, a.DEPEND_ON_PREVIOUS_RULE
from ANL_RULE_ENGINE_STAGE_RULES a 
left join ANL_RULE_ENGINE_STAGE_RULES_TEMP b
on a.RULE_ID=b.RULE_ID and a.RULE_SET_ID=b.RULE_SET_ID 
--select * from ANL_RULE_ENGINE_STAGE_RULES_TEMP_1

drop table ANL_RULE_ENGINE_STAGE_RULES;
alter table ANL_RULE_ENGINE_STAGE_RULES_TEMP_1 rename to ANL_RULE_ENGINE_STAGE_RULES

CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_RULES_TEMP1  ON COMMIT preserve ROWS AS 
select /*+label(GX_OSM_RULE_ENGINE)*/ a.RULE_ID,a.RULE_SET_ID,b.PARAMETER1 as PARAMETER1
FROM   ANL_RULE_ENGINE_STAGE_RULES a
INNER JOIN ANL_RULE_ENGINE_STAGE_RULES b 
ON a.RULE_SET_ID = b.rule_set_id AND 
(
	  (a.RULE_ID = 17 AND b.RULE_ID = 13)
	  OR (a.RULE_ID = 18 AND b.RULE_ID = 15)
) 
--select * from ANL_RULE_ENGINE_STAGE_RULES_TEMP1

CREATE LOCAL TEMP TABLE ANL_RULE_ENGINE_STAGE_RULES_TEMP_1  ON COMMIT preserve ROWS AS 
select /*+label(GX_OSM_RULE_ENGINE)*/ a.RULE_ID ,a.RULE_SET_ID , a.SUB_LEVEL_METRICS ,
case when b.PARAMETER1 is null then a.PARAMETER1 else b.PARAMETER1 end PARAMETER1,a.PARAMETER2,a.PARAMETER3, 
        a.ENABLED, a.PROVIDER_SUB_TYPE, a.PROCESSING_ORDER, a.DEPEND_ON_PREVIOUS_RULE
 from ANL_RULE_ENGINE_STAGE_RULES a 
 left join  ANL_RULE_ENGINE_STAGE_RULES_TEMP1 b
 on a.RULE_ID=b.RULE_ID and a.RULE_SET_ID=b.RULE_SET_ID
--select * from ANL_RULE_ENGINE_STAGE_RULES_TEMP_1
 
--alter table ANL_RULE_ENGINE_STAGE_RULES_TEMP_1 rename to ANL_RULE_ENGINE_STAGE_RULES
-----------------------------------------------------------------------------------






--svr silos
--SELECT silo_id, * FROM RSI_CONFIG_CUSTOMERS WHERE SILO_ID <> 'ALERT_AHOLD' ORDER BY VENDOR_KEY

ALT_UNILEVER_AHOLD	ULEVERAHOLD	5	267
ALT_PHARMAVITE_AHOLD	PHARAHOLD	6	267
ALT_COKE_AHOLD	COKEAHOLD	15	267
ALT_GENERAL_ELECTRIC_AHOLD	GELECAHOLD	23	267
ALT_HERSHEY_AHOLD	HERSHYAHOLD	33	267
ALT_JOHNSONANDJOHNSON_AHOLD	JNJAHOLD	52	267
ALT_PEPSICO_AHOLD	PEPSIAHOLD	55	267
ALT_SC_JOHNSON_AHOLD	SCJNSNAHOLD	110	267
ALT_CAMPBELL_AHOLD	CMPBLLAHOLD	112	267
ALT_MAYBELLINE_AHOLD	MAYBLNAHOLD	113	267
ALT_ENERGIZER_AHOLD	ENRGZRAHOLD	115	267
ALT_KMCLRK_AHOLD	KMCLRKAHOLD	123	267
ALT_THREEM_AHOLD	THREEMAHOLD	157	267
ALT_PG_AHOLD	PNGAHOLD	300	267
ALT_NESTLE_AHOLD	NESTLEAHOLD	342	267
ALT_KELLOGG_AHOLD	KELLOGAHOLD	353	267
ALT_RECKITT_BENCKISER_AHOLD	RECKITAHOLD	355	267
ALT_GHIRARDELLI_AHOLD	GHRDLIAHOLD	365	267
ALT_HENKEL_AHOLD	HENKELAHOLD	366	267
ALT_CONAGRA_AHOLD	CONAGRAHOLD	372	267
ALT_NESTLE_PURINA_AHOLD	NSTPURAHOLD	382	267
ALT_PINNACLE_AHOLD	PINACLAHOLD	388	267
ALT_ADVANTAGE_SALES_AHOLD	ASMAHOLD	391	267
ALT_GEORGIA_PACIFIC_AHOLD	GAPACAHOLD	469	267
ALT_CLOROX_AHOLD	CLOROXAHOLD	470	267
ALT_WHITEWAVE_AHOLD	WHTWAVAHOLD	476	267
ALT_LINDT_AHOLD	LINDTAHOLD	484	267
ALT_GENERAL_MILLS_AHOLD	GMILLSAHOLD	530	267
ALT_DANNON_AHOLD	DANNONAHOLD	647	267
ALT_MONDELEZ_AHOLD	SNACKRAHOLD	701	267
ALT_KRAFTGR_AHOLD	GROKRAHOLD	702	267
ALT_POST_AHOLD	POSTAHOLD	789	267
ALT_KEURIG_AHOLD	KEURIGAHOLD	845	267
ALT_AHOLD_OWN_BRANDS_AHOLD	AHLDPLAHOLD	857	267
ALT_BARILLA_AHOLD	BARILLAHOLD	858	267
ALT_MCCORMICK_AHOLD	MCRMCKAHOLD	5025	267
ALT_HORMEL_AHOLD	HORMELAHOLD	5073	267
ALT_FORCED_AHOLD	FORCEDAHOLD	5088	267
ALT_MARS_AHOLD	MARSAHOLD	5091	267
ALT_SUN_PRODUCTS_AHOLD	SUNPRDAHOLD	5191	267
ALT_DR_PEPPER_SNAPPLE_AHOLD	DPSGAHOLD	5222	267
ALT_REYNOLDS_AHOLD	REYNLDAHOLD	5300	267
ALT_HOME_MARKET_FOODS_AHOLD	HOMEFDAHOLD	5305	267
ALT_RICH_PRODUCTS_AHOLD	RICHPRAHOLD	5315	267
ALT_CHOBANI_AHOLD	CHOBNIAHOLD	5439	267
ALT_REILY_FOODS_AHOLD	REILYFAHOLD	5462	267
ALT_BACK_TO_NATURE_AHOLD	BTNFDSAHOLD	5496	267
ALT_AINSWORTH_PET_AHOLD	ANSWRTAHOLD	5537	267
ALT_CABOT_CREAMERY_AHOLD	CABOTCAHOLD	5540	267
ALT_SIGGIS_DAIRY_AHOLD	SIGGISAHOLD	5558	267
ALT_SARGENTO_AHOLD	SRGNTOAHOLD	5563	267
ALT_ATEECO_AHOLD	ATEECOAHOLD	5592	267
ALT_CLEMENS_FOOD_AHOLD	CLEMNSAHOLD	5604	267
ALT_PROMOTION_IN_MOTION_AHOLD	PROMOTAHOLD	5626	267
ALT_POWERMAX_AHOLD	PWRMAXAHOLD	5632	267
ALT_HUHTAMAKI_AHOLD	HHTMKIAHOLD	5636	267
ALT_FRESHPET_AHOLD	FRSHPTAHOLD	5640	267
ALT_FM_BROWNS_AHOLD	FMBRWNAHOLD	5641	267
ALT_LAND_O_LAKES_AHOLD	LNDLAKAHOLD	5648	267
ALT_BRADSHAW_INTERNATIONAL_AHOLD	BRDSHWAHOLD	5660	267
ALT_DIETZ_WATSON_AHOLD	DTZWATAHOLD	5691	267
ALT_BEL_BRANDS_AHOLD	BLBRNDAHOLD	5696	267
ALT_SUPERIOR_FOOD_BROKERAGE_AHOLD	SUPRFDAHOLD	5704	267
ALT_BD_SALES_AHOLD	BDSLSMAHOLD	5710	267
ALT_BUDD_FOODS_AHOLD	BUDDFDAHOLD	5748	267
ALT_SCOTCH_AHOLD	SCOTCHAHOLD	5801	267
ALT_EURO_AMERICAN_BRANDS_AHOLD	EUROAMAHOLD	5819	267
ALT_ELMHURST_MILKED_AHOLD	ELMMLKAHOLD	5822	267
ALT_DANIELE_AHOLD	DANELEAHOLD	5823	267
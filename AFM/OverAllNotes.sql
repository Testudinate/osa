env:
Sqlserver 
Server name: ENGV2HHDBQA1\RV2
DB name: OSA 

Vertica
DB server: QAVERTICANXG
schema: 

ENGV3DSTR1 - 10.172.36.74
ENGV3DSTR2 - 10.172.36.75
ENGV3DSTR3 - 10.172.36.76
ENGV3DSTR4 - 10.172.36.97
ENGV3DSTR5 - 10.172.36.98



存在三个需要做成services的地方:
1, AFM
    这里将去掉RawAlertETl和FilteredAlertETL. 
	新架构会直接把所有注册过OSM的silo的ANL_FACT_OSM_INCIDENTS表放到一个Vertica Cluster(OSA). 这样就不需要中间的ETL过程。
	注意： OSM Job 和 AFM 可能存在同时跑的情况。当前的逻辑是通过SEQ_NUM来实现的，就是说同一天每次OSM跑完后就生成一个transfer set, SEQ_NUM默认为1， 然后每跑一次+1.
	       RawAlertETL: 会把每次跑的数据都transfer到Alert DW上面去并且把字段SEQ_NUM也加进去。 这样就能知道在target table中处理的是OSM第几次跑的数据。
		   FilteredAlertETL: 会根据meta表ANL_META_ALERTS_SET里面的信息，同步被AFM issued过的SEQ_NUM最大的那批处理过的alerts回到SVR silo上面的staging表里面去，然后更新对应的ANL_FACT_OSM_INCIDENTS表(通过incidentid join staging table)。
		   ANL_FACT_OSM_INCIDENTS表每次跑的结果有一个字段肯定是不一样的-incidentid/alertid. 因为这个字段是自增的。 所以如果一天跑多次，可能除了这个字段以外的字段都是一样的。除非后面几次跑之前有更新过rule_set.

2, FeedbackETL
	Feedback有两种传输方式：
	a, 通过UI Save到SqlServer上面，然后在通过FeedbackETL sync到DW上面。
		当前feedbackETL是从AlertSilo SqlServer DB里面transfer数据到SVR Silo DW上面去。传过去的目的是给Scorecard用。
	b, Merchandiser把数据上传到RDP, 然后通过RDP传到SVR Silo DW. 
	Services里面需要包含上面两种功能以及外部访问的接口。
	
	--从目前的数据来看，所有的feedback都是通过RDP_AUX这个rdp来transfer到各个silo上面
	--下面的sql可以查到有哪些silo是通过rdp的方式transfer feedback数据的
	select retailer.RETAILER_name,vendor.vendor_name,count(*) from RDP_AUX.DS_FACT_FEEDBACK feedback
	inner join RDP_AUX.DIM_VENDOR vendor
	on feedback.vendor_key = vendor.vendor_key
	inner join RDP_AUX.DIM_RETAILER retailer
	on feedback.RETAILER_KEY = retailer.RETAILER_KEY
	group by retailer.RETAILER_name,vendor.vendor_name
	order by 1,2
	
	--下面的sql可以从transfer_detail表里面查到event_key
	--Hub
	select process_keys as EVENT_KEY,* from etl.RSI_TRANSFER_DETAIL 
	--where retailer_key = 5099 and vendor_key = 146 
	where process_keys is not null
	order by transfer_detail_start desc
	
3, OSMScorecard
	这里需要一个单独的Services。 由于Alert Silo上面不需要Cube，所以ScorecardETL的功能也会随之取消掉。
	不过这里面需要提供外部访问的接口。AFM里面会用到Scorecard里面的表



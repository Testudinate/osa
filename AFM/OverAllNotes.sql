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



����������Ҫ����services�ĵط�:
1, AFM
    ���ｫȥ��RawAlertETl��FilteredAlertETL. 
	�¼ܹ���ֱ�Ӱ�����ע���OSM��silo��ANL_FACT_OSM_INCIDENTS��ŵ�һ��Vertica Cluster(OSA). �����Ͳ���Ҫ�м��ETL���̡�
	ע�⣺ OSM Job �� AFM ���ܴ���ͬʱ�ܵ��������ǰ���߼���ͨ��SEQ_NUM��ʵ�ֵģ�����˵ͬһ��ÿ��OSM����������һ��transfer set, SEQ_NUMĬ��Ϊ1�� Ȼ��ÿ��һ��+1.
	       RawAlertETL: ���ÿ���ܵ����ݶ�transfer��Alert DW����ȥ���Ұ��ֶ�SEQ_NUMҲ�ӽ�ȥ�� ��������֪����target table�д������OSM�ڼ����ܵ����ݡ�
		   FilteredAlertETL: �����meta��ANL_META_ALERTS_SET�������Ϣ��ͬ����AFM issued����SEQ_NUM���������������alerts�ص�SVR silo�����staging������ȥ��Ȼ����¶�Ӧ��ANL_FACT_OSM_INCIDENTS��(ͨ��incidentid join staging table)��
		   ANL_FACT_OSM_INCIDENTS��ÿ���ܵĽ����һ���ֶο϶��ǲ�һ����-incidentid/alertid. ��Ϊ����ֶ��������ġ� �������һ���ܶ�Σ����ܳ�������ֶ�������ֶζ���һ���ġ����Ǻ��漸����֮ǰ�и��¹�rule_set.

2, FeedbackETL
	Feedback�����ִ��䷽ʽ��
	a, ͨ��UI Save��SqlServer���棬Ȼ����ͨ��FeedbackETL sync��DW���档
		��ǰfeedbackETL�Ǵ�AlertSilo SqlServer DB����transfer���ݵ�SVR Silo DW����ȥ������ȥ��Ŀ���Ǹ�Scorecard�á�
	b, Merchandiser�������ϴ���RDP, Ȼ��ͨ��RDP����SVR Silo DW. 
	Services������Ҫ�����������ֹ����Լ��ⲿ���ʵĽӿڡ�
	
	--��Ŀǰ���������������е�feedback����ͨ��RDP_AUX���rdp��transfer������silo����
	--�����sql���Բ鵽����Щsilo��ͨ��rdp�ķ�ʽtransfer feedback���ݵ�
	select retailer.RETAILER_name,vendor.vendor_name,count(*) from RDP_AUX.DS_FACT_FEEDBACK feedback
	inner join RDP_AUX.DIM_VENDOR vendor
	on feedback.vendor_key = vendor.vendor_key
	inner join RDP_AUX.DIM_RETAILER retailer
	on feedback.RETAILER_KEY = retailer.RETAILER_KEY
	group by retailer.RETAILER_name,vendor.vendor_name
	order by 1,2
	
	--�����sql���Դ�transfer_detail������鵽event_key
	--Hub
	select process_keys as EVENT_KEY,* from etl.RSI_TRANSFER_DETAIL 
	--where retailer_key = 5099 and vendor_key = 146 
	where process_keys is not null
	order by transfer_detail_start desc
	
3, OSMScorecard
	������Ҫһ��������Services�� ����Alert Silo���治��ҪCube������ScorecardETL�Ĺ���Ҳ����֮ȡ������
	������������Ҫ�ṩ�ⲿ���ʵĽӿڡ�AFM������õ�Scorecard����ı�



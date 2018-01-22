/*
--All tables needed for AFM
ANL_DIM_FEEDBACK_ASSUMPTIONS
ANL_FACT_FEEDBACK
ANL_FACT_OSM_INCIDENTS
ANL_RULE_ENGINE_META_DATA_PROVIDERS
ANL_RULE_ENGINE_META_PROVIDERS
ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER
ANL_RULE_ENGINE_META_RULES
ANL_RULE_ENGINE_RULE_SET
ANL_RULE_ENGINE_RULES
ANL_RULE_ENGINE_SUB_LEVEL_FILTER
ANL_RULE_ENGINE_UPC_STORE_LIST
OLAP_ITEM
OLAP_ITEM_OSM
OLAP_STORE
SPD_FACT_PIVOT
ANL_STORE_SALES

insert into OSA_AHOLD_BEN.ANL_DIM_FEEDBACK_ASSUMPTIONS select * from PEPSI_AHOLD_MB.ANL_DIM_FEEDBACK_ASSUMPTIONS;
insert into OSA_AHOLD_BEN.ANL_FACT_FEEDBACK select * from PEPSI_AHOLD_MB.ANL_FACT_FEEDBACK;
insert into OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS select * from PEPSI_AHOLD_MB.ANL_FACT_OSM_INCIDENTS;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_META_DATA_PROVIDERS select * from PEPSI_AHOLD_MB.ANL_RULE_ENGINE_META_DATA_PROVIDERS;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_META_PROVIDERS select * from PEPSI_AHOLD_MB.ANL_RULE_ENGINE_META_PROVIDERS;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER select * from PEPSI_AHOLD_MB.ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_META_RULES select * from PEPSI_AHOLD_MB.ANL_RULE_ENGINE_META_RULES;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_RULE_SET select * from PEPSI_AHOLD_MB.ANL_RULE_ENGINE_RULE_SET;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_RULES select * from PEPSI_AHOLD_MB.ANL_RULE_ENGINE_RULES;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_SUB_LEVEL_FILTER select * from PEPSI_AHOLD_MB.ANL_RULE_ENGINE_SUB_LEVEL_FILTER;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_UPC_STORE_LIST select * from PEPSI_AHOLD_MB.ANL_RULE_ENGINE_UPC_STORE_LIST;
insert into OSA_AHOLD_BEN.OLAP_ITEM select * from PEPSI_AHOLD_MB.OLAP_ITEM;
insert into OSA_AHOLD_BEN.OLAP_ITEM_OSM select * from PEPSI_AHOLD_MB.OLAP_ITEM_OSM;
insert into OSA_AHOLD_BEN.OLAP_STORE select * from PEPSI_AHOLD_MB.OLAP_STORE;
insert into OSA_AHOLD_BEN.SPD_FACT_PIVOT select * from PEPSI_AHOLD_MB.SPD_FACT_PIVOT;


ANL_RULE_ENGINE_STAGE_RULE_LIST
DIM_HUB.ANL_DIM_OSM_INTERVENTIONCLASSIFICATION
ANL_META_RAW_ALERTS_SEQ

--app
RSI_DIM_VENDOR
RSI_DIM_RETAILER
RSI_DIM_SILO
RSI_CONFIG_CUSTOMER

--below tables need vendor_key
anl_rule_engine_stage_rule_list
anl_rule_engine_* 

*/


DROP SCHEMA if exists OSA_AHOLD_BEN cascade;
CREATE SCHEMA IF NOT EXISTS OSA_AHOLD_BEN;

CREATE TABLE IF NOT EXISTS OSA_AHOLD_BEN.ANL_DIM_FEEDBACK_ASSUMPTIONS
    (
     Merchandiser varchar(64) NOT NULL  ENCODING RLE,
     FeedbackDesc varchar(255) NOT NULL  ENCODING RLE,
     AcceptAdjustment char(1) NULL  ENCODING RLE,
     EffectiveFeedback char(1) NULL  ENCODING RLE,
     FeedbackRank char(1) NULL  ENCODING RLE,
     FeedbackSubRank char(1) NULL  ENCODING RLE,
     HasFeedback char(1) NULL  ENCODING RLE,
     TrueAlert char(1) NULL  ENCODING RLE,
     NotOnPlanogram char(1) NULL ENCODING RLE,
    alertTypeList varchar(512) ENCODING RLE,
    PaybackPeriod integer NULL ENCODING RLE,
    InterventionCost Numeric(9,2) NULL ENCODING RLE,
    SplitInterventionCost char(1) NULL ENCODING RLE,
    PRIMARY KEY("Merchandiser", "FeedbackDesc")
    ) 
ORDER BY Merchandiser, FeedbackDesc
SEGMENTED BY hash(Merchandiser, FeedbackDesc) ALL NODES;


CREATE TABLE IF NOT EXISTS OSA_AHOLD_BEN.ANL_FACT_FEEDBACK
(
    EVENT_KEY                  INT NOT NULL ENCODING RLE,
    RETAILER_KEY               INT NOT NULL ENCODING RLE,
    VENDOR_KEY                 INT NOT NULL ENCODING RLE,
    STORE_VISIT_DATE          DATETIME NULL ENCODING RLE,
    PERIOD_KEY                 INT NOT NULL ENCODING RLE,
    TYPE                       varchar(1) NOT NULL ENCODING RLE,
    TYPE_DATE                  varchar(10) ENCODING RLE,
    ALERT_ID                   INT NULL ENCODING RLE,
    ALERT_TYPE                 varchar(64) NULL ENCODING RLE,
    MERCHANDISER_STORE_NUMBER  varchar(512) NULL ENCODING RLE,
    STORE_ID                   varchar(512) NULL ENCODING RLE,
    MERCHANDISER_UPC           varchar(512) NULL ENCODING RLE,
    INNER_UPC                  varchar(512) NULL ENCODING RLE,
    MERCHANDISER               varchar(100) NULL ENCODING RLE,
    STORE_REP                  varchar(1000) NULL ENCODING RLE,
    SOURCE                     varchar(1000) NULL ENCODING RLE,
    BEGIN_STATUS               varchar(255) NULL ENCODING RLE,
    ACTION                     varchar(255) NULL ENCODING RLE,
    FEEDBACK_DESCRIPTION       varchar(255) NULL ENCODING RLE,
    FEEDBACK_HOTLINEREPORTDATE DATETIME NULL ENCODING RLE,
    FEEDBACK_ISININVENTORY     varchar(5)  NULL ENCODING RLE,
    UPC_STATUS varchar(10) NULL ENCODING RLE,
    MSI varchar(255) NULL ENCODING RLE
) ORDER BY EVENT_KEY, PERIOD_KEY, RETAILER_KEY, VENDOR_KEY 
SEGMENTED BY hash(ALERT_ID) ALL NODES
--PARTITION BY PERIOD_KEY//100;
PARTITION BY VENDOR_KEY;


CREATE TABLE IF NOT EXISTS OSA_AHOLD_BEN.ANL_DIM_OSM_INTERVENTIONCLASSIFICATION
(
    InterventionKey int NOT NULL ENCODING RLE,
    InterventionDesc1 varchar(50) NOT NULL ENCODING RLE,
    InterventionDesc2 varchar(150) ENCODING RLE,
    SupplierInterventionId varchar(50) ENCODING RLE,
    MerchandizeQuestion varchar(250) ENCODING RLE,
    Application varchar(50) ENCODING RLE,
    InterventionTemplate varchar(100) ENCODING RLE,
    AlertType varchar(100) ENCODING RLE,
    AlertTypeDesc varchar(200) ENCODING RLE,
    AlertSubType varchar(100) ENCODING RLE,
    AlertIntegerType int ENCODING RLE,
    RankCalculation varchar(100) ENCODING RLE,
	PRIMARY KEY (InterventionKey)
) ORDER BY InterventionKey
UNSEGMENTED ALL NODES;


CREATE TABLE IF NOT EXISTS OSA_AHOLD_BEN.ANL_RULE_ENGINE_META_DATA_PROVIDERS
(
  DATA_PROVIDER_NAME VARCHAR (512) NOT NULL PRIMARY KEY ENCODING RLE,
  PROVIDER_BASE_TABLE_NAME VARCHAR (512) NOT NULL ENCODING RLE,
  PROVIDER_PRE_PROCESSING_SP VARCHAR (512) NULL ENCODING RLE,
  PROVIDER_BASE_TABLE_REJECT_REASON_COLUMN VARCHAR (512) NOT NULL DEFAULT 'RejectReaons' ENCODING RLE,
  PROVIDER_BASE_TABLE_OWNER_COLUMN VARCHAR (512) NOT NULL DEFAULT 'OWNER' ENCODING RLE,
  PROVIDER_BASE_TABLE_PK_COLUMN VARCHAR (512) NOT NULL ENCODING RLE,
  PROVIDER_POST_PROCESSING_SP VARCHAR(512) NULL ENCODING RLE
) UNSEGMENTED all nodes;


CREATE TABLE if not exists OSA_AHOLD_BEN.ANL_RULE_ENGINE_META_PROVIDERS
(
  PROVIDER_NAME VARCHAR (512)  NOT NULL PRIMARY KEY ENCODING RLE,
  PROVIDER_PRE_PROCESSING_SP VARCHAR(512) NULL ENCODING RLE,
  PROVIDER_POST_PROCESSING_SP VARCHAR(512) NULL ENCODING RLE
) UNSEGMENTED all nodes;


CREATE TABLE if not exists OSA_AHOLD_BEN.ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER
(
  PROVIDER_NAME VARCHAR (512)  NOT NULL  ENCODING RLE, 
  SILO_TYPE CHAR (1) NOT NULL ENCODING RLE,
  SILO_PROCESSING_ORDER INT  NOT NULL ENCODING RLE,
  primary key (PROVIDER_NAME,SILO_TYPE)
) UNSEGMENTED all nodes;


CREATE TABLE if not exists OSA_AHOLD_BEN.ANL_RULE_ENGINE_META_RULES
( 
  RULE_ID INT NOT NULL PRIMARY KEY ENCODING RLE,
  PROVIDER_NAME VARCHAR (512)  NOT NULL  ENCODING RLE, 
  PROVIDER_SUB_TYPE VARCHAR (512)  ENCODING RLE, 
  FILTER_NAME VARCHAR (512)  NOT NULL ENCODING RLE,
  METRICS_NAME VARCHAR (512)  NOT NULL  ENCODING RLE,
  METRICS_CONDITION VARCHAR (65000)  ENCODING RLE,
  METRICS_TYPE VARCHAR (512)  NOT NULL  ENCODING RLE, 
  METRICS_VALUE_TYPE VARCHAR (512)  NOT NULL  ENCODING RLE, 
  METRICS_ORDER INT  NOT NULL  ENCODING RLE, 
  METRICS_REJECT_REASON VARCHAR (512)  NOT NULL  ENCODING RLE, 
  DEPEND_ON_PREVIOUS_RULE CHAR (1) ENCODING RLE,
  RULE_HINTS VARCHAR(512) NOT NULL ENCODING RLE,
  SUB_LEVEL_FILTERS varchar(100) NULL ENCODING RLE,
  RULE_ACTION_TYPE VARCHAR (512)  NOT NULL  ENCODING RLE-- filter out rule or pick up rule
) UNSEGMENTED all nodes;


CREATE TABLE if not exists OSA_AHOLD_BEN.ANL_RULE_ENGINE_RULE_SET
(
  RULE_SET_ID INT NOT NULL ENCODING RLE,-- primary key,
  RULE_SET_NAME	VARCHAR(512) NOT NULL ENCODING RLE,
  ENGINE_PROVIDER_NAME	VARCHAR(512) NOT NULL ENCODING RLE,
  DATA_PROVIDER_NAME	VARCHAR(512) NOT NULL ENCODING RLE,
  OWNER VARCHAR(512) NOT NULL ENCODING RLE,
  SILO_ID VARCHAR(512) NOT NULL ENCODING RLE, --KEY
  ITEM_SCOPE VARCHAR(512) NULL ENCODING RLE,
  STORE_SCOPE VARCHAR(512) NULL ENCODING RLE,	
  TYPES_LIST VARCHAR(512) NULL ENCODING RLE,
  ENABLED CHAR(1) NOT NULL ENCODING RLE,
  CREATED_BY	VARCHAR(512)  NULL ENCODING RLE,
  CREATED_DATE	TIMESTAMP  NULL ENCODING RLE,
  UPDATED_BY	VARCHAR(512) ENCODING RLE,
  UPDATED_DATE	TIMESTAMP ENCODING RLE,
  primary key (RULE_SET_ID,SILO_ID)
) UNSEGMENTED all nodes;


CREATE TABLE if not exists OSA_AHOLD_BEN.ANL_RULE_ENGINE_RULES
(
  RULE_ID int NOT NULL ENCODING RLE,
  RULE_SET_ID int NOT NULL ENCODING RLE,
  SUB_LEVEL_METRICS varchar(512) NULL ENCODING RLE,
  PARAMETER1 varchar(65000) NULL ENCODING RLE,
  PARAMETER2 varchar(65000) NULL ENCODING RLE,
  PARAMETER3 varchar(65000) NULL ENCODING RLE,
  ENABLED char(1) NOT NULL DEFAULT 'T' ENCODING RLE,
  CREATED_BY varchar(512)  NULL ENCODING RLE,
  CREATED_DATE TIMESTAMP  NULL ENCODING RLE,
  UPDATED_BY varchar(512) NULL ENCODING RLE,
  UPDATED_DATE TIMESTAMP NULL ENCODING RLE,
  primary key (RULE_ID,	RULE_SET_ID)
) UNSEGMENTED all nodes;


CREATE TABLE if not exists OSA_AHOLD_BEN.ANL_RULE_ENGINE_SUB_LEVEL_FILTER
(
  RULE_ID	INT NOT NULL ENCODING RLE,
  RULE_SET_ID	INT NOT NULL ENCODING RLE,
  SUB_LEVEL_VALUE VARCHAR(512) NULL ENCODING RLE,
  METRICS_VALUE	VARCHAR(512) NULL ENCODING RLE,
  PARAMETER2	VARCHAR(512) NULL ENCODING RLE,
  PARAMETER3	VARCHAR(512) NULL ENCODING RLE,
  CREATED_BY	VARCHAR(512)  NULL ENCODING RLE,
  CREATED_DATE	TIMESTAMP  NULL ENCODING RLE,
  UPDATED_BY	VARCHAR(512) ENCODING RLE,
  UPDATED_DATE	TIMESTAMP ENCODING RLE,
  SUB_LEVEL_CATEGORY VARCHAR(512) NULL ENCODING RLE,
  primary key (RULE_ID,RULE_SET_ID,SUB_LEVEL_VALUE)
) UNSEGMENTED all nodes;


CREATE TABLE if not exists OSA_AHOLD_BEN.ANL_RULE_ENGINE_UPC_STORE_LIST
(
  FILE_ID	INT NOT NULL ENCODING RLE,
  UPC		VARCHAR(512) NOT NULL ENCODING RLE,
  STOREID	VARCHAR(512) NOT NULL ENCODING RLE
) UNSEGMENTED all nodes;


CREATE TABLE if not exists OSA_AHOLD_BEN.ANL_RULE_ENGINE_STAGE_RULE_LIST
(
  FILE_ID	VARCHAR(512) NOT NULL ENCODING RLE,
  ATTRIBUTE_NAME	VARCHAR(512) NOT NULL ENCODING RLE,
  VALUE	VARCHAR(512) NOT NULL ENCODING RLE
) UNSEGMENTED all nodes;


CREATE TABLE OSA_AHOLD_BEN.ANL_META_RAW_ALERTS_SEQ(
	VENDOR_KEY int NOT NULL ENCODING RLE,
	RETAILER_KEY int NOT NULL ENCODING RLE,
	SEQ_NUM int NOT NULL ENCODING RLE,
	ALERT_DAY int not NULL ENCODING RLE,
    PRIMARY KEY (VENDOR_KEY, RETAILER_KEY)
)ORDER BY RETAILER_KEY
UNSEGMENTED ALL NODES;


CREATE TABLE if not exists OSA_AHOLD_BEN.OLAP_ITEM_OSM
(
    VENDOR_KEY int NOT NULL,
    ITEM_KEY int NOT NULL,
    UPC varchar(512),
    VENDOR_PACK_QTY varchar(30),
    ITEM_GROUP varchar(512),
    ITEM_DESCRIPTION varchar(512),
    OSM_ITEM_NBR varchar(512),
    OSM_ITEM_STATUS varchar(512),
    OSM_ITEM_TYPE varchar(512),
    OSM_WHSE_PACK_QTY varchar(512),
    OSM_MAJOR_CATEGORY_NO varchar(512),
    OSM_MAJOR_CATEGORY varchar(512),
    OSM_CATEGORY varchar(512),
    OSM_SUB_CATEGORY_NO varchar(512),
    OSM_SUB_CATEGORY varchar(512),
    OSM_BRAND varchar(512),
    OSM_UNIT_PRICE float,
    OSM_VENDOR_STK_NBR varchar(512),
    OSM_VENDOR_PACK_COST varchar(512),
    OSM_WHSE_PACK_COST varchar(512),
	PRIMARY KEY(ITEM_KEY)
) ORDER BY ITEM_KEY
UNSEGMENTED ALL NODES;


CREATE TABLE if not exists OSA_AHOLD_BEN.OLAP_STORE
(
    RETAILER_KEY int NOT NULL,
    STORE_KEY int NOT NULL,
    STORE_ID varchar(512),
    RETAILER_NAME varchar(512),
    PRIME_DC varchar(512),
    COMP_OPEN_DATE varchar(512),
    COMP_CLOSE_DATE varchar(512),
    MARKET_CLUSTER varchar(512),
    STATE varchar(512),
    CITY varchar(512),
    ZIP varchar(512),
    MAP_REGION varchar(512),
    ZIP_PREFIX varchar(512),
    RSI_COUNTRY varchar(512),
    RSI_RETAILER_DUNS varchar(512),
    COMP_STORE varchar(1),
    DC_ST_MAP_LOC varchar(512),
    SAR_AGENT_NAME varchar(512),
    SAR_STORE_NAME varchar(512),
    SAR_AGENCY_STORE varchar(512),
    AD_BRK_DAY varchar(512),
    ADDRESS1 varchar(512),
    AHLD_CLUSTER varchar(512),
    AHLD_NON_TRADITIONAL varchar(512),
    BANNER_NAME varchar(512),
    BANNER_NBR varchar(512),
    COMPANY_NBR varchar(512),
    DISTRICT varchar(512),
    DISTRICT_NBR varchar(512),
    MAIN_LOCATION_NBR varchar(512),
    MARKET varchar(512),
    PEAPOD_FLAG varchar(512),
    PEP_ASM_DISTRICT varchar(512),
    PEP_ASM_REGION varchar(512),
    PEP_ASM_TERRITORY varchar(512),
    PEP_PWS_DOS varchar(512),
    PEP_PWS_RSM varchar(512),
    PEP_PWS_SR_DOS varchar(512),
    PEP_STORE_TYPE varchar(512),
    PEPUS_FLNA_KAM varchar(512),
    PEPUS_PEPSI_KAM varchar(512),
    RDM_CHAINLVL1 varchar(512),
    RDM_CHAINLVL2 varchar(512),
    RDM_CHAINLVL3 varchar(512),
    RDM_CHAINLVL4 varchar(512),
    RDM_CHAINLVL5 varchar(512),
    RDM_CHAINLVL6 varchar(512),
    RDM_CUSTOMERNBR varchar(512),
    RDM_FLNADIST varchar(512),
    RDM_FLNADIV varchar(512),
    RDM_FLNAREG varchar(512),
    RDM_FLNARTENBR varchar(512),
    RDM_FLNAZONE varchar(512),
    RDM_PEPBTLRCITY varchar(512),
    RDM_PEPBTLRCOUNTY varchar(512),
    RDM_PEPBTLRSTATE varchar(512),
    RDM_PEPDIVNAME varchar(512),
    RDM_PEPGMANAME varchar(512),
    RDM_PEPMUNAME varchar(512),
    RDM_PEPMUNBR varchar(512),
    RDM_PEPOWNERNAME varchar(512),
    RDM_RETCHNLTYPE varchar(512),
    RDM_STORENBR varchar(512),
    RDM_TDLINXNBR varchar(512),
    REGION varchar(512),
    RSI_AFRICAN_AMERICAN varchar(512),
    RSI_ASIAN varchar(512),
    RSI_CAUCASIAN varchar(512),
    RSI_HISPANIC varchar(512),
    RSI_MEDIAN_AGE varchar(512),
    RSI_MEDIAN_HH_SIZE varchar(512),
    RSI_MEDIAN_INCOME varchar(512),
    RSI_METRO_AREA_CODE varchar(512),
    RSI_METRO_AREA_DESC varchar(512),
    STORE_TYPE varchar(512),
    STR_CLOSE_DATE varchar(512),
    STR_OPEN_DATE varchar(512),
    STR_SQ_FT varchar(512),
    WEATHERSTN_USAF varchar(512),
    WEATHERSTN_WBAN varchar(512),
	PRIMARY KEY(STORE_KEY)
) ORDER BY STORE_KEY
UNSEGMENTED ALL NODES;


CREATE SEQUENCE OSA_AHOLD_BEN.IncidentID_Seq  CACHE 10000  MINVALUE 55000000000 ;

CREATE TABLE IF NOT EXISTS OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS (
	"Vendor_key" int NOT NULL ENCODING RLE,
	"Retailer_Key" int NOT NULL ENCODING RLE,
	"ItemNumber" varchar(512) NOT NULL ENCODING RLE,
	"Store_Key" int NOT NULL ENCODING RLE,
	"Period_Key" int NOT NULL ENCODING RLE,
	"InterventionKey" INT NOT NULL ENCODING RLE,
	"FirstPublishDate" TIMESTAMP NOT NULL ENCODING RLE,
	"LastPublishDate" TIMESTAMP NOT NULL ENCODING RLE,
	"MerchandizerAccessibleValue" int NULL ENCODING RLE,
	"IncidentDetails" varchar(250) NULL ENCODING RLE,
	"ClosureDate" TIMESTAMP NULL ENCODING RLE,
	"ClosureType" INT NOT NULL ENCODING RLE,
	"AppReportedStatus" INT NULL ENCODING RLE,
	"AppDetectedClosureDate" TIMESTAMP NULL ENCODING RLE,
	"ResponseDate" TIMESTAMP NULL ENCODING RLE,
	"ResponseId" int NOT NULL ENCODING RLE,
	"IssuanceId" int NOT NULL ENCODING RLE,
	"IncidentID" INTEGER DEFAULT NEXTVAL('OSA_AHOLD_BEN.IncidentID_Seq') ENCODING RLE, 
	"ResponseComments" varchar(300) NULL ENCODING RLE,
	"RSInventory" int NULL ENCODING RLE,
	"RSPI" int NULL ENCODING RLE,
	"RetailOnHandAverage" int NULL ENCODING RLE,
	"RSInventoryAverage" int NULL ENCODING RLE,
	"RetailOnHand" int NULL ENCODING RLE,
	"DaysSincePOS" int NULL ENCODING RLE,
	"RSPIIsMax" int NULL ENCODING RLE,
	"LostSalesToDate" float NULL ENCODING RLE,
	"LostUnitsToDate" float NULL ENCODING RLE,
	"ConditionLength" int NULL ENCODING RLE,
	"ZeroScanDays" int NULL ENCODING RLE,
	"SalesRate13weeks" float NULL ENCODING RLE,
	"SalesLastWeek" float NULL ENCODING RLE,
	"MaxDaysZeroStock" int NULL ENCODING RLE,
	"AvgRSDemand" float NULL ENCODING RLE,
	"AvgPrice" float NULL ENCODING RLE,
	"ITEM_KEY" int NULL ENCODING RLE,
	"SUBVENDOR_ID_KEY" int NULL ENCODING RLE,
	"ExpectedLostSalesUnits" float NULL ENCODING RLE,
	"ExpectedLostSalesAmount" float NULL ENCODING RLE,
	"LastPOSScanPeriod" int NULL ENCODING RLE,
	"UPC" varchar(512) NULL ENCODING RLE,
	"STOREID" varchar(512) NULL ENCODING RLE,
	"ITEM_DESCRIPTION" varchar(512) NULL ENCODING RLE,
	"MAJOR_CATEGORY_NO" varchar(20) NULL ENCODING RLE,
	"MAJOR_CATEGORY" varchar(100) NULL ENCODING RLE,
	"CATEGORY" varchar(100) NULL ENCODING RLE,
	"SUB_CATEGORY" varchar(100) NULL ENCODING RLE,
	"BRAND" varchar(100) NULL ENCODING RLE,
	"UNIT_PRICE" float NULL ENCODING RLE,
	"VENDOR_PACK_QTY" varchar(30) NULL ENCODING RLE,
	"WHSE_PACK_QTY" varchar(30) NULL ENCODING RLE,
	"STATE" varchar(512) NULL ENCODING RLE,
	"REGION" varchar(512) NULL ENCODING RLE,
	"DISTRICT" varchar(100) NULL ENCODING RLE,
	"Owner" varchar (64) NULL ENCODING RLE,
	"RejectReasons" varchar (512) NULL ENCODING RLE,
	"ClassificationCode" int NULL ENCODING RLE,
	"GapProbability" float NULL ENCODING RLE,
	"AvgPOSPerDay" float NULL ENCODING RLE,
	"DaysSincePositiveRetailAdj" int NULL ENCODING RLE,
	"DaysSinceNegativeRetailAdj" int NULL ENCODING RLE,
	"DaysSincePositiveUnexplainedAdj" int NULL ENCODING RLE,
	"DaysSinceShrink" int NULL ENCODING RLE,
	"DaysSinceGS" int NULL ENCODING RLE,
	"CurrInvErr" int NULL ENCODING RLE,
	"ConditionSales" int NULL ENCODING RLE,
	"ConditionDays" int NULL ENCODING RLE,
	"DefaultSales" int NULL ENCODING RLE,
	"DefaultDays" int NULL ENCODING RLE,
	"DefaultConditionRate" float NULL ENCODING RLE,
	"SupplyChainConditionRate" float NULL ENCODING RLE,
	"MaxAdjustment" int NULL ENCODING RLE,
	"MinAdjustment" int NULL ENCODING RLE,
	"MaxUnexpAdjustment" int NULL ENCODING RLE,
	"MinUnexpAdjustment" int NULL ENCODING RLE,
	"TVFlip" int NULL ENCODING RLE,
	"DSD_Ind" INT NULL ENCODING RLE,
	"RatesofSalesChange" float NULL ENCODING RLE,
	"RSDaysOH" float NULL ENCODING RLE,
	"POG_Ind" INT NULL ENCODING RLE,
	"AlertUPC" varchar(512) NULL ENCODING RLE,
	"PROMO_Ind" INT NULL ENCODING RLE,
	"ShelfCapacity" int NULL ENCODING RLE,
	"CasePack" int NULL ENCODING RLE,
	"ProjectedWeeklySalesGain" float NULL ENCODING RLE,
	"AlertLostUnitsToDate" float NULL ENCODING RLE,
	"AlertLostSalesToDate" float NULL ENCODING RLE,
	"POSUNITS" int NULL ENCODING RLE,
	"POSSALES" numeric(9, 2) NULL ENCODING RLE,
	"Inactive Distribution Point Lost Sales Volume Units" numeric(9, 2) NULL ENCODING RLE,
	"Lost Distribution Point Lost Sales Volume Units" numeric(9, 2) NULL ENCODING RLE,
	"Zero Sales Day Lost Sales Volume Units" numeric(9, 2) NULL ENCODING RLE,
	"Short Term Offsale Lost Sales Volume Units" numeric(9, 2) NULL ENCODING RLE,
	"Regular Selling Price" numeric(9, 2) NULL ENCODING RLE,
	"Expected Sales Volume Units" numeric(9, 2) NULL ENCODING RLE,
	"Zero Sales Day Lost Sales Amount"  numeric(9, 2) NULL ENCODING RLE,
	"Short Term Offsale Lost Sales Amount" numeric(9, 2) NULL ENCODING RLE,
	"Lost Distribution Point Lost Sales Amount" numeric(9, 2) NULL ENCODING RLE,
	"Inactive Distribution Point Lost Sales Amount" numeric(9, 2) NULL ENCODING RLE,
	"Total Availability Lost Sales Amount" numeric(9, 2) NULL ENCODING RLE,
	"Total Availability Lost Sales Volume Units" numeric(9, 2) NULL ENCODING RLE,
	"Total Distribution Lost Sales Amount" numeric(9, 2) NULL ENCODING RLE,
	"Total Distribution Lost Sales Volume Units" numeric(9, 2) NULL ENCODING RLE,
	"Total Lost Sales Amount" numeric(9, 2) NULL ENCODING RLE,
	"Total Lost Sales Volume Units" numeric(9, 2) NULL ENCODING RLE,
	"AlertLostUnitsToDate2" float NULL ENCODING RLE,
	"PromoType" int NULL ENCODING RLE,
	"RegularSalesRate" float NULL ENCODING RLE,
	"PromoSalesRate" float NULL ENCODING RLE,
	"DaysSincePromo" int NULL ENCODING RLE,
	"AlertItemNumber" varchar(512) NULL ENCODING RLE,
	"Attributes" varchar(40000) NULL ENCODING RLE,
	"Alert0DayAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert1DayAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert2DaysAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert3DaysAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert4DaysAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert5DaysAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert6DaysAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert7DaysAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert8DaysAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert9DaysAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert10DaysAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert11DaysAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert12DaysAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"Alert13DaysAgoStatus" VARCHAR(32) NULL ENCODING RLE,
	"RANK_VALUE" float null ENCODING RLE,
    Item_Store varchar(1027),
    Whse_Nbr varchar(512),
    OOS_Status varchar(50),
    Max_Shelf_Qty int,
    NWeeks_Pos_Qty numeric(14,4),
    NWeeks_Base_Index int,
    POS_Demand_Weekly numeric(14,4),
    Curr_Str_On_Order_Qty int,
    Curr_Str_In_Transit_Qty int,
    Curr_Str_In_Whse_Qty int,
    Curr_Whse_On_Hand_Qty int,
    Total_Pipeline int,
    WOS_Target_On_Hand_Qty numeric(14,4),
    Retailer_DD numeric(14,4),
    RS_Demand_Weekly numeric(14,4),
    WOS_Target_On_Hand_Cases numeric(14,4),
    WOS numeric(14,4),
    Suggested_Order_Whse_PACK_Qty numeric(14,4),
    Restricted_Order_Qty_Whse_Pk numeric(14,4),
    Whse_Pks_to_Add numeric(14,4),
    FinalOrder numeric(14,4),
    Current_Str_DD numeric(14,4),
    New_Suggested_DD numeric(14,4),
    LN_Wks_Historical_DD numeric(14,4),
    Recommended_Action varchar(1),
    New_Unit_Chg numeric(14,4),
    New_Chg_Percentage numeric(14,4),
    InventoryRequiredInd varchar(3),
	PRIMARY KEY(VENDOR_KEY,RETAILER_KEY,Period_Key,itemNumber,STORE_KEY),
	UNIQUE (IncidentID)
)
ORDER BY VENDOR_KEY,RETAILER_KEY,Period_Key,itemNumber,STORE_KEY,InterventionKey
SEGMENTED BY MODULARHASH(Period_Key,ITEM_KEY,STORE_KEY,InterventionKey) ALL NODES
--PARTITION BY Period_Key//100;
PARTITION BY VENDOR_KEY;



--only used for some specific rules
CREATE TABLE if not exists OSA_AHOLD_BEN.SPD_FACT_PIVOT
(
    RETAILER_KEY int NOT NULL,
    VENDOR_KEY int NOT NULL,
    ITEM_KEY int NOT NULL,
    STORE_KEY int NOT NULL,
    PERIOD_KEY int NOT NULL,
    SUBVENDOR_ID_KEY int NOT NULL,
    DC_KEY int NOT NULL,
    DC_RETAILER_KEY int NOT NULL,
    "Total Sales Amount" numeric(14,4),
    "Total Sales Volume Units" numeric(14,4),
    "Store On Hand Volume Units" numeric(14,4),
    "Promoted Sales Amount" numeric(14,4),
    "Promoted Sales Volume Units" numeric(14,4),
    "Regular Sales Amount" numeric(14,4),
    "Regular Sales Volume Units" numeric(14,4),
    "Store Ordered Volume Units" numeric(14,4),
    "Store Ordered Volume Cases" numeric(14,4),
    "Store Receipts Volume Units" numeric(14,4),
    "Store Receipts Volume Cases" numeric(14,4),
    "Mature Store Indicator" int DEFAULT 0,
    "Mature Sales Volume Units" numeric(14,4),
    "ASC Distribution Flag" numeric(14,4),
    "CAO Order Flag" numeric(14,4),
    "Cost Retail Authorized Flag" numeric(14,4),
    "Count of Store PI Adjustments" numeric(14,4),
    "CS Distribution Flag" numeric(14,4),
    "Days Remaining to Future NPI Start Date" numeric(14,4),
    "DSD Store Receipts Amount" numeric(14,4),
    "DSD Store Receipts Amount at Retail" numeric(14,4),
    "DSD Store Receipts Promoted Amount" numeric(14,4),
    "DSD Store Receipts Promoted Amount at Retail" numeric(14,4),
    "DSD Store Receipts Promoted Volume Cases" numeric(14,4),
    "DSD Store Receipts Promoted Volume Units" numeric(14,4),
    "DSD Store Receipts Turn Amount" numeric(14,4),
    "DSD Store Receipts Turn Amount at Retail" numeric(14,4),
    "DSD Store Receipts Turn Volume Cases" numeric(14,4),
    "DSD Store Receipts Turn Volume Units" numeric(14,4),
    "DSD Store Receipts Volume Cases" numeric(14,4),
    "DSD Store Receipts Volume Units" numeric(14,4),
    "DSD Store Return Amount" numeric(14,4),
    "DSD Store Return Amount at Retail" numeric(14,4),
    "DSD Store Return Volume Units" numeric(14,4),
    DSD_IND numeric(14,4),
    "Future Disc Ind" numeric(14,4),
    "Future NPI Ind" numeric(14,4),
    "Future NPI Start Date" numeric(14,4),
    "Future NPI Store Shelf Capacity Volume Units" numeric(14,4),
    "Last Store Shelf Count Date" numeric(14,4),
    "Net Cost Per Unit" numeric(14,4),
    NPI_Start_Period_Key numeric(14,4),
    "Other Adjustment Amount" numeric(14,4),
    "Other Adjustment Volume Units" numeric(14,4),
    "POG Authorized Flag" numeric(14,4),
    "POG Commodity Flag" numeric(14,4),
    POG_IND numeric(14,4),
    "Promoted Sales Weight" numeric(14,4),
    "Promotion Calendar Indicator" numeric(14,4),
    "Reclaim Amount" numeric(14,4),
    "Reclaim Volume Units" numeric(14,4),
    "Regular Sales Volume Cases" numeric(14,4),
    "Regular Sales Weight" numeric(14,4),
    "Retailer Store Forecast Volume Units" numeric(14,4),
    "Run Down Days Remaining Disc" numeric(14,4),
    "Shipper Component Store Forceout Amount" numeric(14,4),
    "Shipper Component Store Forceout Volume Cases" numeric(14,4),
    "Shipper Component Store Forceout Volume Units" numeric(14,4),
    "Shipper Component Store Ordered Amount" numeric(14,4),
    "Shipper Component Store Ordered Promoted Amount" numeric(14,4),
    "Shipper Component Store Ordered Promoted Volume Cases" numeric(14,4),
    "Shipper Component Store Ordered Promoted Volume Units" numeric(14,4),
    "Shipper Component Store Ordered Turn Amount" numeric(14,4),
    "Shipper Component Store Ordered Turn Volume Cases" numeric(14,4),
    "Shipper Component Store Ordered Turn Volume Units" numeric(14,4),
    "Shipper Component Store Ordered Volume Cases" numeric(14,4),
    "Shipper Component Store Ordered Volume Units" numeric(14,4),
    "Shipper Component Store Receipts Amount" numeric(14,4),
    "Shipper Component Store Receipts Regular Order Amount" numeric(14,4),
    "Shipper Component Store Receipts Regular Order Volume Cases" numeric(14,4),
    "Shipper Component Store Receipts Regular Order Volume Units" numeric(14,4),
    "Shipper Component Store Receipts Volume Cases" numeric(14,4),
    "Shipper Component Store Receipts Volume Units" numeric(14,4),
    "Shipper Component Store Receipts Warehouse Generated Amount" numeric(14,4),
    "Shipper Component Store Receipts Warehouse Generated Volume Cases" numeric(14,4),
    "Shipper Component Store Receipts Warehouse Generated Volume Units" numeric(14,4),
    "Shipper Component Store Received Promoted Amount" numeric(14,4),
    "Shipper Component Store Received Promoted Volume Cases" numeric(14,4),
    "Shipper Component Store Received Promoted Volume Units" numeric(14,4),
    "Shipper Component Store Received Turn Amount" numeric(14,4),
    "Shipper Component Store Received Turn Volume Cases" numeric(14,4),
    "Shipper Component Store Received Turn Volume Units" numeric(14,4),
    "Shipper Store Ordered Amount" numeric(14,4),
    "Shipper Store Ordered Volume Cases" numeric(14,4),
    "Shipper Store Receipts Amount" numeric(14,4),
    "Shipper Store Receipts Volume Cases" numeric(14,4),
    "Store BRI Amount" numeric(14,4),
    "Store BRI Volume Units" numeric(14,4),
    "Store Forceout Amount" numeric(14,4),
    "Store Forceout Amount Regular" numeric(14,4),
    "Store Forceout Volume Cases" numeric(14,4),
    "Store Forceout Volume Cases Regular" numeric(14,4),
    "Store Forceout Volume Units" numeric(14,4),
    "Store Forceout Volume Units Regular" numeric(14,4),
    "Store Mezzanine On Hand Amount" numeric(14,4),
    "Store Mezzanine On Hand Volume Units" numeric(14,4),
    "Store On Hand Amount" numeric(14,4),
    "Store On Hand Volume Cases" numeric(14,4),
    "Store Ordered Amount" numeric(14,4),
    "Store Ordered Amount Turn Regular" numeric(14,4),
    "Store Ordered Promoted Amount" numeric(14,4),
    "Store Ordered Promoted Amount Regular" numeric(14,4),
    "Store Ordered Promoted Volume Cases" numeric(14,4),
    "Store Ordered Promoted Volume Cases Regular" numeric(14,4),
    "Store Ordered Promoted Volume Units" numeric(14,4),
    "Store Ordered Promoted Volume Units Regular" numeric(14,4),
    "Store Ordered Turn Amount" numeric(14,4),
    "Store Ordered Turn Amount Regular" numeric(14,4),
    "Store Ordered Turn Volume Cases" numeric(14,4),
    "Store Ordered Turn Volume Cases Regular" numeric(14,4),
    "Store Ordered Turn Volume Units" numeric(14,4),
    "Store Ordered Turn Volume Units Regular" numeric(14,4),
    "Store Receipts Amount" numeric(14,4),
    "Store Receipts Regular Order Amount" numeric(14,4),
    "Store Receipts Regular Order Amount Regular" numeric(14,4),
    "Store Receipts Regular Order Volume Cases" numeric(14,4),
    "Store Receipts Regular Order Volume Cases Regular" numeric(14,4),
    "Store Receipts Regular Order Volume Units" numeric(14,4),
    "Store Receipts Regular Order Volume Units Regular" numeric(14,4),
    "Store Receipts Warehouse Generated Amount" numeric(14,4),
    "Store Receipts Warehouse Generated Amount Regular" numeric(14,4),
    "Store Receipts Warehouse Generated Volume Cases" numeric(14,4),
    "Store Receipts Warehouse Generated Volume Cases Regular" numeric(14,4),
    "Store Receipts Warehouse Generated Volume Units" numeric(14,4),
    "Store Receipts Warehouse Generated Volume Units Regular" numeric(14,4),
    "Store Received Promoted Amount" numeric(14,4),
    "Store Received Promoted Amount Regular" numeric(14,4),
    "Store Received Promoted Volume Cases" numeric(14,4),
    "Store Received Promoted Volume Cases Regular" numeric(14,4),
    "Store Received Promoted Volume Units" numeric(14,4),
    "Store Received Promoted Volume Units Regular" numeric(14,4),
    "Store Received Turn Amount" numeric(14,4),
    "Store Received Turn Amount Regular" numeric(14,4),
    "Store Received Turn Volume Cases" numeric(14,4),
    "Store Received Turn Volume Cases Regular" numeric(14,4),
    "Store Received Turn Volume Units" numeric(14,4),
    "Store Received Turn Volume Units Regular" numeric(14,4),
    "Store Secondary Shelf Capacity Amount" numeric(14,4),
    "Store Secondary Shelf Capacity Volume Units" numeric(14,4),
    "Store Shelf Capacity Amount" numeric(14,4),
    "Store Shelf Capacity Volume Units" numeric(14,4),
    "Store Shelf On Hand Amount" numeric(14,4),
    "Store Shelf On Hand Volume Units" numeric(14,4),
    "Suppression Indicator" numeric(14,4),
    "Swell Allowance Amount" numeric(14,4),
    "Swell Amount" numeric(14,4),
    "Swell Volume Units" numeric(14,4),
    "Target Exit Date" numeric(14,4),
    "Total Linear Footage" numeric(14,4),
    "Total Sales Volume Cases" numeric(14,4),
    "Total Sales Weight" numeric(14,4),
    "TPR Ad Type" numeric(14,4),
    "TPR Ad Type Price Reduction" numeric(14,4),
    "TPR Ad Type Unit Value" numeric(14,4),
    "Transition Item Indicator" numeric(14,4),
    "Upcoming Weekly Ad Ind" numeric(14,4),
    "Weekly Ad Ind" numeric(14,4),
    "WIC Flag" numeric(14,4),
    DSD_IND_KEY int,
    FUTURE_NPI_IND int,
    FUTURE_DISC_IND int,
    NPI_IND int,
    NPI_PERIOD_KEY int,
    "Grace Flag" int,
    SCAN_DATE int,
    S2S numeric(14,4),
    "S2S Indicator" int,
    "NPI and Scanned Ind" int,
    FUTURE_NPI_IND_KEY int,
    FUTURE_DISC_IND_KEY int,
	PRIMARY KEY (RETAILER_KEY, VENDOR_KEY, ITEM_KEY, STORE_KEY, PERIOD_KEY, SUBVENDOR_ID_KEY)
)
 ORDER BY ITEM_KEY, STORE_KEY, SUBVENDOR_ID_KEY, PERIOD_KEY, RETAILER_KEY, VENDOR_KEY
SEGMENTED BY hash(ITEM_KEY, STORE_KEY) ALL NODES
--PARTITION BY ((PERIOD_KEY // 100));
PARTITION BY VENDOR_KEY;


CREATE VIEW OSA_AHOLD_BEN.ANL_STORE_SALES AS
 SELECT fact.VENDOR_KEY,
        fact.RETAILER_KEY,
        fact.ITEM_KEY,
        fact.STORE_KEY,
        fact.PERIOD_KEY,
        fact.SUBVENDOR_ID_KEY,
        fact.DC_KEY,
        fact.DC_RETAILER_KEY,
        fact."Store On Hand Volume Units" AS "On Hand",
        fact."Total Sales Amount" AS "Total Sales",
        fact."Total Sales Volume Units" AS "Total Units",
        fact."Store Receipts Volume Units" AS "Store Receipts",
        fact."Store Ordered Volume Units" AS "Ordered Quantity",
        fact."Promoted Sales Amount" AS "Promo Sales",
        fact."Promoted Sales Volume Units" AS "Promo Units",
        NULL AS "Damaged and Defective",
        CASE WHEN (fact.POG_IND IS NULL) THEN NULL::int ELSE CASE WHEN (fact.DSD_IND > 0::numeric(18,0)) THEN CASE WHEN (fact.POG_IND > 0::numeric(18,0)) THEN 1 ELSE 0 END ELSE (CASE WHEN (fact.POG_IND > 0::numeric(18,0)) THEN 1 ELSE 0 END & CASE WHEN (fact."CAO Order Flag" > 0::numeric(18,0)) THEN 1 ELSE 0 END) END END AS Pog_Ind,
        CASE WHEN (fact."Promotion Calendar Indicator" IS NULL) THEN NULL::int ELSE CASE WHEN (fact."Promotion Calendar Indicator" > 0::numeric(18,0)) THEN 1 ELSE 0 END END AS PROMO_Ind,
        fact."Regular Sales Amount" AS "Regular Sales",
        fact."Regular Sales Volume Units" AS "Regular Units",
        CASE WHEN (((fact."TPR Ad Type" = 0::numeric(18,0)) OR (fact."TPR Ad Type" IS NULL)) AND ((fact."Weekly Ad Ind" = 0::numeric(18,0)) OR (fact."Weekly Ad Ind" IS NULL))) THEN 0 WHEN ((fact."Weekly Ad Ind" = 1::numeric(18,0)) AND ((fact."TPR Ad Type" = 0::numeric(18,0)) OR (fact."TPR Ad Type" IS NULL))) THEN 1 WHEN ((fact."Weekly Ad Ind" = 1::numeric(18,0)) AND (fact."TPR Ad Type" = 1::numeric(18,0))) THEN 2 WHEN ((fact."Weekly Ad Ind" = 1::numeric(18,0)) AND (fact."TPR Ad Type" = 3::numeric(18,0))) THEN 3 WHEN ((fact."Weekly Ad Ind" = 1::numeric(18,0)) AND (fact."TPR Ad Type" = 4::numeric(18,0))) THEN 4 WHEN ((fact."Weekly Ad Ind" = 1::numeric(18,0)) AND (fact."TPR Ad Type" = 5::numeric(18,0))) THEN 5 WHEN ((fact."Weekly Ad Ind" = 1::numeric(18,0)) AND (fact."TPR Ad Type" = 8::numeric(18,0))) THEN 6 WHEN ((fact."Weekly Ad Ind" = 1::numeric(18,0)) AND (fact."TPR Ad Type" = 9::numeric(18,0))) THEN 7 WHEN ((fact."Weekly Ad Ind" = 1::numeric(18,0)) AND (fact."TPR Ad Type" = 14::numeric(18,0))) THEN 8 WHEN (((fact."Weekly Ad Ind" = 0::numeric(18,0)) OR (fact."Weekly Ad Ind" IS NULL)) AND (fact."TPR Ad Type" = 1::numeric(18,0))) THEN 9 WHEN (((fact."Weekly Ad Ind" = 0::numeric(18,0)) OR (fact."Weekly Ad Ind" IS NULL)) AND (fact."TPR Ad Type" = 3::numeric(18,0))) THEN 10 WHEN (((fact."Weekly Ad Ind" = 0::numeric(18,0)) OR (fact."Weekly Ad Ind" IS NULL)) AND (fact."TPR Ad Type" = 4::numeric(18,0))) THEN 11 WHEN (((fact."Weekly Ad Ind" = 0::numeric(18,0)) OR (fact."Weekly Ad Ind" IS NULL)) AND (fact."TPR Ad Type" = 5::numeric(18,0))) THEN 12 WHEN (((fact."Weekly Ad Ind" = 0::numeric(18,0)) OR (fact."Weekly Ad Ind" IS NULL)) AND (fact."TPR Ad Type" = 8::numeric(18,0))) THEN 13 WHEN (((fact."Weekly Ad Ind" = 0::numeric(18,0)) OR (fact."Weekly Ad Ind" IS NULL)) AND (fact."TPR Ad Type" = 9::numeric(18,0))) THEN 14 WHEN (((fact."Weekly Ad Ind" = 0::numeric(18,0)) OR (fact."Weekly Ad Ind" IS NULL)) AND (fact."TPR Ad Type" = 14::numeric(18,0))) THEN 15 ELSE NULL::int END AS Promo_Type,
        fact."TPR Ad Type Price Reduction" AS PROMO_PARAM1,
        fact."TPR Ad Type Unit Value" AS PROMO_PARAM2,
        NULL::int AS Adjustments
FROM OSA_AHOLD_BEN.SPD_FACT_PIVOT fact;

 

 
CREATE TABLE if not exists OSA_AHOLD_BEN.OLAP_ITEM
(
    VENDOR_KEY int NOT NULL,
    ITEM_KEY int NOT NULL,
    UPC varchar(512),
    VENDOR_NAME varchar(512),
    CORPORATE_GROUP varchar(512),
    VENDOR_PACK_QTY numeric(14,4),
    DEFAULT_STORE_PACK_QTY numeric(14,4),
    VENDOR_NBR varchar(512),
    ITEM_GROUP varchar(512),
    ITEM_DESCRIPTION varchar(512),
    NEW_ITEM varchar(512),
    ITEM_EXIT_DATE date,
    ITEM_EXIT_DATE_NAME varchar(512),
    EQUIVALENT_UNIT numeric(14,4),
    EQUIVALENT_UOM varchar(512),
    AVG_CODE_LIFE_REMAINING varchar(512),
    ITEM_COST numeric(14,4),
    RSI_PRODUCT_LIFECYCLE varchar(512),
    RSI_PROMO_FAMILY varchar(512),
    RSI_PROMO_FAMILY_UPC varchar(512),
    RSI_CASE_UPC varchar(512),
    RSI_ITEM_SIZE varchar(512),
    RSI_SUPPLIER_PACK_QTY varchar(512),
    RSI_WHSE_PACK_QTY varchar(512),
    RSI_SHIP_TO_DC varchar(512),
    RSI_VENDOR_DUNS varchar(512),
    DC_ST_MAP_PROD varchar(512),
    SAR_ITEM_NUM varchar(512),
    SAR_CASE_PACK varchar(512),
    SAR_KEY_ITEM varchar(512),
    SAR_CATEGORY varchar(512),
    SAR_ITEM_DESC varchar(512),
    RSI_PRODUCT_ID_13 varchar(512),
    CASE_CONVERSION char(1),
    AHLD_BONUS_IND varchar(512),
    AHLD_BONUS_ITEM_LINK varchar(512),
    AHLD_CASE_PACK varchar(512),
    AHLD_CAT_ID varchar(512),
    AHLD_CAT_NAME varchar(512),
    AHLD_CONFLICT_ITEM varchar(512),
    AHLD_DEPT_ID varchar(512),
    AHLD_DEPT_NAME varchar(512),
    AHLD_DSD_WH_CNFLCT varchar(512),
    AHLD_DSD_WH_FLAG varchar(512),
    AHLD_INTERNAL_ITEM varchar(512),
    AHLD_ITEM_DESC varchar(512),
    AHLD_ITEM_SIZE varchar(512),
    AHLD_MASTER_ITEM varchar(512),
    AHLD_MASTER_NO_TRANS varchar(512),
    AHLD_PROMO_FAM varchar(512),
    AHLD_PRTFOLIO_NAME varchar(512),
    AHLD_PRVT_LABEL varchar(512),
    AHLD_SEGMENT_ID varchar(512),
    AHLD_SEGMENT_NAME varchar(512),
    AHLD_SUBCAT_ID varchar(512),
    AHLD_SUBCAT_NAME varchar(512),
    AHLD_SUBDEPT_ID varchar(512),
    AHLD_SUBDEPT_NAME varchar(512),
    AHLD_SUBSTITUTE_ITEM varchar(512),
    AHLD_TRANS_FROM varchar(512),
    AHLD_TRANS_TO varchar(512),
    AHLD_UOM varchar(512),
    AHLD_UPC_STATUS varchar(512),
    AHLD_WEIGHTED_FLG varchar(512),
    AHLD_WHS_ITEM varchar(512),
    NEW_ITEM_FLAG varchar(512),
    PEPUS_BSP varchar(512),
    PEPUS_VENDOR_PACK_QTY varchar(512),
    PEPUS_VENDOR_PCK_CONF varchar(512),
    RAHLD_CASE_GTIN varchar(512),
    RAHLD_COMMON_PACK varchar(512),
    RAHLD_GTIN varchar(512),
    RAHLD_ITEMDESC varchar(512),
    RAHLD_OZ_PR_UNIT varchar(512),
    RAHLD_UNITS_PR_PK varchar(512),
    RAHLDG_CMN_PK_NM varchar(512),
    RAHLDG_CONSUNINPK varchar(512),
    RAHLDG_ITEMDESC varchar(512),
    RAHLDG_NET_CONTENT varchar(512),
    RAHLDG_NETCNT_UOM varchar(512),
    RAHLDG_PCK_TYPE varchar(512),
    RAHLDG_POSDESC varchar(512),
    RAHLDG_UNITSINPCK varchar(512),
    RESET_CYCLE varchar(512),
    RUPC_BRAND varchar(512),
    RUPC_CAT varchar(512),
    RUPC_COMPANY varchar(512),
    RUPC_COMPANY5 varchar(512),
    RUPC_FLNA_BDC varchar(512),
    RUPC_FLNA_CLASS varchar(512),
    RUPC_FLNA_FLAVOR varchar(512),
    RUPC_FLNA_SIZE varchar(512),
    RUPC_FOODORBEV varchar(512),
    RUPC_LINE_EXT varchar(512),
    RUPC_MACROCAT varchar(512),
    RUPC_PEPSIORCOMP varchar(512),
    RUPC_PROMOTIONGROUP varchar(512),
    RUPC_SUBCAT varchar(512),
    RUPC_TRADEMARK varchar(512),
    RUPCG_BRAND varchar(512),
    RUPCG_PCKTYPECDE varchar(512),
    RUPCG_PROD_GRP varchar(512),
    RUPCG_SHLF_LIFE varchar(512),
    RUPCG_SUB_BRAND varchar(512),
    SHIPPER_FLAG varchar(512),
    UPC_A varchar(512),
	PRIMARY KEY(ITEM_KEY)
) ORDER BY ITEM_KEY
UNSEGMENTED ALL NODES;

 
/*
CREATE TABLE OSA_AHOLD_BEN.RSI_DIM_VENDOR(
	VENDOR_KEY int NOT NULL ENCODING RLE,
	VENDOR_NAME varchar(120) NOT NULL ENCODING RLE,
	VENDOR_SNAME varchar(20) NULL ENCODING RLE,
	ACTIVE char(1) NULL ENCODING RLE,
	IS_CATEGORY char(1) NULL ENCODING RLE,
	IS_DUMMY char(1) NULL ENCODING RLE,
    PRIMARY KEY (VENDOR_KEY)
)ORDER BY VENDOR_KEY
UNSEGMENTED ALL NODES;


CREATE TABLE OSA_AHOLD_BEN.RSI_DIM_RETAILER(
	RETAILER_KEY int NOT NULL ENCODING RLE,
	RETAILER_NAME varchar(120) NOT NULL ENCODING RLE,
	RETAILER_SNAME varchar(20) NOT NULL ENCODING RLE,
	ACTIVE char(1) NULL ENCODING RLE,
	IS_WHSE char(1) NULL ENCODING RLE,
	IS_DUMMY char(1) NULL ENCODING RLE,
    PRIMARY KEY (RETAILER_KEY)
)ORDER BY RETAILER_KEY
UNSEGMENTED ALL NODES;
*/
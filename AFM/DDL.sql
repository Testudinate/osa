/*
--All tables needed for AFM
--App DB
ANL_RULE_ENGINE_META_DATA_PROVIDERS
ANL_RULE_ENGINE_META_PROVIDERS
ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER
ANL_RULE_ENGINE_META_RULES
ANL_RULE_ENGINE_RULE_SET
ANL_RULE_ENGINE_RULES
ANL_RULE_ENGINE_SUB_LEVEL_FILTER
ANL_RULE_ENGINE_UPC_STORE_LIST

insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_META_DATA_PROVIDERS select * from HUB_FUNCTION_BETA.dbo.ANL_RULE_ENGINE_META_DATA_PROVIDERS;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_META_PROVIDERS select * from HUB_FUNCTION_BETA.dbo.ANL_RULE_ENGINE_META_PROVIDERS;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER select * from HUB_FUNCTION_BETA.dbo.ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_META_RULES select * from HUB_FUNCTION_BETA.dbo.ANL_RULE_ENGINE_META_RULES;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_RULE_SET select * from HUB_FUNCTION_BETA.dbo.ANL_RULE_ENGINE_RULE_SET;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_RULES select * from HUB_FUNCTION_BETA.dbo.ANL_RULE_ENGINE_RULES;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_SUB_LEVEL_FILTER select * from HUB_FUNCTION_BETA.dbo.ANL_RULE_ENGINE_SUB_LEVEL_FILTER;
insert into OSA_AHOLD_BEN.ANL_RULE_ENGINE_UPC_STORE_LIST select * from HUB_FUNCTION_BETA.dbo.ANL_RULE_ENGINE_UPC_STORE_LIST;
--app
RSI_DIM_VENDOR
RSI_DIM_RETAILER
RSI_DIM_SILO
RSI_CONFIG_CUSTOMER

--DW
ANL_DIM_FEEDBACK_ASSUMPTIONS
ANL_FACT_FEEDBACK
ANL_FACT_OSM_INCIDENTS
OLAP_ITEM
OLAP_ITEM_OSM
OLAP_STORE
SPD_FACT_PIVOT
ANL_STORE_SALES

insert into OSA_AHOLD_BEN.ANL_DIM_FEEDBACK_ASSUMPTIONS select * from PEPSI_AHOLD_MB.ANL_DIM_FEEDBACK_ASSUMPTIONS;
insert into OSA_AHOLD_BEN.ANL_FACT_FEEDBACK select * from PEPSI_AHOLD_MB.ANL_FACT_FEEDBACK;
insert into OSA_AHOLD_BEN.ANL_FACT_OSM_INCIDENTS select * from PEPSI_AHOLD_MB.ANL_FACT_OSM_INCIDENTS;
insert into OSA_AHOLD_BEN.OLAP_ITEM select * from PEPSI_AHOLD_MB.OLAP_ITEM;
insert into OSA_AHOLD_BEN.OLAP_ITEM_OSM select * from PEPSI_AHOLD_MB.OLAP_ITEM_OSM;
insert into OSA_AHOLD_BEN.OLAP_STORE select * from PEPSI_AHOLD_MB.OLAP_STORE;
insert into OSA_AHOLD_BEN.SPD_FACT_PIVOT select * from PEPSI_AHOLD_MB.SPD_FACT_PIVOT;

ANL_RULE_ENGINE_STAGE_RULE_LIST
DIM_HUB.ANL_DIM_OSM_INTERVENTIONCLASSIFICATION
ANL_META_RAW_ALERTS_SEQ

*/

---Tables in APP Server----
--no need vendor_key, for all vendors. 
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ANL_RULE_ENGINE_META_DATA_PROVIDERS]') AND type in (N'U'))
CREATE TABLE [dbo].[ANL_RULE_ENGINE_META_DATA_PROVIDERS]
(
   [DATA_PROVIDER_NAME] NVARCHAR (512) NOT NULL PRIMARY KEY,
   [PROVIDER_BASE_TABLE_NAME] NVARCHAR (512) NOT NULL,
   [PROVIDER_PRE_PROCESSING_SP] NVARCHAR (512) NULL,
   [PROVIDER_BASE_TABLE_REJECT_REASON_COLUMN] NVARCHAR (512) NOT NULL DEFAULT 'RejectReaons',
   [PROVIDER_BASE_TABLE_OWNER_COLUMN] NVARCHAR (512) NOT NULL DEFAULT 'OWNER',
   [PROVIDER_BASE_TABLE_PK_COLUMN] NVARCHAR (512) NOT NULL,
   [PROVIDER_POST_PROCESSING_SP] NVARCHAR(512) NULL
)
GO

--no need vendor_key, for all vendors. 
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ANL_RULE_ENGINE_META_PROVIDERS]') AND type in (N'U'))
CREATE TABLE [dbo].[ANL_RULE_ENGINE_META_PROVIDERS]
(
   [PROVIDER_NAME] NVARCHAR (512)  NOT NULL PRIMARY KEY,
   [PROVIDER_PRE_PROCESSING_SP] NVARCHAR(512) NULL,
   [PROVIDER_POST_PROCESSING_SP] NVARCHAR(512) NULL
)
GO

--no need anymore
--IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER]') AND type in (N'U'))
--CREATE TABLE [dbo].[ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER]
--( 
--     [PROVIDER_NAME] NVARCHAR (512)  NOT NULL , 
--     [SILO_TYPE] CHAR (1) NOT NULL,
--     [SILO_PROCESSING_ORDER] INT  NOT NULL,
--     CONSTRAINT [PK_ANL_RULE_ENGINE_META_RULE_SET_SILO_TYPE_ORDER] PRIMARY KEY CLUSTERED 
--	(
--		[PROVIDER_NAME] ASC,
--		[SILO_TYPE] ASC
--	)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY] 
--) 
--GO


--a meta table to store V-R Owner mapping and owner priority. used to process owner in order
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ANL_META_VR_OWNER_MAPPING]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[ANL_META_VR_OWNER_MAPPING](
       [VENDOR_KEY] int NOT NULL,
       [RETAILER_KEY] int NOT NULL,
       [OWNER] NVARCHAR(512) NOT NULL,
       [PRIORITY] int
    )
END
go

--no need vendor_key, for all vendors. 
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ANL_RULE_ENGINE_META_RULES]') AND type in (N'U'))
CREATE TABLE [dbo].[ANL_RULE_ENGINE_META_RULES]
( 
  [RULE_ID] INT NOT NULL PRIMARY KEY,
  [PROVIDER_NAME] NVARCHAR (512)  NOT NULL , 
  [PROVIDER_SUB_TYPE] NVARCHAR (512) , 
  [FILTER_NAME] NVARCHAR (512)  NOT NULL,
  [METRICS_NAME] NVARCHAR (512)  NOT NULL ,
  [METRICS_CONDITION] NVARCHAR (MAX) ,
  [PARAMETER1] NVARCHAR(512),
  [PARAMETER2] NVARCHAR(512),
  [PARAMETER3] NVARCHAR(512),
  [METRICS_TYPE] NVARCHAR (512)  NOT NULL , 
  [METRICS_VALUE_TYPE] NVARCHAR (512)  NOT NULL , 
  [METRICS_ORDER] INT  NOT NULL , 
  [METRICS_REJECT_REASON] NVARCHAR (512)  NOT NULL , 
  [DEPEND_ON_PREVIOUS_RULE] CHAR (1),
  [RULE_HINTS] NVARCHAR(512) NOT NULL,
  [SUB_LEVEL_FILTERS] NVARCHAR(100) NULL,
  [RULE_ACTION_TYPE] NVARCHAR(512)  NOT NULL, -- filter out rule or pick up rule
  [RULE_DESCRIPTION] NVARCHAR(MAX)  NOT NULL
) 
GO


--added vendor_key
--RULE_SET_ID should be unique for different vendors
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ANL_RULE_ENGINE_RULE_SET]') AND type in (N'U'))
CREATE TABLE [dbo].[ANL_RULE_ENGINE_RULE_SET]
(
    [VENDOR_KEY] int NOT NULL,	-- primary key
    [RETAILER_KEY] int NOT NULL,	-- primary key
    [RULE_SET_ID] INT NOT NULL IDENTITY,-- primary key,
    [RULE_SET_NAME]	NVARCHAR(512) NOT NULL,
    [ENGINE_PROVIDER_NAME] NVARCHAR(512) NOT NULL,
    [DATA_PROVIDER_NAME] NVARCHAR(512) NOT NULL,
    [OWNER] NVARCHAR(512) NOT NULL,
--  [SILO_ID] NVARCHAR(512) NOT NULL, --KEY
    [ITEM_SCOPE] NVARCHAR(512) NULL,
    [STORE_SCOPE] NVARCHAR(512) NULL,	
    [TYPES_LIST] NVARCHAR(512) NULL,
    [ENABLED] CHAR(1) NOT NULL,
    [CREATED_BY] NVARCHAR(512) NOT NULL,
    [CREATED_DATE] DATETIME NULL,
    [UPDATED_BY] NVARCHAR(512),
    [UPDATED_DATE] DATETIME,
    CONSTRAINT [PK_ANL_RULE_ENGINE_RULE_SET] PRIMARY KEY CLUSTERED
    (
        [VENDOR_KEY] ASC,
        [RETAILER_KEY] ASC,
        [RULE_SET_ID] ASC
    )WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY] 
)
GO


--no need vendor_key, can trace based on RULE_SET_ID
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ANL_RULE_ENGINE_RULES]') AND type in (N'U'))
CREATE TABLE [dbo].[ANL_RULE_ENGINE_RULES](
    [RULE_ID] [int] NOT NULL,
    [RULE_SET_ID] [int] NOT NULL,
    [SUB_LEVEL_METRICS] [nvarchar](512) NULL,
    [PARAMETER1] [nvarchar](max) NULL,
    [PARAMETER2] [nvarchar](max) NULL,
    [PARAMETER3] [nvarchar](max) NULL,
    [ENABLED] [char](1) NOT NULL DEFAULT 'T',
    [CREATED_BY] [nvarchar](512) NOT NULL,
    [CREATED_DATE] [datetime] NULL,
    [UPDATED_BY] [nvarchar](512) NULL,
    [UPDATED_DATE] [datetime] NULL,
 CONSTRAINT [PK_ANL_RULE_ENGINE_RULES] PRIMARY KEY CLUSTERED 
( [RULE_ID] ASC, [RULE_SET_ID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
)
GO


--no need vendor_key, can trace based on RULE_SET_ID
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ANL_RULE_ENGINE_SUB_LEVEL_FILTER]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[ANL_RULE_ENGINE_SUB_LEVEL_FILTER]
    (
        [RULE_ID]	INT NOT NULL,
        [RULE_SET_ID]	INT NOT NULL,
        [SUB_LEVEL_VALUE] NVARCHAR(512) NULL,
        [METRICS_VALUE]	NVARCHAR(512) NULL,
        [PARAMETER2]	NVARCHAR(512) NULL,
        [PARAMETER3]	NVARCHAR(512) NULL,
        [CREATED_BY]	NVARCHAR(512) NOT NULL,
        [CREATED_DATE]	DATETIME NULL,
        [UPDATED_BY]	NVARCHAR(512),
        [UPDATED_DATE]	DATETIME,
        [SUB_LEVEL_CATEGORY] NVARCHAR(512) NULL
    )
    CREATE UNIQUE CLUSTERED INDEX UX_ANL_RULE_ENGINE_SUB_LEVEL_FILTER on ANL_RULE_ENGINE_SUB_LEVEL_FILTER (RULE_ID,RULE_SET_ID,SUB_LEVEL_VALUE)
END
GO


--to be confirmed. probably need to add vendor_key
--(FILE_ID = ANL_RULE_ENGINE_RULES.parameter1(ruleid: 27, 28, 29) & ANL_RULE_ENGINE_RULE_SET.ITEM_SCOPE/STORE_SCOPE)
--adding rule_set_id??   UPC might unique for all vendors, but STOREID is not? So need to add RULE_SET_ID(provided that RULE_SET_ID only associated 1 VENDOR, Checking table ANL_RULE_ENGINE_RULE_SET).
--File_id is unique, but we still need to add vendor-key and retailer_key to re-fresh this table when sync data from SqlServer to Vertica
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ANL_RULE_ENGINE_UPC_STORE_LIST]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[ANL_RULE_ENGINE_UPC_STORE_LIST]
    (
        --[VENDOR_KEY] int NOT NULL,	-- primary key
        --[RETAILER_KEY] int NOT NULL,	-- primary key
        [FILE_ID] INT NOT NULL,
        [RULE_SET_ID] INT NOT NULL,
        [UPC]     NVARCHAR(512) NOT NULL,
        [STOREID] NVARCHAR(512) NOT NULL
    )
    CREATE CLUSTERED INDEX [IX_ANL_RULE_ENGINE_UPC_STORE_LIST] ON [dbo].[ANL_RULE_ENGINE_UPC_STORE_LIST] 
    ( [FILE_ID] ASC
    )
END
GO

--store all SEQ_NUM with flag, this can be used to control the mutex.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ANL_META_RAW_ALERTS_SEQ]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[ANL_META_RAW_ALERTS_SEQ](
        [VENDOR_KEY] int NOT NULL,
        [RETAILER_KEY] int NOT NULL,
        [SEQ_NUM] int NOT NULL,
        [ALERT_DAY] int not NULL,
        [CREATED_DATE] datetime NULL, --when this entry created
        [UPDATED_DATE] datetime NULL, --when this entry updated
        [STATUS] NVARCHAR(512) NOT NULL --New, inProgressing, Completed
    )
    CREATE CLUSTERED INDEX [IX_ANL_ANL_META_RAW_ALERTS_SEQ] ON [dbo].[ANL_META_RAW_ALERTS_SEQ] 
    ( [VENDOR_KEY] ASC, [RETAILER_KEY] ASC
    )
end
go



IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[RSI_DIM_VENDOR]') AND type in (N'U'))
CREATE TABLE [dbo].[RSI_DIM_VENDOR](
	[VENDOR_KEY] [int] NOT NULL,
	[VENDOR_NAME] [nvarchar](60) NOT NULL,
	[VENDOR_SNAME] [nvarchar](10) NULL,
	[CALENDAR_KEY] [int] NULL,
	[FUSION_VENDOR_NAME] [nvarchar](60) NOT NULL,
	[FUSION_VENDOR_SNAME] [nvarchar](10) NOT NULL,
	[ACTIVE] [char](1) default N'F' NULL,
	[IS_CATEGORY] [bit] default 0 NULL,
	[IS_DUMMY] [bit] default 0 NULL,
 CONSTRAINT [PK_RSI_DIM_VENDOR] PRIMARY KEY CLUSTERED 
(
	[VENDOR_KEY] ASC
)
) ON [PRIMARY]

GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[RSI_DIM_RETAILER]') AND type in (N'U'))
CREATE TABLE [dbo].[RSI_DIM_RETAILER](
	[RETAILER_KEY] [int] NOT NULL,
	[RETAILER_NAME] [nvarchar](60) NOT NULL,
	[RETAILER_SNAME] [nvarchar](10) NOT NULL,
	[FUSION_RETAILER_NAME] [nvarchar](60) NOT NULL,
	[FUSION_RETAILER_SNAME] [nvarchar](10) NOT NULL,
	[ACTIVE] [char](1) default N'F' NULL,
	[FACT_CURRENCY] [nvarchar](10) default N'$' NULL,
	[IS_WHSE] [bit] default 0 NULL,
	[IS_DUMMY] [bit] default 0 NULL,
 CONSTRAINT [PK_RSI_DIM_RETAILER] PRIMARY KEY CLUSTERED 
(
	[RETAILER_KEY] ASC
)
) ON [PRIMARY]

GO



----Tables in DW Server----
--OSA_AHOLD_BEN;
DROP SCHEMA IF EXISTS OSA_AHOLD_BEN CASCADE;
CREATE SCHEMA IF NOT EXISTS OSA_AHOLD_BEN;

--used to store synced data of table ANL_RULE_ENGINE_UPC_STORE_LIST from APP server 
CREATE TABLE IF NOT EXISTS OSA_AHOLD_BEN.ANL_RULE_ENGINE_STAGE_RULE_LIST
(
    FILE_ID	VARCHAR(512) NOT NULL ENCODING RLE,
    ATTRIBUTE_NAME	VARCHAR(512) NOT NULL ENCODING RLE,
    VALUE	VARCHAR(512) NOT NULL ENCODING RLE
) UNSEGMENTED all nodes;


--no need vendor_key, for all vendors. 
CREATE TABLE IF NOT EXISTS OSA_AHOLD_BEN.ANL_DIM_FEEDBACK_ASSUMPTIONS
(
    Merchandiser varchar(64) NOT NULL ENCODING RLE,
    FeedbackDesc varchar(255) NOT NULL ENCODING RLE,
    AcceptAdjustment char(1) ENCODING RLE,
    EffectiveFeedback char(1) ENCODING RLE,
    FeedbackRank char(1) ENCODING RLE,
    FeedbackSubRank char(1) ENCODING RLE,
    HasFeedback char(1) ENCODING RLE,
    TrueAlert char(1) ENCODING RLE,
    NotOnPlanogram char(1) ENCODING RLE,
    alertTypeList varchar(512) ENCODING RLE,
    PaybackPeriod int ENCODING RLE,
    InterventionCost numeric(9,2) ENCODING RLE,
    SplitInterventionCost char(1) ENCODING RLE,
    PRIMARY KEY (Merchandiser, FeedbackDesc)
) ORDER BY Merchandiser, FeedbackDesc
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


--no need vendor_key, for all vendors. 
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


CREATE TABLE IF NOT EXISTS OSA_AHOLD_BEN.OLAP_ITEM_OSM
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


CREATE TABLE IF NOT EXISTS OSA_AHOLD_BEN.OLAP_STORE
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


CREATE SEQUENCE IF NOT EXISTS OSA_AHOLD_BEN.IncidentID_Seq  CACHE 10000  MINVALUE 55000000000 ;

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
	PRIMARY KEY(VENDOR_KEY,RETAILER_KEY,Period_Key,itemNumber,STORE_KEY),
	UNIQUE (IncidentID)
)
ORDER BY VENDOR_KEY,RETAILER_KEY,Period_Key,itemNumber,STORE_KEY,InterventionKey
SEGMENTED BY MODULARHASH(Period_Key,ITEM_KEY,STORE_KEY,InterventionKey) ALL NODES
--PARTITION BY Period_Key//100;
PARTITION BY VENDOR_KEY;


CREATE TABLE IF NOT EXISTS OSA_AHOLD_BEN.ANL_FACT_ALERT
(
	"SEQ_NUM" int NOT NULL ENCODING RLE,
	"VENDOR_KEY" int NOT NULL ENCODING RLE,
	"RETAILER_KEY" int NOT NULL ENCODING RLE,
	"ITEM_KEY" int NOT NULL ENCODING RLE,
	"ALERT_ID" INT NOT NULL ENCODING RLE,
	"STORE_KEY" int NOT NULL ENCODING RLE,
	"PERIOD_KEY" int NOT NULL ENCODING RLE,
	"InterventionKey" INT NOT NULL ENCODING RLE,
	"SUBVENDOR_ID_KEY" INT NULL ENCODING RLE,
	"FIRST_PUBLISH_DATE_PERIOD_KEY" int NOT NULL ENCODING RLE,
	"LAST_PUBLISH_DATE_PERIOD_KEY" int NOT NULL ENCODING RLE,
	"IncidentDetails" VARCHAR(250) NULL ENCODING RLE,
	"IssuanceId" int NOT NULL ENCODING RLE,
	"Owner" VARCHAR (64) NULL ENCODING RLE,
	"RejectReasons" VARCHAR(512) NULL ENCODING RLE,
	"MerchandizerAccessibleValue" int NULL ENCODING RLE,
	"RSInventory" int NULL ENCODING RLE,
	"RSPI" int NULL ENCODING RLE,
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
	"ExpectedLostSalesUnits" float NULL ENCODING RLE,
	"ExpectedLostSalesAmount" float NULL ENCODING RLE,
	"LastPOSScanPeriod"		 int NULL ENCODING RLE,
	"ClassificationCode"	 int NULL ENCODING RLE,
	"GapProbability"  float NULL ENCODING RLE,
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
	"RatesofSalesChange" FLOAT NULL ENCODING RLE,
	"RSDaysOH" FLOAT NULL ENCODING RLE,
	"POG_Ind" INT NULL ENCODING RLE,
	"AlertUPC" VARCHAR(512) NULL ENCODING RLE,
	"ShelfCapacity" int NULL ENCODING RLE,
	"PROMO_IND"  INT NULL ENCODING RLE,
	"POG_LOCATION"  VARCHAR(1024) NULL ENCODING RLE,
	"POG_DESCRIPTION" VARCHAR(1024) NULL ENCODING RLE,
	"NUMBER_OF_FACINGS" float NULL ENCODING RLE,
	"CasePack" INT ENCODING RLE,
	"ProjectedWeeklySalesGain" FLOAT NULL ENCODING RLE,
	"AlertLostUnitsToDate" FLOAT NULL ENCODING RLE,
	"AlertLostSalesToDate" FLOAT NULL ENCODING RLE,
	"Attributes" VARCHAR(40000) NULL ENCODING RLE,
	"POSUNITS" int NULL ENCODING RLE,
	"POSSALES" decimal(9, 2) NULL ENCODING RLE,
	"Inactive Distribution Point Lost Sales Volume Units" decimal(9, 2) NULL ENCODING RLE,
	"Lost Distribution Point Lost Sales Volume Units" decimal(9, 2) NULL ENCODING RLE,
	"Zero Sales Day Lost Sales Volume Units" decimal(9, 2) NULL ENCODING RLE,
	"Short Term Offsale Lost Sales Volume Units" decimal(9, 2) NULL ENCODING RLE,
	"Regular Selling Price" decimal(9, 2) NULL ENCODING RLE,
	"Expected Sales Volume Units" decimal(9, 2) NULL ENCODING RLE,
	"Zero Sales Day Lost Sales Amount"  decimal(9, 2) NULL ENCODING RLE,
	"Short Term Offsale Lost Sales Amount" decimal(9, 2) NULL ENCODING RLE,
	"Lost Distribution Point Lost Sales Amount" decimal(9, 2) NULL ENCODING RLE,
	"Inactive Distribution Point Lost Sales Amount" decimal(9, 2) NULL ENCODING RLE,
	"Total Availability Lost Sales Amount" decimal(9, 2) NULL ENCODING RLE,
	"Total Availability Lost Sales Volume Units" decimal(9, 2) NULL ENCODING RLE,
	"Total Distribution Lost Sales Amount" decimal(9, 2) NULL ENCODING RLE,
	"Total Distribution Lost Sales Volume Units" decimal(9, 2) NULL ENCODING RLE,
	"Total Lost Sales Amount" decimal(9, 2) NULL ENCODING RLE,
	"Total Lost Sales Volume Units" decimal(9, 2) NULL ENCODING RLE,
	"AlertLostUnitsToDate2" float NULL ENCODING RLE,
	"PromoType" int NULL ENCODING RLE,
	"RegularSalesRate" float NULL ENCODING RLE,
	"PromoSalesRate" float NULL ENCODING RLE,
	"DaysSincePromo" int NULL ENCODING RLE,
	AlertItemNumber VARCHAR(512) NULL ENCODING RLE,
	Alert0DayAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert1DayAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert2DaysAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert3DaysAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert4DaysAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert5DaysAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert6DaysAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert7DaysAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert8DaysAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert9DaysAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert10DaysAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert11DaysAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert12DaysAgoStatus VARCHAR(32) NULL ENCODING RLE,
	Alert13DaysAgoStatus VARCHAR(32) NULL ENCODING RLE,
	rank_value float null ENCODING RLE
) ORDER BY ITEM_KEY, STORE_KEY, SUBVENDOR_ID_KEY, PERIOD_KEY, RETAILER_KEY, VENDOR_KEY
SEGMENTED BY MODULARHASH(ITEM_KEY,STORE_KEY) ALL NODES
--PARTITION BY PERIOD_KEY//100;
PARTITION BY VENDOR_KEY;




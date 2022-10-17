WITH 
campaign_em_dtl AS (
	SELECT 	
		em.TRACKING,
		em.CAMP_INHOME,
		em.CAMP_ID,
		em.DBM_VERSION,
		em.CAMP_CREATIVE,
		em.CAMP_CREATIVE_DESC,
		em.CAMP_TEST,
		em.CAMP_CONTACT,
		em.CAMP_FILE,
		em.EPP_IND,
		em.AGE_BAND,
		em.MEASURE_BASE,
		em.CUST_ID,
		em.BACCT_NUM AS BAN,
		em.TELENO AS MSISDN,
		em.DELIVERED, 
		em.OPENED, -- Do we know what time they opened?
		em.CLICKTHROUGH,
		em.SOFTBOUNCE,
		em.HARDBOUNCE,
		em.UNSUBSCRIBE,
		em.DEVICE,
		em.OPERATINGSYSTEM 
	FROM HSM_CM_AD.CAMP_ENGAGE_EM_DTL em
 
),

order_dtl AS (
	SELECT 
		ffh_ord.CUST_ID,
		ffh_ord.BILL_ACCOUNT_NUMBER AS BAN,
		ffh_ord.PROD_INSTNC_ID,
		ffh_ord.CHANNEL_GROUP,
		ffh_ord.CHANNEL,
		ffh_ord.PRODUCT, 
		ffh_ord.PRODUCT_FAMILY,
		ffh_ord.TECH_TYPE,
		ffh_ord.PLATFORM,
		ffh_ord.ORDER_STATUS,
		ffh_ord.YIELD_STATUS,
		ffh_ord.YIELD_SUB_STATUS,
		ffh_ord.SALES_ACTIVITY_TYPE,
		ffh_ord.SALES_ACTIVITY,
		ffh_ord.PRODUCT_NAME ,
		ffh_ord.CURRENT_ORDER_STATUS,
		ffh_ord.INITIAL_CREATED_DT,
		ffh_ord.CREATED_DT,
		ffh_ord.DUE_DT,
		ffh_ord.ACTIVATION_DT,
		ffh_ord.DEACTIVATION_DT
	FROM HSM_CM.ORDER_FFH_DTL ffh_ord 
	--WHERE BILL_ACCOUNT_NUMBER  = '604169589'
)

SELECT 
	em_dtl.*,
	order_dtl.*
FROM campaign_em_dtl em_dtl
INNER JOIN order_dtl order_dtl 
ON -- I think we need MORE JOIN conditions OR Filters?
	em_dtl.CUST_ID = order_dtl.CUST_ID -- JOIN ON customer ID 
	AND order_dtl.INITIAL_CREATED_DT BETWEEN em_dtl.CAMP_INHOME AND em_dtl.CAMP_INHOME + 31 
	AND CAMP_ID='LWC' 
	AND CAMP_INHOME > TO_DATE('2022-07-01', 'YYYY-MM-DD')
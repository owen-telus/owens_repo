WITH camp_dtl AS (
  SELECT 
    em.CAMP_INHOME,
    em.CAMP_TEST,
    em.CAMP_ID,
    em.CUST_ID, 
    em.BACCT_NUM,
    em.CAMP_EMAIL,
    em.OPENED,
    em.DELIVERED,
    em.CLICKTHROUGH,
    em.UNSUBSCRIBE
  FROM HSM_CM_AD.CAMP_ENGAGE_EM_DTL em
  LEFT JOIN HSM_CM_AD.CAMP_CONTACT_FFH_EXC ffh ON
  em.SEGMENT_ID = ffh.SEGMENT_ID
  LEFT JOIN HSM_CM_AD.CAMP_CONTACT_MOB_EXC mob ON
  em.MOB_CAMPAIGN_NUM = mob.MOB_CAMPAIGN_NUM and em.MOB_LIST_NUM = mob.MOB_LIST_NUM
  WHERE 
    em.CAMP_INHOME BETWEEN TO_DATE('07/01/2022','MM/DD/YYYY') AND TO_DATE('08/31/2022','MM/DD/YYYY') -- Please always specify CAMP_INHOME to reduce size of record return 
    --AND em.CAMP_TEST = 'R' -- Use R for Targeted and C for Control groups
    AND em.CAMP_ID IN ('OWN')
    AND ffh.SEGMENT_ID IS NULL AND mob.MOB_CAMPAIGN_NUM IS NULL
    --and ROWNUM < 100 -- Please remove this line to include all records
  )
SELECT 
  camp.*, 
  ord.ORDER_ID , 
  ord.CREATED_DT , 
  ord.CUST_ID ,
  ord.BILL_ACCOUNT_NUMBER, 
  ord.FMS_ADDRESS_ID ,
  ord.PRODUCT_NAME ,
  ord.PREVIOUS_PRODUCT_NAME , 
  ord.CURRENT_ORDER_STATUS
FROM HSM_CM.ORDER_FFH_DTL ord
INNER JOIN
camp_dtl camp
ON ord.CUST_ID = camp.CUST_ID
WHERE 
ord.CREATED_DT >= TO_DATE('07/01/2022','MM/DD/YYYY') 
AND ord.CURRENT_ORDER_STATUS IN ('Installed')
--AND PRODUCT IN ( 'PIK')
AND PRODUCT IN ('TTV', 'PIK')
--and ROWNUM < 100
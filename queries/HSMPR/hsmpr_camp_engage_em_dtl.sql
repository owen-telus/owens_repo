SELECT 
  em.CAMP_INHOME,
  em.CAMP_TEST,
  em.CAMP_ID,
  em.BACCT_NUM,
  em.CAMP_EMAIL,
  em.DELIVERED,
  em.CLICKTHROUGH,
  em.UNSUBSCRIBE
FROM HSM_CM_AD.CAMP_ENGAGE_EM_DTL em

LEFT JOIN HSM_CM_AD.CAMP_CONTACT_FFH_EXC ffh ON -- What's this table for?
em.SEGMENT_ID = ffh.SEGMENT_ID

LEFT JOIN HSM_CM_AD.CAMP_CONTACT_MOB_EXC mob ON
em.MOB_CAMPAIGN_NUM = mob.MOB_CAMPAIGN_NUM and em.MOB_LIST_NUM = mob.MOB_LIST_NUM

WHERE 
  em.CAMP_INHOME BETWEEN TO_DATE('05/01/2022','MM/DD/YYYY') AND TO_DATE('05/31/2022','MM/DD/YYYY') -- Please always specify CAMP_INHOME to reduce size of record return 
  AND em.CAMP_TEST = 'R' -- Use R for Targeted and C for Control groups
  AND em.CAMP_ID IN ('TVJ')
  AND ffh.SEGMENT_ID IS NULL AND mob.MOB_CAMPAIGN_NUM IS NULL -- ?
  and ROWNUM < 100 -- Please remove this line to include all records 
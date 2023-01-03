WITH LWC_results AS (
	SELECT 
		CAMP_CREATIVE,
		PRODUCT_NAME,
		EXTRACT(MONTH FROM CREATED_DT) AS MONTH_numeric,
		to_char(CREATED_DT, 'Month') AS MONTH_
	FROM HSM_CM_AD.CAMP_ORDER_LFT_DTL  
	WHERE CAMP_ID = 'LWC'
	AND CAMP_INHOME >= TO_DATE('2022-01-01', 'YYYY-MM-DD')
	AND PRODUCT = 'LWC'
	-- CAMP_CREATIVE in the Tableau Dashboard
	AND CAMP_CREATIVE IN (	'LWC0927CG',
							'LWC0222FFHE',
							'LWC0222FFHEA',
							'LWC0222FFHE',
							'LWC0222FFHC',
							'LWCJULYSL2CG',
							'LWC0222FFHC',
							'LWCJULYSL1EU',
							'LWC0222FFHE',
							'LWC0222FFHE',
							'LWCJULYSL1EU',
							'LWC0927EU',
							'LWC0222FFHE',
							'LWC0222FFHE',
							'LWC0222FFHC',
							'LWCJULYSL1CG',
							'LWCJULYSL1CG',
							'LWC0927CG',
							'LWC0222FFHEB',
							'LWC0222FFHC',
							'LWCJULYSL2CG',
							'LWC0122FBPEEU',
							'LWC0222FFHC',
							'LWC0222FFHE',
							'LWC0222FFHC',
							'LWC0222FFHC',
							'LWC0222FFHE',
							'LWCJULYSL2EU',
							'LWC0222FFHE',
							'LWC0222FFHC',
							'LWC0222FFHC',
							'LWC0222FFHE',
							'LWC0222FFHC',
							'LWCJULYSL2EU',
							'LWC0222FFHE',
							'LWC0222FFHE',
							'LWC0222FFHC',
							'LWC0927CG',
							'LWC0222FFHE',
							'LWC0222FFHC',
							'LWCJULYSL1CG',
							'LWC0222FFHE',
							'LWCJULYSL2EU',
							'LWC0222FFHC',
							'LWC0222FFHE',
							'LWC0222FFHE',
							'LWC0122FBPEC',
							'LWC0222FFHC',
							'LWC0222FFHC',
							'LWC0222FFHC',
							'LWC0222FFHE',
							'LWC0222FFHC',
							'LWC0927EU',
							'LWC0222FFHE',
							'LWC0222FFHC',
							'LWC0222FFHC',
							'LWC0222FFHC',
							'LWC0222FFHC',
							'LWC0222FFHE',
							'LWC0927CG',
							'LWC0222FFHE',
							'LWCJULYSL2CG',
							'LWC0222FFHE',
							'LWC0222FFHE',
							'LWC0222FFHE',
							'LWC0927EU',
							'LWC0222FFHC',
							'LWC0927EU',
							'LWCJULYSL1EU',
							'LWC0222FFHC',
							'LWCautoEU',
							'LWCQ3autoCG',
							'LWCQ4OCTCG',
							'LWCQ1BPEC',
							'LWCautoEU',
							'LWCautoEU',
							'LWCQ4OCTEU',
							'LWCQ3autoCG',
							'LWCQ4OCTEU',
							'LWCQ3autoCG',
							'LWCQ4OCTCG',
							'LWCautoEU',
							'LWCQ3autoCG',
							'LWCQ3autoCG',
							'LWCQ2JENN',
							'LWCQ3autoCG',
							'LWCautoEU',
							'LWCQ1BPEEU')	
	
								
	),

LWC_agg AS (

	SELECT 
		CAMP_CREATIVE,
		MONTH_numeric,
		MONTH_,
		PRODUCT_NAME,
		COUNT(*) AS num_products_sold 
	FROM LWC_results 
	GROUP BY CAMP_CREATIVE, MONTH_numeric, MONTH_, PRODUCT_NAME
	ORDER BY CAMP_CREATIVE, MONTH_numeric

)


SELECT 
	SUM(num_products_sold) AS total_products_sold 
FROM LWC_agg 

-- Get Count of 5G+ Customers over time using product instance tables and count base on 5G+ SOC Codes
SELECT 
  prod_instnc_ts,
  COUNT(*) as num_5g_plus_users
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts > '2022-01-01'
  AND pp_bus_pp_catlg_itm_cd IN ('PPM40G90','POM50G100','PPM40G80U','POM40G85N','PPM45P90N','POM50G90N','PPM40G85P','POM50G95N','PPM40G80Q','PPM40G80N','PPM50G90Q','POM50G90','PPM15MS65','PPM25QC60','PPM25ON60','PPM25MS65','PPM45MS65','PPM45MS70','PPMQ45G70','PPMO45G70','PPM40QC75','PPMS45G75','PPM40ON75','PMUL45GQC','PSKU45G75','PPM45MS75','PMUL45GON','PULSH80','PSKUNW40G','PMUL40GPN','PMUL45GSM','PPM50G80','PPM50GQ80','PSKU50G80','POM50SKMB','POM50ON80','PPMS50GCU','PPMQ50GCU','PPMO50GCU','PSKUN50GP','PPMB40G90','PPM50G85','PSKUNW50G','PPMB50GCU','PPM40G80Q','PPM40G80N','PPM50G90Q','POM50G90','PPM15MS65','PPM25QC60','PPM25ON60','PPM25MS65','PPM45MS65','PPM45MS70','PPMQ45G70','PPMO45G70','PPM40QC75','PPMS45G75','PPM40ON75','PMUL45GQC','PSKU45G75','PPM45MS75','PMUL45GON','PULSH80','PSKUNW40G','PMUL40GPN','PMUL45GSM','PPM50G80','PPM50GQ80','PSKU50G80','POM50SKMB','POM50ON80','PPMS50GCU','PPMQ50GCU','PPMO50GCU','PSKUN50GP','PPMB40G90','PPM50G85','PSKUNW50G','PPMB50GCU','PMUL45GNP','PPM50GSK','PPM45G95P','PPM50G95','POM50GUL','POM50GUL','PUL45GB90','LPOM60G95','PULCU40G','POM40G95','LPM60G105','PMUL45G85','PMUL40GQS','LPOM50G75','LPMU60G90','PMUL45GMB','PMUL40GMB','PMUL45GSK','PMUL40GQS','LPMU60G85','LPMU60G95','PMUL60GQC','PMUL60GON','PMUL60GSM','PPM50G95N','POM50GPN','PPM40CU75','POM40N75C','PPM50G90N','POM50GPPL','PMU50G85N','PPM50CU95','PPM40G75Q','PMUL50G85') 
GROUP BY prod_instnc_ts
ORDER BY prod_instnc_ts


-- Get Count of 5G Customers over time using product instance tables and count base on 5G+ SOC Codes
SELECT 
  prod_instnc_ts,
  COUNT(*) as num_5g_users
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts > '2022-01-01'
  AND pp_bus_pp_catlg_itm_cd IN ('PPM25G85','PUL30G85','PPM20G80','PPM20G75U','PPM25G75N','PPM25G80P','PULNWS30G','PUL35NW85','PMUL20Q70','PUL20G70','PULSH20MS','PSKUNW20G','PMUL45G65','PMUL45GPP','PMUL25Q65','PMUL25G65','PMUL25GSM','PPM45G70','PPMB20G80','PPM45G75','PPM25G85','PUL30G85','PPM20G80','PPM20G75U','PPM25G75N','PPM25G80P','PULNWS30G','PUL35NW85','PMUL20Q70','PUL20G70','PULSH20MS','PSKUNW20G','PMUL45G65','PMUL45GPP','PMUL25Q65','PMUL25G65','PMUL25GSM','PPM45G70','PPMB20G80','PPM45G75','PMUL25G80','PMUL25G75','PMUL25GMB','PMUL25GSK','POM20G70N','POM25G45N','PPM30G50N','PUL20SHPN','POM30GP70','POM15GP60','PPM25G65P','PPM25ON65','PPM20G65','PPM20G65Q') 
GROUP BY prod_instnc_ts
ORDER BY prod_instnc_ts


-- Get Count of unlimited plan Customers over time using product instance tables and count base on 5G+ SOC Codes
SELECT 
  prod_instnc_ts,
  COUNT(*) as num_unlimited_plan_users
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts > '2022-01-01'
  AND pp_bus_pp_catlg_itm_cd IN ('PPM20GQ65','PPM20MS70','PPM40MS75','PPM40GQ75','PPM40MS80','PPM40GQ80','PPM40GO80','PPM60MS80','PPM60GQ80','PPM50GQCU','PPM50GOCU','POMUU100P','PPM60GQ90','PPM60GO90','PPM60MS90','PUL20GNWP','P20ULNW65','PPM20QC70','PPM40QC80','PPM40ON80','PPM20ON70','PPM10GQ60','PMUL10GQ','PPM10GO60','PPM25GQ65','PPM25G65Q','PMUL10G50','PPM25G65O','PPM20GO65','PPM25GNPQ','PULSH15GQ','PPM10GQ70','PPM30GQ65','PPM10GO70','','PULSH25GQ','PUL30SH70','PPM40GO75','PMUL15G55','PUL20NW65','PUL20G65N','PPM60GO80','PPM30G70Q','PPM30GO70','PPM25GO65','POM15G85','POM15G85N','POM15G85P','POM20G85Q','PPM25GNPO','PPM40QC70','PPM40ON70','POM20G85','P25GUL75','PPM30GON','PPM30GU60','P15GUL65','PM50GCU85','PPM30GO65','PPM50GU85','PPM40MS70','PULSH40MS','PULSH50GQ','PULU20GMS','P50GU115','PULSH50MS','PULSH15MS','PULSH75','POM25G95P','PULSH60GP','PPM40GQCU','POM25G95N','PULSH25MS','PPM40GOCU','POM25GCUN','PULU40G95','PULU30GMS','PUL25SH65','PULSH60ON','PULU40GMS','PULSH30MS','PULU60PQ','','PULU60105','PULSH6085','PUL60G105','PULSH100Q','POMCUPQP','POM50CUSP','P100UL160','POMUU135P','PULSH100','PPM20SH65','PUL15SH60','PUL30SH60','PUL30SH65','POMUULP','POM100CUP','POMCUS180','POM100CUP','POMCUS180','PUL10SH70','PPM10G75','POM15G65','PPM15G70','PPM15G75','PPM20G70','PPM20GB75','PULSH2080','PPM25G70','PPM25GB75','PPM25GB80','PPM30G70','PPM30G75','PULSH30G','PPM30G85','PPM30G90','PMUL35G80','PULSH35G','PPM40G75','PMUL40G80','PULSH40GP','PULS40G90','PULSH40G','PMUL45G90','PULSH50G','POM60GB95','POM60G100','PULSH100G','POM20GNP','POM20GBP','POM30G100','PPM30G105','PPM30G110','POM35GP','POM40G85','POM40G90','POM40GP','POM40G110','PULCU40GB','PPM50GCUS','POM50G145','POM60GCUP','PPM60G95','PPM60G100','POM60GPN','POM60G115','POM60G120','POM100GP','PUL20SH55','PPOM25G45','PPM30G50','POM20G75N','PMUL35G80','PMUL40G80','PULS40G90','PMUL45G90','PUL25G65','PUL30G65Q','PUL35G75Q','PUL25MB65','PUL30G65M','PUL35G75M','PPM25G80','PPM40G85','PPM40GMSQ','PPM40GB80','PPM20GQ65','PPM20MS70','PPM40MS75','PPM40GQ75','PPM40MS80','PPM40GQ80','PPM40GO80','PPM60MS80','PPM60GQ80','PPM50GQCU','PPM50GOCU','POMUU100P','PPM60GQ90','PPM60GO90','PPM60MS90','PMUL35G80','PMUL40G80','PULS40G90','PMUL45G90','PUL25G65','PUL30G65Q','PUL35G75Q','PUL25MB65','PUL30G65M','PUL35G75M','PPM25G80','PPM40G85','PPM40GMSQ','PPM40GB80','P2GNON75','PNO2GUN70') 
GROUP BY prod_instnc_ts
ORDER BY prod_instnc_ts
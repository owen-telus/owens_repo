-- population, population density, and area in km^2 of fsa
-- First 3 characters of postal code = fsa
-- Be aware of joining with Billing Account Information Postal Code, as the postal code in ref_table.bq_hpbi_billing_account_dim contains postal codes in the US and incorrect postal code formats

SELECT 
    fsa, 
    AVG(Population__2016) AS population, 
    AVG(population_density) AS population_density, 
    SUM(area_by_postal_code_km2) AS area
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_exttable_fsa_population_density` 
GROUP BY fsa 
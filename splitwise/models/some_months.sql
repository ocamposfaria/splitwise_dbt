{{ config(materialized='view') }}

SELECT '2023-01' AS `month` 
UNION ALL SELECT '2023-02' AS `month` 
UNION ALL SELECT '2023-03' AS `month` 
UNION ALL SELECT '2023-04' AS `month` 
UNION ALL SELECT '2023-05' AS `month` 
UNION ALL SELECT '2023-06' AS `month` 
UNION ALL SELECT '2023-07' AS `month` 
UNION ALL SELECT '2023-08' AS `month` 
UNION ALL SELECT '2023-09' AS `month` 
UNION ALL SELECT '2023-10' AS `month` 
UNION ALL SELECT '2023-11' AS `month` 
UNION ALL SELECT '2023-12' AS `month`
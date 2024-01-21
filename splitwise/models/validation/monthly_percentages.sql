{{ config(materialized='view') }}

-- percentuais até fevereiro de 2023, sem considerar VR na conta

SELECT
*,
cost_juau/(cost_juau + cost_lana) as percentage_juau,
cost_lana/(cost_juau + cost_lana) as percentage_lana

FROM (
SELECT
	month,
    - sum(cost_juau) as cost_juau,
    - sum(cost_lana) as cost_lana
    
FROM {{ref('earnings_final')}}
WHERE month < '2023-03' and ((category = 'ganhos' and name not like '%VR%') or (category = 'ganhos extra' and name = 'PA'))
GROUP BY 1
ORDER BY 1 DESC) ganhos

union all

-- percentuais a partir de março de 2023, considerando VR na conta
SELECT
*,
cost_juau/(cost_juau + cost_lana) as percentage_juau,
cost_lana/(cost_juau + cost_lana) as percentage_lana

FROM (
SELECT
	month,
    - sum(cost_juau) as cost_juau,
    - sum(cost_lana) as cost_lana
    
FROM {{ref('earnings_final')}}
WHERE month >= '2023-03' and category = 'ganhos'
GROUP BY 1
ORDER BY 1 DESC) ganhos

ORDER BY 1 DESC
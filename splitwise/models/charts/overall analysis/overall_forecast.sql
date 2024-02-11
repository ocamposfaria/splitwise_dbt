WITH

segmentar_ganhos AS (
SELECT *, substring(month, 1, 4) as year, cost_juau + cost_lana as cost_house, 'future_estimated' as future_mode FROM {{ref('overall_costs')}} WHERE `month` < substring(curdate() /* - interval 2 month*/, 1, 7)  union all
SELECT *, substring(month, 1, 4) as year, cost_juau + cost_lana as cost_house, 'future_estimated' as future_mode FROM {{ref('overall_costs_future_estimated')}} 
	union all
SELECT *, substring(month, 1, 4) as year, cost_juau + cost_lana as cost_house, 'future_planned' as future_mode FROM {{ref('overall_costs')}} WHERE `month` < substring(curdate() /* - interval 2 month*/, 1, 7) union all
SELECT *, substring(month, 1, 4) as year, cost_juau + cost_lana as cost_house, 'future_planned' as future_mode FROM {{ref('overall_costs_future_planned')}}
	union all
SELECT *, substring(month, 1, 4) as year, cost_juau + cost_lana as cost_house, 'future_limited' as future_mode FROM {{ref('overall_costs')}} WHERE `month` < substring( curdate() /* - interval 2 month*/, 1, 7) union all
SELECT *, substring(month, 1, 4) as year, cost_juau + cost_lana as cost_house, 'future_limited' as future_mode FROM {{ref('overall_costs_future_limits')}}
),

overall_forecast_without_savings AS (

SELECT `month`, `group`, category, cost_juau, cost_lana, future_mode, 'ganhos' as earning_category, year, cost_house
FROM segmentar_ganhos
WHERE category <> 'ganhos extra'

	union all 

SELECT `month`, `group`, category, cost_juau, cost_lana, future_mode, 'ganhos + ganhos extra' as earning_category, year, cost_house
FROM segmentar_ganhos

)

SELECT * FROM overall_forecast_without_savings

	union all

SELECT 
    month,
    'poupança' AS `group`,
    'poupança' AS category,
    - SUM(cost_juau) as cost_juau,
    - SUM(cost_lana) as cost_lana,
    future_mode,
    earning_category,
    year,
    - SUM(cost_house) as cost_house
FROM

    overall_forecast_without_savings

GROUP BY month , future_mode , earning_category , year
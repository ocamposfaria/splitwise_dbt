WITH

segmentar_futuro AS (
SELECT *, substring(month, 1, 4) as year, cost_juau + cost_lana as cost_house, "future_estimated (pessimista): estimativa de gastos baseada na média dos últimos meses para 'nossa residência' e 'compras', e planilha de gastos futuros para 'viagens'" as future_mode FROM {{ref('overall_costs')}} WHERE `month` < substring(curdate() /* - interval 2 month*/, 1, 7)  union all
SELECT *, substring(month, 1, 4) as year, cost_juau + cost_lana as cost_house, "future_estimated (pessimista): estimativa de gastos baseada na média dos últimos meses para 'nossa residência' e 'compras', e planilha de gastos futuros para 'viagens'" as future_mode FROM {{ref('overall_costs_future_estimated')}} 
	union all
SELECT *, substring(month, 1, 4) as year, cost_juau + cost_lana as cost_house, "future_planned (razoável): estimativa de gastos para 'nossa residência' e planilha de gastos futuros para 'compras' e 'viagens'" as future_mode FROM {{ref('overall_costs')}} WHERE `month` < substring(curdate() /* - interval 2 month*/, 1, 7) union all
SELECT *, substring(month, 1, 4) as year, cost_juau + cost_lana as cost_house, "future_planned (razoável): estimativa de gastos para 'nossa residência' e planilha de gastos futuros para 'compras' e 'viagens'" as future_mode FROM {{ref('overall_costs_future_planned')}}
	union all
SELECT *, substring(month, 1, 4) as year, cost_juau + cost_lana as cost_house, "future_limited (otimista): limites para 'nossa residência' e planilha de gastos futuros para 'compras' e 'viagens'" as future_mode FROM {{ref('overall_costs')}} WHERE `month` < substring( curdate() /* - interval 2 month*/, 1, 7) union all
SELECT *, substring(month, 1, 4) as year, cost_juau + cost_lana as cost_house, "future_limited (otimista): limites para 'nossa residência' e planilha de gastos futuros para 'compras' e 'viagens'" as future_mode FROM {{ref('overall_costs_future_limits')}}
),

segmentar_extras AS (

SELECT `month`, `group`, category, cost_juau, cost_lana, future_mode, 'ganhos' as earning_category, year, cost_house
FROM segmentar_futuro
WHERE category <> 'ganhos extra'

	union all 

SELECT `month`, `group`, category, cost_juau, cost_lana, future_mode, 'ganhos + ganhos extra' as earning_category, year, cost_house
FROM segmentar_futuro

),

segmentar_poupanca AS (
    SELECT * FROM segmentar_extras

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
        segmentar_extras

    GROUP BY month , future_mode , earning_category , year
)

SELECT
    *,
    if(`month` >= substring(curdate(), 1, 7), 'previsto', 'realizado') as time_split
FROM segmentar_poupanca
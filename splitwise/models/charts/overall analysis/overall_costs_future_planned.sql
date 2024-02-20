WITH

gastos_mes_passado AS (
SELECT 
	`overall_costs`.`group` AS `group`,
	IF(((`overall_costs`.`category` = 'padaria')
		OR (`overall_costs`.`category` = 'feira')), 'conveniencia', `overall_costs`.`category`) AS `category`,
	avg(`overall_costs`.`cost_juau` + `overall_costs`.`cost_lana`) AS `cost_house`,
    avg(`overall_costs`.`cost_juau`) AS `cost_juau`,
    avg(`overall_costs`.`cost_lana`) AS `cost_lana`
FROM
	{{ref('overall_costs')}}
WHERE
		(`overall_costs`.`group` NOT IN ('viagens', 'compras'))
		AND (overall_costs.category NOT IN ('ganhos' , 'ganhos extra'))
		AND overall_costs.month between substring(curdate() - interval 6 month, 1, 7) and substring(curdate() - interval 1 month, 1, 7) -- média dos últimos 6 meses
GROUP BY 1, 2
),

estimativa_futuro_nossa_residencia AS (
SELECT 
	`group`,
	`category`,
    CASE
		WHEN category = 'apenas joão' THEN g.`cost_juau`
        WHEN category = 'apenas lana' THEN 0
        ELSE `cost_house` * percentage_juau
	END as cost_juau,
    CASE
		WHEN category = 'apenas joão' THEN 0
        WHEN category = 'apenas lana' THEN g.`cost_lana`
        ELSE `cost_house` * percentage_lana
	END as cost_lana,
	month 
FROM gastos_mes_passado g
JOIN {{ref('monthly_percentages')}} p ON p.month >= substring(curdate(), 1, 7)
),

ganhos_previstos AS (
	SELECT
		'ganhos',
		category,
		cost_juau,
		cost_lana,
		month
	FROM {{ref('earnings_final')}}
	WHERE month >= substring(curdate(), 1, 7)
    ),

gastos_futuros AS (
SELECT 
    future_expenses.group AS `group`,
    future_expenses.category AS category,
    future_expenses.cost_juau AS cost_juau,
    future_expenses.cost_lana AS cost_lana,
	future_expenses.month AS month
FROM
    {{source('sheets', 'future_expenses_sheet')}} future_expenses
WHERE
    future_expenses.month >= SUBSTRING(CURDATE(), 1, 7)
),

compras_e_viagens_mes_atual_e_futuro AS (
SELECT 
    overall_costs.group AS `group`,
    overall_costs.category AS category,
    overall_costs.cost_juau AS cost_juau,
    overall_costs.cost_lana AS cost_lana,
    overall_costs.month AS month
FROM
    {{ref('overall_costs')}}
WHERE
    overall_costs.month >= substr(curdate(), 1, 7) -- mes atual
    and `group` in ('compras', 'viagens')
),

presentes_futuros AS (
	SELECT
		'nossa residência' as `group`,
		category,
		limit_juau as cost_juau,
		limit_lana as cost_lana,
		month
	FROM {{ref('limits_final')}}
	WHERE month >= substring(curdate(), 1, 7)
	and category = 'presentes'
)

SELECT * FROM estimativa_futuro_nossa_residencia
union all SELECT * FROM ganhos_previstos
union all SELECT * FROM gastos_futuros
union all SELECT * FROM compras_e_viagens_mes_atual_e_futuro
union all SELECT * FROM presentes_futuros
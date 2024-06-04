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
    

estimativa_compras AS (
	SELECT 
		`overall_costs`.`group` AS `group`,
		IF(((`overall_costs`.`category` = 'padaria')
			OR (`overall_costs`.`category` = 'feira')), 'conveniencia', `overall_costs`.`category`) AS `category`,
		sum(`overall_costs`.`cost_juau`)/12 AS `cost_juau`,
		sum(`overall_costs`.`cost_lana`)/12 AS `cost_lana`
	FROM
		{{ref('overall_costs')}}
	WHERE
		((1 = 1)
			AND (`overall_costs`.`group` IN ('compras'))
			AND (overall_costs.category NOT IN ('ganhos' , 'ganhos extra'))
			AND (CAST(CONCAT(overall_costs.month, '-01') AS DATE) <= (CURDATE() - INTERVAL 1 MONTH))
			AND (CAST(CONCAT(overall_costs.month, '-01') AS DATE) > (CURDATE() - INTERVAL 13 MONTH)))
	GROUP BY overall_costs.group , IF(((overall_costs.category = 'padaria')
		OR (overall_costs.category = 'feira')), 'conveniencia', overall_costs.category)
),

meses_para_estimativa AS (
SELECT 
	some_months.month AS month
FROM
	{{ref('some_months')}} some_months
WHERE
	(CAST(CONCAT(some_months.month, '-01') AS DATE) >= (CURDATE() - INTERVAL 1 MONTH))
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
),

gastos_futuros_viagens AS (
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
	and `group` = 'viagens'
)

SELECT * FROM estimativa_futuro_nossa_residencia union all

(SELECT * FROM estimativa_compras
JOIN meses_para_estimativa)

union all SELECT * FROM ganhos_previstos
union all SELECT * FROM presentes_futuros
union all SELECT * FROM gastos_futuros_viagens
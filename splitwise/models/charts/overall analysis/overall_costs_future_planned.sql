WITH

media_nossa_residencia AS (
	SELECT 
		`overall_costs`.`group` AS `group`,
		IF(((`overall_costs`.`category` = 'padaria')
			OR (`overall_costs`.`category` = 'feira')), 'conveniencia', `overall_costs`.`category`) AS `category`,
		(SUM(`overall_costs`.`cost_juau`) / 2) AS `cost_juau`,
		(SUM(`overall_costs`.`cost_lana`) / 2) AS `cost_lana`
	FROM
		{{ref('overall_costs')}}
	WHERE
		((1 = 1)
			AND (`overall_costs`.`group` NOT IN ('viagens' , 'compras'))
			AND (overall_costs.category NOT IN ('ganhos' , 'ganhos extra'))
			AND (CAST(CONCAT(overall_costs.month, '-01') AS DATE) <= (CURDATE() - INTERVAL 1 MONTH))
			AND (CAST(CONCAT(overall_costs.month, '-01') AS DATE) > (CURDATE() - INTERVAL 3 MONTH)))
	GROUP BY overall_costs.group , IF(((overall_costs.category = 'padaria')
		OR (overall_costs.category = 'feira')), 'conveniencia', overall_costs.category)
),

ganhos_previstos AS (
	SELECT 
		'ganhos' AS `group`,
		'ganhos' AS category,
		-(6460) AS cost_juau,
		-(4087) AS cost_lana
    union all 
	SELECT 
		'ganhos' AS `group`,
		'ganhos extra' AS category,
		-(0) AS cost_juau,
		-(0) AS cost_lana
    ),

meses_para_estimativa AS (
SELECT 
	some_months.month AS month
FROM
	{{ref('some_months')}}
WHERE
	(CAST(CONCAT(some_months.month, '-01') AS DATE) >= (CURDATE() - INTERVAL 1 MONTH))
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

compras_mes_atual AS (
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
)

SELECT
	*
FROM
(SELECT * FROM media_nossa_residencia union all
SELECT * FROM ganhos_previstos) sq
JOIN meses_para_estimativa

union all SELECT * FROM gastos_futuros
union all SELECT * FROM compras_mes_atual
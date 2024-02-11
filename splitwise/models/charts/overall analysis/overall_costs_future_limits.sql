WITH

limites_nossa_residencia AS (
	SELECT 
        'nossa residência' AS `group`,
		limits_final.category AS category,
		limits_final.limit_juau AS cost_juau,
		limits_final.limit_lana AS cost_lana
    FROM
        {{ref('limits_final')}} 
    WHERE
        limits_final.month IN (SELECT MAX(limits_final.month) FROM {{ref('limits_final')}})
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
(SELECT * FROM limites_nossa_residencia union all
SELECT * FROM ganhos_previstos) sq
JOIN meses_para_estimativa

union all SELECT * FROM gastos_futuros
union all SELECT * FROM compras_mes_atual
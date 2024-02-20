WITH

limites_futuros AS (
	SELECT
		'nossa residência' as `group`,
		category,
		limit_juau as cost_juau,
		limit_lana as cost_lana,
		month
	FROM {{ref('limits_final')}}
	WHERE month >= substring(curdate(), 1, 7)
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
    `bob`.`future_expenses_sheet` future_expenses
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
)

SELECT * FROM limites_futuros
union all SELECT * FROM ganhos_previstos
union all SELECT * FROM gastos_futuros
union all SELECT * FROM compras_e_viagens_mes_atual_e_futuro
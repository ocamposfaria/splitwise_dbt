WITH anos AS (
	SELECT substring(`month`, 1, 4) as year FROM {{ref('some_months')}} GROUP BY 1 HAVING `year` >= 2024 
), 

gastos_presentes_lana AS (
	SELECT
		'presentes' as category,
		0 as cost_juau,
        value as cost_lana,
		concat(`year`, '-', `month`) as `year_month`
	FROM {{ source('sheets','gifts_sheet') }} JOIN anos
	WHERE giver = 'lana'
	ORDER BY `year_month`
),

gastos_presentes_joao AS (
	SELECT
		'presentes' as category,
		value as cost_juau,
        0 as cost_lana,
		concat(`year`, '-', `month`) as `year_month`
	FROM {{ source('sheets','gifts_sheet') }} JOIN anos
	WHERE giver = 'joão'
	ORDER BY `year_month`
),

gastos_presentes AS (
	SELECT * FROM gastos_presentes_joao
	union all SELECT * FROM gastos_presentes_lana
)

SELECT 
	category,
	sum(cost_juau + cost_lana) as cost,
    sum(cost_juau) as cost_juau,
    sum(cost_lana) as cost_lana,
    `year_month`
FROM gastos_presentes GROUP BY 1, 5
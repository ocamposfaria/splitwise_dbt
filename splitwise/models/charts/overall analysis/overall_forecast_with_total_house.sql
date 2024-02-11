SELECT 
	`month`,
	`group`,
	CASE
		WHEN category = 'feira' THEN 'conveniência'
		WHEN category = 'padaria' THEN 'conveniência'
	ELSE category END as category,
	abs(cost_juau) + abs(cost_lana) as cost_house,
    abs(cost_juau) as cost_juau,
    abs(cost_lana) as cost_lana,
	future_mode,
	earning_category
FROM {{ref("overall_forecast")}}
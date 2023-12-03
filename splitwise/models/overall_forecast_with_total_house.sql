SELECT 
	`month`,
	`group`,
	category,
	cost_juau + cost_lana as cost_house,
    cost_juau,
    cost_lana,
	future_mode,
	earning_category
FROM {{ref("overall_forecast")}}
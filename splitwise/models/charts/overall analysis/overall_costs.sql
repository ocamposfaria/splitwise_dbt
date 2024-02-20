WITH ganhos AS (SELECT 
		'ganhos' AS `group`,
		splitwise_final.category AS category,
		SUM(splitwise_final.cost_juau) AS cost_juau,
		SUM(splitwise_final.cost_lana) AS cost_lana,
		`splitwise_final`.`month` AS `month`
	FROM
		{{ref('splitwise_final')}}
	WHERE
		((splitwise_final.source IN ('Nossa Residencia' , 'VR', 'just me', 'apenas lana'))
			AND (splitwise_final.category IN ('ganhos',	'ganhos extra')))
	GROUP BY 1, 2, 5),
    
nossa_residencia AS (SELECT 
		'nossa residência' AS `group`,
		splitwise_final.category AS category,
		SUM(splitwise_final.cost_juau) AS cost_juau,
		SUM(splitwise_final.cost_lana) AS cost_lana,
		`splitwise_final`.`month` AS `month`
	FROM
		{{ref('splitwise_final')}}
	WHERE
		((splitwise_final.source IN ('Nossa Residencia' , 'VR', 'just me', 'apenas lana'))
			AND (splitwise_final.category IN ('contas' , 'mercado',
			'outros',
			'transporte',
			'alimentacao',
			'bem-estar',
			'feira',
			'padaria',
			'presentes',
			'assinaturas',
			'conveniencia',
			'apenas joão',
			'apenas lana')))
	GROUP BY 1, 2, 5),
    
compras AS (SELECT 
		'compras' AS `group`,
		'compras' AS category,
		SUM(splitwise_final.cost_juau) AS cost_juau,
		SUM(splitwise_final.cost_lana) AS cost_lana,
		splitwise_final.month AS month
	FROM
		{{ref('splitwise_final')}}
	WHERE
		splitwise_final.source IN ('Nossa Residencia' , 'VR', 'just me', 'apenas lana')
			AND splitwise_final.category = 'compras'
	GROUP BY 1, 2, 5),

viagens AS (SELECT 
		'viagens' AS `group`,
		splitwise_final.source AS category,
		SUM(splitwise_final.cost_juau) AS cost_juau,
		SUM(splitwise_final.cost_lana) AS cost_lana,
		splitwise_final.month AS month
	FROM
		{{ref('splitwise_final')}}
	WHERE
		((splitwise_final.source NOT IN ('Nossa Residencia' , 'VR', 'just me', 'apenas lana'))
			AND (splitwise_final.source <> 'Black Mobly'))
	GROUP BY 1, 2, 5)
    
SELECT * FROM ganhos union all
SELECT * FROM nossa_residencia union all
SELECT * FROM compras union all
SELECT * FROM viagens
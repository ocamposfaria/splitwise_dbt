{{ config(materialized='view') }}

SELECT 
	`splitwise_final`.`month` AS `month`,
	`splitwise_final`.`name` AS `name`,
	(CASE
		WHEN (`splitwise_final`.`source` = 'VR') THEN 'monthly costs'
		WHEN
			((`splitwise_final`.`source` IN ('just me' , 'Nossa Residencia'))
				AND (splitwise_final.category <> 'compras'))
		THEN
			'monthly costs'
		WHEN
			((splitwise_final.source IN ('just me' , 'Nossa Residencia'))
				AND (splitwise_final.category = 'compras'))
		THEN
			'compras'
		ELSE splitwise_final.source
	END) AS source,
	SUM(splitwise_final.cost_juau) AS cost,
	'joão' AS `cost by`
FROM
	{{ref('splitwise_final')}}
WHERE
	((splitwise_final.category NOT IN ('ganhos' , 'ganhos extra'))
		AND (splitwise_final.source <> 'Black Mobly'))
GROUP BY splitwise_final.month , splitwise_final.name , (CASE
	WHEN (splitwise_final.source = 'VR') THEN 'monthly costs'
	WHEN
		((splitwise_final.source IN ('just me' , 'Nossa Residencia'))
			AND (splitwise_final.category <> 'compras'))
	THEN
		'monthly costs'
	WHEN
		((splitwise_final.source IN ('just me' , 'Nossa Residencia'))
			AND (splitwise_final.category = 'compras'))
	THEN
		'compras'
	ELSE splitwise_final.source
END) 
UNION ALL SELECT 
	splitwise_final.month AS month,
	splitwise_final.`name` AS name,
	(CASE
		WHEN (splitwise_final.source = 'VR') THEN 'monthly costs'
		WHEN
			((splitwise_final.source IN ('apenas lana' , 'Nossa Residencia'))
				AND (splitwise_final.category <> 'compras'))
		THEN
			'monthly costs'
		WHEN
			((splitwise_final.source IN ('apenas lana' , 'Nossa Residencia'))
				AND (splitwise_final.category = 'compras'))
		THEN
			'compras'
		ELSE splitwise_final.source
	END) AS source,
	SUM(splitwise_final.cost_lana) AS cost,
	'lana' AS `cost by`
FROM
	{{ref('splitwise_final')}}
WHERE
	((splitwise_final.category NOT IN ('ganhos' , 'ganhos extra'))
		AND (splitwise_final.source <> 'Black Mobly'))
GROUP BY splitwise_final.month , splitwise_final.name , (CASE
	WHEN (splitwise_final.source = 'VR') THEN 'monthly costs'
	WHEN
		((splitwise_final.source IN ('apenas lana' , 'Nossa Residencia'))
			AND (splitwise_final.category <> 'compras'))
	THEN
		'monthly costs'
	WHEN
		((splitwise_final.source IN ('apenas lana' , 'Nossa Residencia'))
			AND (splitwise_final.category = 'compras'))
	THEN
		'compras'
	ELSE splitwise_final.source
END)
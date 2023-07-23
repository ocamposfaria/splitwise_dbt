{{ config(materialized='view') }}

SELECT 
    `splitwise_final`.`month` AS `month`,
    'nossa residência' AS `group`,
    splitwise_final.category AS category,
    SUM(splitwise_final.cost_juau) AS cost_juau,
    SUM(splitwise_final.cost_lana) AS cost_lana
FROM
    {{ref('splitwise_final')}}
WHERE
    ((splitwise_final.source IN ('Nossa Residencia' , 'VR', 'just me', 'apenas lana'))
        AND (splitwise_final.category IN ('contas' , 'mercado',
        'outros',
        'transporte',
        'alimentação',
        'feira',
        'padaria',
        'assinaturas',
        'conveniencia',
        'ganhos',
        'ganhos extra')))
GROUP BY 1, 2, 3 
UNION ALL SELECT 
    splitwise_final.month AS month,
    'compras' AS `group`,
    splitwise_final.category AS category,
    SUM(splitwise_final.cost_juau) AS cost_juau,
    SUM(splitwise_final.cost_lana) AS cost_lana
FROM
    {{ref('splitwise_final')}}
WHERE
    ((splitwise_final.source IN ('Nossa Residencia' , 'VR', 'just me', 'apenas lana'))
        AND (splitwise_final.category = 'compras'))
GROUP BY splitwise_final.month , 'compras' , splitwise_final.category 
UNION ALL SELECT 
    splitwise_final.month AS month,
    'viagens' AS `group`,
    splitwise_final.source AS category,
    SUM(splitwise_final.cost_juau) AS cost_juau,
    SUM(splitwise_final.cost_lana) AS cost_lana
FROM
    {{ref('splitwise_final')}}
WHERE
    ((splitwise_final.source NOT IN ('Nossa Residencia' , 'VR', 'just me', 'apenas lana'))
        AND (splitwise_final.source <> 'Black Mobly'))
GROUP BY splitwise_final.month , 'viagens' , splitwise_final.source 
UNION ALL SELECT 
    splitwise_final.month AS month,
    'apenas joão' AS `group`,
    splitwise_final.category AS category,
    SUM(splitwise_final.cost_juau) AS cost_juau,
    SUM(splitwise_final.cost_lana) AS cost_lana
FROM
    {{ref('splitwise_final')}}
WHERE
    ((splitwise_final.source IN ('Nossa Residencia' , 'VR', 'just me', 'apenas lana'))
        AND (splitwise_final.category = 'apenas joão'))
GROUP BY splitwise_final.month , 'apenas joão' , splitwise_final.category 
UNION ALL SELECT 
    splitwise_final.month AS month,
    'apenas lana' AS `group`,
    splitwise_final.category AS category,
    SUM(splitwise_final.cost_juau) AS cost_juau,
    SUM(splitwise_final.cost_lana) AS cost_lana
FROM
    {{ref('splitwise_final')}}
WHERE
    ((splitwise_final.source IN ('Nossa Residencia' , 'VR', 'just me', 'apenas lana'))
        AND (splitwise_final.category = 'apenas lana'))
GROUP BY splitwise_final.month , 'apenas lana' , splitwise_final.category
ORDER BY month DESC
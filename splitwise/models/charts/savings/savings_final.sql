{{ config(materialized='view') }}

SELECT 
    `splitwise_final`.`month` AS `month`,
    `splitwise_final`.`source` AS `source`,
    (SUM(IF((`splitwise_final`.`category` IN ('ganhos' , 'ganhos extra')),
        `splitwise_final`.`cost_juau`,
        0)) * -(1)) AS `total_earning`,
    (SUM(IF((`splitwise_final`.`category` NOT IN ('ganhos' , 'ganhos extra')),
        `splitwise_final`.`cost_juau`,
        0)) * -(1)) AS `cost`,
    (SUM(`splitwise_final`.`cost_juau`) * -(1)) AS `balance`,
    'ganhos + ganhos extra' AS `earning_category`,
    'joão' AS cost_by
FROM
    {{ref('splitwise_final')}}
WHERE
    (splitwise_final.source <> 'Black Mobly')
GROUP BY splitwise_final.month , splitwise_final.source 
UNION ALL SELECT 
    splitwise_final.month AS month,
    splitwise_final.source AS source,
    (SUM(IF((splitwise_final.category = 'ganhos'),
        splitwise_final.cost_juau,
        0)) * -(1)) AS total_earning,
    (SUM(IF((splitwise_final.category NOT IN ('ganhos' , 'ganhos extra')),
        splitwise_final.cost_juau,
        0)) * -(1)) AS cost,
    (SUM(IF((splitwise_final.category <> 'ganhos extra'),
        splitwise_final.cost_juau,
        0)) * -(1)) AS balance,
    'ganhos' AS earning_category,
    'joão' AS cost_by
FROM
    {{ref('splitwise_final')}}
WHERE
    (splitwise_final.source <> 'Black Mobly')
GROUP BY splitwise_final.month , splitwise_final.source 
UNION ALL SELECT 
    splitwise_final.month AS month,
    splitwise_final.source AS source,
    (SUM(IF((splitwise_final.category IN ('ganhos' , 'ganhos extra')),
        splitwise_final.cost_lana,
        0)) * -(1)) AS total_earning,
    (SUM(IF((splitwise_final.category NOT IN ('ganhos' , 'ganhos extra')),
        splitwise_final.cost_lana,
        0)) * -(1)) AS cost,
    (SUM(splitwise_final.cost_lana) * -(1)) AS balance,
    'ganhos + ganhos extra' AS earning_category,
    'lana' AS cost_by
FROM
    {{ref('splitwise_final')}}
WHERE
    (splitwise_final.source <> 'Black Mobly')
GROUP BY splitwise_final.month , splitwise_final.source 
UNION ALL SELECT 
    splitwise_final.month AS month,
    splitwise_final.source AS source,
    (SUM(IF((splitwise_final.category = 'ganhos'),
        splitwise_final.cost_lana,
        0)) * -(1)) AS total_earning,
    (SUM(IF((splitwise_final.category NOT IN ('ganhos' , 'ganhos extra')),
        splitwise_final.cost_lana,
        0)) * -(1)) AS cost,
    (SUM(IF((splitwise_final.category <> 'ganhos extra'),
        splitwise_final.cost_lana,
        0)) * -(1)) AS balance,
    'ganhos' AS earning_category,
    'lana' AS cost_by
FROM
    {{ref('splitwise_final')}}
WHERE
    (splitwise_final.source <> 'Black Mobly')
GROUP BY splitwise_final.month , splitwise_final.source
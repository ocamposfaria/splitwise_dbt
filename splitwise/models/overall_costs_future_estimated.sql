{{ config(materialized='view') }}

SELECT 
    `sel2`.`month` AS `month`,
    `sel1`.`group` AS `group`,
    `sel1`.`category` AS `category`,
    `sel1`.`cost_juau` AS `cost_juau`,
    `sel1`.`cost_lana` AS `cost_lana`
FROM
    ((SELECT 
        `overall_costs`.`group` AS `group`,
            IF(((`overall_costs`.`category` = 'padaria')
                OR (`overall_costs`.`category` = 'feira')), 'conveniencia', `overall_costs`.`category`) AS `category`,
            (SUM(`overall_costs`.`cost_juau`) / 5) AS `cost_juau`,
            (SUM(`overall_costs`.`cost_lana`) / 5) AS `cost_lana`
    FROM
        {{ref('overall_costs')}}
    WHERE
        ((1 = 1)
            AND (`overall_costs`.`group` <> 'viagens')
            AND (`overall_costs`.`category` NOT IN ('ganhos' , 'ganhos extra'))
            AND (CAST(CONCAT(`overall_costs`.`month`, '-01') AS DATE) <= (CURDATE() - INTERVAL 1 MONTH))
            AND (CAST(CONCAT(`overall_costs`.`month`, '-01') AS DATE) > (CURDATE() - INTERVAL 6 MONTH)))
    GROUP BY `overall_costs`.`group` , IF(((`overall_costs`.`category` = 'padaria')
        OR (`overall_costs`.`category` = 'feira')), 'conveniencia', `overall_costs`.`category`) UNION ALL SELECT 
        'nossa residência' AS `group`,
            'ganhos' AS category,
            -(6300) AS cost_juau,
            -(4087) AS cost_lana
        UNION ALL SELECT 
        'nossa residência' AS `group`,
            'ganhos extra' AS category,
            -(0) AS cost_juau,
            -(0) AS cost_lana
    ) sel1
    JOIN (SELECT 
        some_months.month AS month
    FROM
        {{ref('some_months')}}
    WHERE
        (CAST(CONCAT(some_months.month, '-01') AS DATE) >= (CURDATE() - INTERVAL 1 MONTH))) sel2) 
UNION ALL 

SELECT 
    future_expenses.month AS month,
    future_expenses.group AS `group`,
    future_expenses.category AS category,
    future_expenses.cost_juau AS cost_juau,
    future_expenses.cost_lana AS cost_lana
FROM
    {{source('sheets', 'future_expenses_sheet')}} future_expenses
WHERE
    future_expenses.group = 'viagens'
    and future_expenses.month >= SUBSTRING(CURDATE(), 1, 7) 


UNION ALL 


SELECT 
    overall_costs.month AS month,
    overall_costs.group AS `group`,
    overall_costs.category AS category,
    overall_costs.cost_juau AS cost_juau,
    overall_costs.cost_lana AS cost_lana
FROM
    {{ ref('overall_costs') }}
WHERE
    overall_costs.month >= substr(curdate(), 1, 7) -- mes atual
    and `group` = 'viagens'
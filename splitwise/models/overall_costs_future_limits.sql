{{ config(materialized='view') }}

SELECT 
    `sel2`.`month` AS `month`,
    `sel1`.`group` AS `group`,
    `sel1`.`category` AS `category`,
    `sel1`.`cost_juau` AS `cost_juau`,
    `sel1`.`cost_lana` AS `cost_lana`
FROM
    ((SELECT 
        'nossa residência' AS `group`,
            limits_final.category AS category,
            limits_final.limit_juau AS cost_juau,
            limits_final.limit_lana AS cost_lana
    FROM
        {{ref('limits_final')}}
    WHERE
        (limits_final.month IN (SELECT 
                MAX(limits_final.month)
            FROM
                {{ref('limits_final')}})
            AND (limits_final.category NOT IN ('apenas joão' , 'apenas lana'))) UNION ALL SELECT 
        'nossa residência' AS `group`,
            'ganhos' AS category,
            -(4064.77) AS cost_juau,
            -(3495) AS cost_lana
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
UNION ALL SELECT 
    future_expenses.month AS month,
    future_expenses.group AS `group`,
    future_expenses.category AS category,
    future_expenses.cost_juau AS cost_juau,
    future_expenses.cost_lana AS cost_lana
FROM
    bob.future_expenses_sheet future_expenses
WHERE future_expenses.month >= substring(curdate(), 1, 7)
UNION ALL SELECT 
    overall_costs.month AS month,
    overall_costs.group AS `group`,
    overall_costs.category AS category,
    overall_costs.cost_juau AS cost_juau,
    overall_costs.cost_lana AS cost_lana
FROM
    {{ref('overall_costs')}}
WHERE
    ((CAST(CONCAT(overall_costs.month, '-01') AS DATE) >= CURDATE())
        AND (overall_costs.group <> 'nossa residencia'))
{{ config(materialized='view') }}

SELECT 
    `l`.`month` AS `month`,
    `l`.`category` AS `category`,
    IFNULL(`mc`.`month_cost_house`, 0) AS `month_cost_house`,
    `l`.`cost` AS `limit_house`,
    IFNULL(`mc`.`month_cost_juau`, 0) AS `month_cost_juau`,
    `l`.`cost_juau` AS `limit_juau`,
    IFNULL(`mc`.`month_cost_lana`, 0) AS `month_cost_lana`,
    `l`.`cost_lana` AS `limit_lana`
FROM
    (`bob`.`limits` `l`
    LEFT JOIN (SELECT 
        `splitwise_final`.`month` AS `month`,
            `splitwise_final`.`category` AS `category`,
            SUM(`splitwise_final`.`cost`) AS `month_cost_house`,
            SUM(`splitwise_final`.`cost_juau`) AS `month_cost_juau`,
            SUM(`splitwise_final`.`cost_lana`) AS `month_cost_lana`
    FROM
        {{ref('splitwise_final')}}
    WHERE
        ((1 = 1)
            AND (`splitwise_final`.`source` IN ('Nossa Residência' , 'VR', 'just me', 'apenas lana')))
    GROUP BY splitwise_final.month , splitwise_final.category) mc ON (((mc.month = l.month)
        AND (mc.category = l.category))))
WHERE
    (l.month <> 'old')
ORDER BY l.month DESC
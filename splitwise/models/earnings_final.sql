{{ config(materialized='view') }}

SELECT 
    NULL AS `expense_id`,
    `earnings`.`category` AS `category`,
    `earnings`.`name` AS `name`,
    (-(`earnings`.`cost_juau`) - `earnings`.`cost_lana`) AS `cost`,
    -(`earnings`.`cost_juau`) AS `cost_juau`,
    -(`earnings`.`cost_lana`) AS `cost_lana`,
    `earnings`.`month` AS `month`,
    NULL AS `created_at`,
    NULL AS `updated_at`,
    'João' AS created_by,
    (CASE
        WHEN (earnings.cost_juau <> 0) THEN 'just me'
        WHEN (earnings.cost_lana <> 0) THEN 'apenas lana'
    END) AS source,
    (CASE
        WHEN (earnings.cost_juau <> 0) THEN 100
        ELSE 0
    END) AS percentage_juau,
    (CASE
        WHEN (earnings.cost_lana <> 0) THEN 100
        ELSE 0
    END) AS percentage_lana
FROM
    {{ source('sheets','earnings_sheet') }} earnings
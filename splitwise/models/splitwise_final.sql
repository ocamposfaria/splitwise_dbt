{{ config(materialized='view') }}

SELECT 
    `sel`.`expense_id` AS `expense_id`,
    `sel`.`category` AS `category`,
    `sel`.`name` AS `name`,
    `sel`.`cost` AS `cost`,
    `sel`.`cost_juau` AS `cost_juau`,
    `sel`.`cost_lana` AS `cost_lana`,
    `sel`.`month` AS `month`,
    `sel`.`created_at` AS `created_at`,
    `sel`.`updated_at` AS `updated_at`,
    `sel`.`created_by` AS `created_by`,
    `sel`.`source` AS `source`,
    ((`sel`.`cost_juau` / (`sel`.`cost_juau` + `sel`.`cost_lana`)) * 100) AS `percentage_juau`,
    ((`sel`.`cost_lana` / (`sel`.`cost_juau` + `sel`.`cost_lana`)) * 100) AS `percentage_lana`
FROM
    (SELECT 
        `s`.`expense_id` AS `expense_id`,
            (CASE
                WHEN
                    ((`s`.`name` = 'None')
                        AND (`s`.`source` <> 'just me')
                        AND (`s`.`source` <> 'apenas lana'))
                THEN
                    'None'
                WHEN
                    ((`s`.`name` = 'None')
                        AND (`s`.`source` = 'just me'))
                THEN
                    'apenas joão'
                WHEN
                    ((s.name = 'None')
                        AND (s.source = 'apenas lana'))
                THEN
                    'apenas lana'
                ELSE s.category
            END) AS category,
            (CASE
                WHEN (s.name = 'None') THEN s.category
                ELSE s.name
            END) AS name,
            (CASE
                WHEN
                    (((s.source IN ('just me' , 'Black Mobly'))
                        AND (s.category = 'ganhos'))
                        OR (s.category = 'ganhos extra'))
                THEN
                    -(s.cost)
                WHEN
                    (((s.source = 'apenas lana')
                        AND (s.category = 'ganhos'))
                        OR (s.category = 'ganhos extra'))
                THEN
                    -(s.cost)
                ELSE s.cost
            END) AS cost,
            (CASE
                WHEN (s.source = 'apenas lana') THEN 0
                WHEN
                    (((s.source IN ('just me' , 'Black Mobly'))
                        AND (s.category = 'ganhos'))
                        OR (s.category = 'ganhos extra'))
                THEN
                    -(s.cost)
                WHEN
                    ((s.source IN ('just me' , 'Black Mobly'))
                        AND (s.category <> 'ganhos')
                        AND (s.category <> 'ganhos extra'))
                THEN
                    s.cost
                WHEN
                    ((s.source = 'VR')
                        AND (s.repayments_from = 'Hallana'))
                THEN
                    s.cost
                WHEN
                    ((s.source = 'VR')
                        AND (s.repayments_from = 'João'))
                THEN
                    0
                WHEN (`s`.repayments_from = 'Hallana') THEN (`s`.cost - `s`.repayments)
                ELSE `s`.repayments
            END) AS cost_juau,
            (CASE
                WHEN (`s`.source IN ('just me' , 'Black Mobly')) THEN 0
                WHEN
                    (((`s`.source = 'apenas lana')
                        AND (`s`.category = 'ganhos'))
                        OR (`s`.category = 'ganhos extra'))
                THEN
                    -(`s`.cost)
                WHEN
                    ((`s`.source = 'apenas lana')
                        AND (`s`.category <> 'ganhos')
                        AND (`s`.category <> 'ganhos extra'))
                THEN
                    `s`.cost
                WHEN
                    ((`s`.source = 'VR')
                        AND (`s`.repayments_from = 'João'))
                THEN
                    s.cost
                WHEN
                    ((s.source = 'VR')
                        AND (s.repayments_from = 'Hallana'))
                THEN
                    0
                WHEN (s.repayments_from = 'João') THEN (s.cost - s.repayments)
                ELSE s.repayments
            END) AS cost_lana,
            IF(((s.details IS NOT NULL)
                AND (s.details <> 'None')), s.details, s.month) AS month,
            s.created_at AS created_at,
            s.updated_at AS updated_at,
            s.created_by AS created_by,
            s.source AS source
    FROM
        bob.splitwise s
    WHERE
        ((s.deleted_by = 'None')
            AND (s.category <> 'Dinheiro extra pai juau')
            AND (s.category <> 'Payment')
            AND (s.category <> 'QUITE')
            AND (s.month <> 'old')
            AND (s.expense_id NOT IN ('1982325837' , '2374550198', '2374528360', '2420271153', '2420307343'))
            AND (s.category NOT IN ('ganhos' , 'ganhos extra')))
    ORDER BY s.expense_id DESC) sel 

UNION ALL 

SELECT 
    earnings_final.expense_id AS expense_id,
    earnings_final.category AS category,
    earnings_final.name AS name,
    earnings_final.cost AS cost,
    earnings_final.cost_juau AS cost_juau,
    earnings_final.cost_lana AS cost_lana,
    earnings_final.month AS month,
    earnings_final.created_at AS created_at,
    earnings_final.updated_at AS updated_at,
    earnings_final.created_by AS created_by,
    earnings_final.source AS source,
    earnings_final.percentage_juau AS percentage_juau,
    earnings_final.percentage_lana AS percentage_lana
FROM
    bob.earnings_final
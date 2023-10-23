{{ config(materialized='view') }}

SELECT 
    `splitwise_final`.`month` AS `month`,
    `splitwise_final`.`category` AS `category`,
    `splitwise_final`.`name` AS `name`,
    `splitwise_final`.`cost` AS `cost`,
    COUNT(`splitwise_final`.`expense_id`) AS `COUNT(expense_id)`,
    MIN(`splitwise_final`.`expense_id`) AS `min(expense_id)`
FROM
    {{ref('splitwise_final')}}
WHERE
    (`splitwise_final`.`expense_id` NOT IN ('2273815986' , '2221041952',
        '1981650253',
        '2273865471',
        '2206184023',
        '2103586878',
        '2066617090',
        '2087483227',
        '2002904517',
        '1962561756',
        '1981684264',
        '1941295676',
        '1866736829',
        '1866720926',
        '1866755093',
        '2273818781',
        '2231964741',
        '1886089931',
        '2273821296',
        '2622335362',
        '2645891995',
        '2620923949',
        '2620925908',
        '2530673845',
        '2491051824',
        '2491051871',
        '2491051921',
        '2316580770',
        '2316436346',
        '2645892557',
        '2721267931',
        '2721268718',
        '2721247187',
        '2708127785',
        '2708153234',
        '2704938323',
        '2704938917',
        '2704938810',
        '2704938002',
        '2704938964',
        '2736844440'))
GROUP BY `splitwise_final`.`month` , `splitwise_final`.`category` , `splitwise_final`.`name` , `splitwise_final`.`cost`
HAVING (`COUNT(expense_id)` > 1)
ORDER BY `splitwise_final`.`month` DESC , COUNT(`splitwise_final`.`expense_id`) DESC
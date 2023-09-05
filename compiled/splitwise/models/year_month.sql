

SELECT 
    `splitwise_final`.`month` AS `year_month`
FROM
    `bob`.`splitwise_final`
GROUP BY `splitwise_final`.`month`
ORDER BY `splitwise_final`.`month` DESC
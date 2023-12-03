SELECT 
    SUBSTRING(`month`, 1, 4) AS ano,
    `month`,
    `name`,
    SUM(cost),
    SUM(cost_juau),
    SUM(cost_lana)
FROM
    splitwise_final
WHERE
    category LIKE '%ganhos%'
        AND name LIKE '%leo%'
GROUP BY 1, 2, 3
ORDER BY `month`
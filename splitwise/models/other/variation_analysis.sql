SELECT
    `group`,
    category,
    avg(cost_juau) as cost_juau,
    avg(cost_lana) as cost_lana,
    avg(cost_juau + cost_lana) as cost_house,
    months
FROM {{ref('overall_costs')}} o
JOIN (SELECT 12 as months) sq1 
	ON o.month < substring(curdate(), 1) and o.month >= substring(curdate() - interval 12 month, 1)
    and `group`= 'nossa residência'
GROUP BY 1, 2, 6
union all
SELECT
    `group`,
    category,
    avg(cost_juau) as cost_juau,
    avg(cost_lana) as cost_lana,
    avg(cost_juau + cost_lana) as cost_house,
    months
FROM {{ref('overall_costs')}} o
JOIN (SELECT 6 as months) sq1 
	ON o.month < substring(curdate(), 1) and o.month >= substring(curdate() - interval 6 month, 1)
    and `group`= 'nossa residência'
GROUP BY 1, 2, 6
union all
SELECT
    `group`,
    category,
    avg(cost_juau) as cost_juau,
    avg(cost_lana) as cost_lana,
    avg(cost_juau + cost_lana) as cost_house,
    months
FROM {{ref('overall_costs')}} o
JOIN (SELECT 3 as months) sq2
	ON o.month < substring(curdate(), 1) and o.month >= substring(curdate() - interval 3 month, 1)
    and `group`= 'nossa residência'
GROUP BY 1, 2, 6
union all
SELECT
    `group`,
    category,
    avg(cost_juau) as cost_juau,
    avg(cost_lana) as cost_lana,
    avg(cost_juau + cost_lana) as cost_house,
    months
FROM {{ref('overall_costs')}} o
JOIN (SELECT 1 as months) sq3 
	ON o.month < substring(curdate(), 1) and o.month >= substring(curdate() - interval 1 month, 1)
    and `group`= 'nossa residência'
GROUP BY 1, 2, 6


union all 

SELECT 'limites' as `group`, category, limit_juau, limit_lana, limit_house, 12 as months
FROM {{ref('limits_final')}} WHERE `month` = substring(curdate(), 1, 7) union all
SELECT 'limites' as `group`, category, limit_juau, limit_lana, limit_house, 6 as months
FROM {{ref('limits_final')}} WHERE `month` = substring(curdate(), 1, 7) union all
SELECT 'limites' as `group`, category, limit_juau, limit_lana, limit_house, 3 as months
FROM {{ref('limits_final')}} WHERE `month` = substring(curdate(), 1, 7) union all
SELECT 'limites' as `group`, category, limit_juau, limit_lana, limit_house, 1 as months
FROM {{ref('limits_final')}} WHERE `month` = substring(curdate(), 1, 7)
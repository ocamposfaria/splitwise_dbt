{{ config(materialized='view') }}

SELECT 
	category,
    cost,
    if(category = 'apenas joão', cost, if(ln.cost_juau <> 0, ln.cost_juau, ln.cost*percentage_juau)) as cost_juau,
    if(category = 'apenas lana', cost, if(ln.cost_lana <> 0, ln.cost_lana, ln.cost*percentage_lana)) as cost_lana,
    sm.month
FROM
	bob.limits_sheet ln
JOIN {{ref('some_months')}} sm ON sm.month >= version_month_from and sm.month <= version_month_to
JOIN {{ref('monthly_percentages')}} mp on sm.month = mp.month

WHERE concat(sm.month, '-01') <= curdate()
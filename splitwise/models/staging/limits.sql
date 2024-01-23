{{ config(materialized='view') }}

SELECT 
	category,
    if(cost_extra_week <> 0 and fw.month is not null, cost_extra_week, cost) as cost,
    if(cost_extra_week <> 0 and fw.month is not null, ln.cost_extra_week*percentage_juau, if(ln.cost_juau <> 0, ln.cost_juau, ln.cost*percentage_juau)) as cost_juau,
    if(cost_extra_week <> 0 and fw.month is not null, ln.cost_extra_week*percentage_lana, if(ln.cost_lana <> 0, ln.cost_lana, ln.cost*percentage_lana)) as cost_lana,
    sm.month
FROM
    {{ source('sheets','limits_sheet') }} ln
JOIN {{ref('some_months')}} sm ON sm.month >= version_month_from and sm.month <= version_month_to
JOIN {{ref('monthly_percentages')}} mp on sm.month = mp.month
LEFT JOIN {{ref('five_weeks_months')}} fw ON sm.month = fw.month 

WHERE concat(sm.month, '-01') <= curdate()

ORDER BY sm.month DESC
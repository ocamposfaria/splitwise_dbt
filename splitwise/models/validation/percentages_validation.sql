SELECT
	sf.month,
    sf.expense_id,
    sf.category,
    sf.name,
    sf.percentage_juau as percentual_joao_lançado,
    mp.percentage_juau*100 as percentual_joao_real,
    sf.percentage_juau - mp.percentage_juau*100 as diferença_percentual,
    sf.percentage_juau/100*cost - mp.percentage_juau*cost as diferença_reais
FROM
    {{ref('splitwise_final')}} sf
LEFT JOIN {{ref('monthly_percentages')}} mp ON mp.`month` = sf.`month`
WHERE
    `source` = 'Nossa Residencia'
    and sf.percentage_juau - mp.percentage_juau*100 >= 0.5
    and sf.month > '2023-03'
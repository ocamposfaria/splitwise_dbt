{% set bonus_joao_por_mes = '0' %}
{% set bonus_lana_por_mes = '1408.15' %}
{% set mes_inicio = '"2023-01"' %}
{% set mes_fim = '"2023-12"' %}

WITH gastos_nossa_residencia_categorias_sem_compas AS (
	SELECT
		month, 
		sum(cost_juau) as gastos_joao, 
		sum(cost_lana) as gastos_lana
	FROM {{ref('splitwise_final')}} sf
	WHERE
		`month` between {{mes_inicio}} and {{mes_fim}}
		and source = 'Nossa Residencia'
		and category <> 'compras'
	GROUP BY 1),

calculos AS (SELECT
	m.month, 
    m.cost_juau as ganhos_joao,
    m.cost_lana as ganhos_lana,
    m.percentage_juau as percentual_joao,
    m.percentage_lana as percentual_lana,
    g.gastos_joao,
    g.gastos_lana,
    'fake com bonus  ' as transição,
    m.cost_juau + {{bonus_joao_por_mes}} as ganhos_joao_bonus,
    m.cost_lana + {{bonus_lana_por_mes}} as ganhos_lana_bonus,
    (m.cost_juau + {{bonus_joao_por_mes}})/(m.cost_juau + m.cost_lana + {{bonus_joao_por_mes}} + {{bonus_lana_por_mes}}) as percentual_joao_fake,
    (m.cost_lana + {{bonus_lana_por_mes}})/(m.cost_juau + m.cost_lana + {{bonus_joao_por_mes}} + {{bonus_lana_por_mes}}) as percentual_lana_fake,
    ((m.cost_juau + {{bonus_joao_por_mes}})/(m.cost_juau + m.cost_lana + {{bonus_joao_por_mes}} + {{bonus_lana_por_mes}}))*(g.gastos_joao + g.gastos_lana) as gastos_joao_fake,
    ((m.cost_lana + {{bonus_lana_por_mes}})/(m.cost_juau + m.cost_lana + {{bonus_joao_por_mes}} + {{bonus_lana_por_mes}}))*(g.gastos_joao + g.gastos_lana) as gastos_lana_fake,
    g.gastos_joao - (((m.cost_juau + {{bonus_joao_por_mes}})/(m.cost_juau + m.cost_lana + {{bonus_joao_por_mes}} + {{bonus_lana_por_mes}}))*(g.gastos_joao + g.gastos_lana)) as diff_joao,
    g.gastos_lana - (((m.cost_lana + {{bonus_lana_por_mes}})/(m.cost_juau + m.cost_lana + {{bonus_joao_por_mes}} + {{bonus_lana_por_mes}}))*(g.gastos_joao + g.gastos_lana)) as diff_lana
    
FROM {{ref('monthly_percentages')}} m
JOIN gastos_nossa_residencia_categorias_sem_compas g ON g.month = m.month),

resumo AS (SELECT
    sum(diff_joao) as soma_joao,
    sum(diff_lana) as soma_lana
FROM calculos)

SELECT
    if(soma_joao > 0, 'joão ganha ---->', 'lana ganha ---->') as decisão,
    if(soma_joao > 0, soma_joao, soma_lana) as valor
FROM resumo

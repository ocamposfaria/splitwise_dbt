{{ config(materialized='view') }}

SELECT 
	month, count(`name`)
FROM
	(SELECT 
		month, `name`
	FROM
	    {{ref('splitwise_final')}}
	WHERE 1=1 and source = 'Nossa Residencia' and
		(
			trim(`name`) = 'aluguel' 
			or trim(`name`) = 'condominio' 
			or trim(`name`) = 'seguro do carro' 
			or trim(`name`) = 'energia' 
			or trim(`name`) = 'internet' 
			or trim(`name`) LIKE '%IPTU%' 
			or trim(`name`) = 'agua' 
			or trim(`name`) = 'celular lana' 
			or trim(`name`) = 'gas' 
			or trim(`name`) = 'seguro incendio'
		)
        
        ) sq
GROUP BY 1
ORDER BY 1 DESC 
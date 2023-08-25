
  create view `bob`.`overall_forecast__dbt_tmp` as (
    

SELECT 
    `overall_costs`.`month` AS `month`,
    `overall_costs`.`group` AS `group`,
    `overall_costs`.`category` AS `category`,
    `overall_costs`.`cost_juau` AS `cost_juau`,
    `overall_costs`.`cost_lana` AS `cost_lana`,
    'future_estimated' AS `future_mode`
FROM
    `bob`.`overall_costs`
WHERE
    (CAST(CONCAT(`overall_costs`.`month`, '-01') AS DATE) <= (CURDATE() - INTERVAL 1 MONTH)) 
UNION ALL SELECT 
    `overall_costs_future_estimated`.`month` AS `month`,
    `overall_costs_future_estimated`.`group` AS `group`,
    `overall_costs_future_estimated`.`category` AS `category`,
    `overall_costs_future_estimated`.`cost_juau` AS `cost_juau`,
    `overall_costs_future_estimated`.`cost_lana` AS `cost_lana`,
    'future_estimated' AS `future_mode`
FROM
    `bob`.`overall_costs_future_estimated`
UNION ALL SELECT 
    `overall_costs`.`month` AS `month`,
    `overall_costs`.`group` AS `group`,
    `overall_costs`.`category` AS `category`,
    `overall_costs`.`cost_juau` AS `cost_juau`,
    `overall_costs`.`cost_lana` AS `cost_lana`,
    'future_planned' AS `future_mode`
FROM
    `bob`.`overall_costs`
WHERE
    (CAST(CONCAT(`overall_costs`.`month`, '-01') AS DATE) <= (CURDATE() - INTERVAL 1 MONTH)) 
UNION ALL SELECT 
    `overall_costs_future_planned`.`month` AS `month`,
    `overall_costs_future_planned`.`group` AS `group`,
    `overall_costs_future_planned`.`category` AS `category`,
    `overall_costs_future_planned`.`cost_juau` AS `cost_juau`,
    `overall_costs_future_planned`.`cost_lana` AS `cost_lana`,
    'future_planned' AS `future_mode`
FROM
    `bob`.`overall_costs_future_planned`
UNION ALL SELECT 
    `overall_costs`.`month` AS `month`,
    `overall_costs`.`group` AS `group`,
    `overall_costs`.`category` AS `category`,
    `overall_costs`.`cost_juau` AS `cost_juau`,
    `overall_costs`.`cost_lana` AS `cost_lana`,
    'future_limited' AS `future_mode`
FROM
    `bob`.`overall_costs`
WHERE
    (CAST(CONCAT(`overall_costs`.`month`, '-01') AS DATE) <= (CURDATE() - INTERVAL 1 MONTH)) 
UNION ALL SELECT 
    `overall_costs_future_limits`.`month` AS `month`,
    `overall_costs_future_limits`.`group` AS `group`,
    `overall_costs_future_limits`.`category` AS `category`,
    `overall_costs_future_limits`.`cost_juau` AS `cost_juau`,
    `overall_costs_future_limits`.`cost_lana` AS `cost_lana`,
    'future_limited' AS `future_mode`
FROM
    `bob`.`overall_costs_future_limits`
  );
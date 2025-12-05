-- Объединяем данные из 4-x источников через UNION ALL


DROP TABLE IF EXISTS tmp_sources;
CREATE TEMP TABLE tmp_sources AS 

SELECT 
  order_id,
  order_created_date,
  order_completion_date,
  order_status,
  craftsman_id,
  craftsman_name,
  craftsman_address,
  craftsman_birthday,
  craftsman_email,
  product_id,
  product_name,
  product_description,
  product_type,
  product_price,
  customer_id,
  customer_name,
  customer_address,
  customer_birthday,
  customer_email
FROM source1.craft_market_wide

UNION ALL

SELECT 
  order_id,
  order_created_date,
  order_completion_date,
  order_status,
  cmoc.craftsman_id,
  craftsman_name,
  craftsman_address,
  craftsman_birthday,
  craftsman_email,
  cmmp.product_id,
  product_name,
  product_description,
  product_type,
  product_price,
  customer_id,
  customer_name,
  customer_address,
  customer_birthday,
  customer_email
FROM source2.craft_market_masters_products cmmp
JOIN source2.craft_market_orders_customers cmoc ON
	cmoc.craftsman_id = cmmp.craftsman_id AND
	cmoc.product_id = cmmp.product_id

UNION ALL

SELECT 
  order_id,
  order_created_date,
  order_completion_date,
  order_status,
  cmo.craftsman_id,
  craftsman_name,
  craftsman_address,
  craftsman_birthday,
  craftsman_email,
  product_id,
  product_name,
  product_description,
  product_type,
  product_price,
  cmo.customer_id,
  customer_name,
  customer_address,
  customer_birthday,
  customer_email
FROM source3.craft_market_orders cmo 
JOIN source3.craft_market_craftsmans cmcr ON cmcr.craftsman_id = cmo.craftsman_id
JOIN source3.craft_market_customers cmcu ON cmcu.customer_id = cmo.customer_id

UNION ALL

SELECT 
  order_id,
  order_created_date,
  order_completion_date,
  order_status,
  craftsman_id,
  craftsman_name,
  craftsman_address,
  craftsman_birthday,
  craftsman_email,
  product_id,
  product_name,
  product_description,
  product_type,
  product_price,
  customers.customer_id,
  customer_name,
  customer_address,
  customer_birthday,
  customer_email
FROM external_source.craft_products_orders ecpo
JOIN external_source.customers using(customer_id);


-- Проверяем создание таблицы из всех источников
-- SELECT * FROM tmp_sources LIMIT 10; 
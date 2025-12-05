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

-- Обновление существующих записей и добавление новых в dwh.d_craftsmans
MERGE INTO dwh.d_craftsman d
USING (SELECT DISTINCT craftsman_name, craftsman_address, craftsman_birthday, craftsman_email FROM tmp_sources) t
ON d.craftsman_name = t.craftsman_name AND d.craftsman_email = t.craftsman_email
WHEN MATCHED THEN
  UPDATE SET
  	craftsman_address = t.craftsman_address, 
	craftsman_birthday = t.craftsman_birthday,
	load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
  VALUES (t.craftsman_name, t.craftsman_address, t.craftsman_birthday, t.craftsman_email, current_timestamp);

-- Обновление существующих записей и добавление новых dwh.d_products
MERGE INTO dwh.d_product d
USING (SELECT DISTINCT product_name, product_description, product_type, product_price FROM tmp_sources) t
ON d.product_name = t.product_name AND d.product_description = t.product_description AND d.product_price = t.product_price
WHEN MATCHED THEN
  UPDATE SET 
    product_type = t.product_type,
    load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_name, product_description, product_type, product_price, load_dttm)
  VALUES (t.product_name, t.product_description, t.product_type, t.product_price, current_timestamp);

-- Обновление существующих записей и добавление новых dwh.d_customer
MERGE INTO dwh.d_customer d
USING (SELECT DISTINCT customer_name, customer_address, customer_birthday, customer_email FROM tmp_sources) t
ON d.customer_name = t.customer_name AND d.customer_email = t.customer_email
WHEN MATCHED THEN
  UPDATE SET 
  	customer_address = t.customer_address,
  	customer_birthday = t.customer_birthday,
  	load_dttm = current_timestamp
WHEN NOT MATCHED THEN
    INSERT (customer_name, customer_address, customer_birthday, customer_email, load_dttm)
    VALUES (t.customer_name, t.customer_address, t.customer_birthday, t.customer_email, current_timestamp);


-- Создание временной таблицы tmp_sources_fact, откуда уже будем брать данные для f_order
DROP TABLE IF EXISTS tmp_sources_fact;
CREATE TEMP TABLE tmp_sources_fact AS 
SELECT  dp.product_id,
		dc.craftsman_id,
		dcust.customer_id,
		src.order_created_date,
		src.order_completion_date,
		src.order_status,
		current_timestamp 
FROM tmp_sources src
JOIN dwh.d_craftsman dc ON dc.craftsman_name = src.craftsman_name AND dc.craftsman_email = src.craftsman_email 
JOIN dwh.d_customer dcust ON dcust.customer_name = src.customer_name AND dcust.customer_email = src.customer_email 
JOIN dwh.d_product dp ON dp.product_name = src.product_name AND dp.product_description = src.product_description AND dp.product_price = src.product_price;


-- Обновление существующих записей и добавление новых dwh.f_order
MERGE INTO dwh.f_order f
USING tmp_sources_fact t
ON f.product_id = t.product_id AND f.craftsman_id = t.craftsman_id AND f.customer_id = t.customer_id AND f.order_created_date = t.order_created_date 
WHEN MATCHED THEN
  UPDATE SET 
  	order_completion_date = t.order_completion_date,
  	order_status = t.order_status,
  	load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
  VALUES (t.product_id, t.craftsman_id, t.customer_id, t.order_created_date, t.order_completion_date, t.order_status, current_timestamp);


-- Создаем пустую витрину
DROP TABLE IF EXISTS dwh.customer_report_datamart;
CREATE TABLE IF NOT EXISTS dwh.customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор записи
    customer_id BIGINT NOT NULL, -- идентификатор заказчика
    customer_name VARCHAR NOT NULL, -- ФИО заказчика
    customer_address VARCHAR NOT NULL, -- его адрес
    customer_birthday DATE NOT NULL, -- дата рождения
    customer_email VARCHAR NOT NULL, -- электронная почта
    customer_money_spent NUMERIC(15,2) NOT NULL, -- сумма денег, которую потратил заказчик
    platform_money NUMERIC(15,2) NOT NULL, -- сумма денег, которая заработала платформа с его заказов(10% общей суммы его трат)
    count_order BIGINT NOT NULL, -- количество заказов 
    avg_price_order NUMERIC(10,2) NOT NULL, -- средняя стоимость одного заказа
    median_time_order_completed NUMERIC(10,1), -- медианное время в днях от момента создания заказа до его завершения за месяц
    top_product_category VARCHAR NOT NULL, -- самая популярная категория товаров у этого заказчика
    favourite_craftsman  BIGINT NOT NULL, -- идентификатор самого популярного мастера у заказчика
    count_order_created BIGINT NOT NULL, -- количество созданных заказов
    count_order_in_progress BIGINT NOT NULL, -- количество заказов в процессе за месяц
    count_order_delivery BIGINT NOT NULL, -- количество заказов в доставке за месяц
    count_order_done BIGINT NOT NULL, -- количество завершённых заказов за месяц
    count_order_not_done BIGINT NOT NULL, -- количество незавершённых заказов за месяц
    report_period VARCHAR NOT null, -- отчётный период (год и месяц)
    CONSTRAINT customer_report_datamart_pk PRIMARY KEY (customer_id, report_period )
);

-- Создаем таблицу с временем обновления витрины
DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;
CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
id bigint GENERATED ALWAYS AS IDENTITY,
load_dttm DATE NOT NULL,
CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id)
);

-- Определяем какие данные были изменены в витрине данных или добавлены в DWH 
WITH customer_delta AS (
	SELECT 	
		dcs.customer_id AS customer_id,
		dcs.customer_name AS customer_name,
		dcs.customer_address AS customer_address,
		dcs.customer_birthday AS customer_birthday,
		dcs.customer_email AS customer_email,
        fo.order_id AS order_id,
		dp.product_id AS product_id,
		dp.product_price AS product_price,
		dp.product_type AS product_type,
		dc.craftsman_id,
		fo.order_completion_date - fo.order_created_date AS diff_order_date, 
		fo.order_status AS order_status,
		to_char(fo.order_created_date, 'yyyy-mm') as report_period,
		dc.load_dttm as craftsman_load_dttm,
		dcs.load_dttm as customers_load_dttm,
        dp.load_dttm as products_load_dttm
	FROM dwh.f_order fo 
		INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id
		INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
		INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id
	WHERE 
		fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart) OR
        dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart) OR
		dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart) OR
        dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)
                
	),
customers_metrics AS (  -- Основные метрики
	SELECT
		customer_id,
        customer_name,                 
        customer_address,           
        customer_birthday,              
        customer_email,                
        report_period,		
		SUM(product_price) as customer_money_spent,
		SUM(product_price) * 0.1 as platform_money,
		COUNT(DISTINCT order_id) as count_order,
		AVG(product_price) as avg_price_order,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) as median_time_order_completed, 
		SUM(case when order_status = 'created' then 1 else 0 end) as count_order_created, 
		SUM(case when order_status = 'in progress' then 1 else 0 end) as count_order_in_progress,
		SUM(case when order_status = 'delivery' then 1 else 0 end) as count_order_delivery,
		SUM(case when order_status = 'done' then 1 else 0 end) as count_order_done,
		SUM(case when order_status != 'done' then 1 else 0 end) as count_order_not_done
	FROM customer_delta 
	GROUP BY customer_id, customer_name, customer_address, customer_birthday, customer_email, report_period
),
-- Любимый мастер
favourite_craftsman AS (
    SELECT DISTINCT ON (customer_id, report_period)
        customer_id,
        report_period,
        craftsman_id as favourite_craftsman
    FROM (
        SELECT 
            customer_id,
            report_period,
            craftsman_id,
            COUNT(DISTINCT order_id) as order_count
        FROM customer_delta
        GROUP BY customer_id, report_period, craftsman_id
    ) as masters_orders
    ORDER BY  -- Порядок вывода критичен для корректной первой строки
        customer_id, 
        report_period, 
        order_count DESC,  
        craftsman_id
 ),
   -- Любимая категория продуктов
   favourite_product_type AS (
    SELECT DISTINCT ON (customer_id, report_period)
        customer_id,
        report_period,
        product_type as favourite_product_type
    FROM (
        SELECT 
            customer_id,
            report_period,
            product_type,
            COUNT(DISTINCT order_id) as order_count
        FROM customer_delta
        GROUP BY customer_id, report_period, product_type
    ) as category_orders
    ORDER BY  -- Порядок вывода критичен для корректной первой строки
        customer_id, 
        report_period, 
        order_count DESC,  
        product_type  
),

-- Итоговые данные на заливку 
final_data AS (
    SELECT 
        m.customer_id,
        m.customer_name,
        m.customer_address,
        m.customer_birthday,
        m.customer_email,
        m.report_period,
        m.customer_money_spent,
        m.platform_money,
        m.count_order,
        m.avg_price_order,
        m.median_time_order_completed,
        m.count_order_created,
        m.count_order_in_progress,
        m.count_order_delivery,
        m.count_order_done,
        m.count_order_not_done,
        fc.favourite_craftsman,
        fpt.favourite_product_type as top_product_category
    FROM customers_metrics m
    LEFT JOIN favourite_craftsman fc 
        ON m.customer_id = fc.customer_id 
        AND m.report_period = fc.report_period
    LEFT JOIN favourite_product_type fpt 
        ON m.customer_id = fpt.customer_id 
        AND m.report_period = fpt.report_period
)


-- Загрузка данных в витрину
MERGE INTO dwh.customer_report_datamart AS target
    USING final_data AS source
    ON target.customer_id = source.customer_id 
       AND target.report_period = source.report_period
    WHEN MATCHED THEN
        UPDATE SET 
            customer_name = source.customer_name,
            customer_address = source.customer_address,
            customer_birthday = source.customer_birthday,
            customer_email = source.customer_email,
            customer_money_spent = source.customer_money_spent,
            platform_money = source.platform_money,
            count_order = source.count_order,
            avg_price_order = source.avg_price_order,
            median_time_order_completed = source.median_time_order_completed,
            top_product_category = source.top_product_category,
            favourite_craftsman = source.favourite_craftsman,
            count_order_created = source.count_order_created,
            count_order_in_progress = source.count_order_in_progress,
            count_order_delivery = source.count_order_delivery,
            count_order_done = source.count_order_done,
            count_order_not_done = source.count_order_not_done
    WHEN NOT MATCHED THEN
        INSERT (
            customer_id,
            customer_name,
            customer_address,
            customer_birthday,
            customer_email,
            customer_money_spent,
            platform_money,
            count_order,
            avg_price_order,
            median_time_order_completed,
            top_product_category,
            favourite_craftsman,
            count_order_created,
            count_order_in_progress,
            count_order_delivery,
            count_order_done,
            count_order_not_done,
            report_period
        ) VALUES (
            source.customer_id,
            source.customer_name,
            source.customer_address,
            source.customer_birthday,
            source.customer_email,
            source.customer_money_spent,
            source.platform_money,
            source.count_order,
            source.avg_price_order,
            source.median_time_order_completed,
            source.top_product_category,
            source.favourite_craftsman,
            source.count_order_created,
            source.count_order_in_progress,
            source.count_order_delivery,
            source.count_order_done,
            source.count_order_not_done,
            source.report_period
);

-- Записываем время последних обновленных данных
INSERT INTO dwh.load_dates_customer_report_datamart (load_dttm)
SELECT COALESCE(MAX(GREATEST(
    (SELECT MAX(load_dttm) FROM dwh.d_craftsman),
    (SELECT MAX(load_dttm) FROM dwh.d_customer),
    (SELECT MAX(load_dttm) FROM dwh.d_product),
    (SELECT MAX(load_dttm) FROM dwh.f_order)
)), NOW());

SELECT 'customer_report_datamart increment update completed';

--Проверка витрины
select * from dwh.customer_report_datamart order by report_period desc limit 5 


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
--select * from dwh.customer_report_datamart order by report_period desc limit 5 
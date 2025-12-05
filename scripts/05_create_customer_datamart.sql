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
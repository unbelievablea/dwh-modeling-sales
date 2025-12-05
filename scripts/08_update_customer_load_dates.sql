-- Записываем время последних обновленных данных
INSERT INTO dwh.load_dates_customer_report_datamart (load_dttm)
SELECT COALESCE(MAX(GREATEST(
    (SELECT MAX(load_dttm) FROM dwh.d_craftsman),
    (SELECT MAX(load_dttm) FROM dwh.d_customer),
    (SELECT MAX(load_dttm) FROM dwh.d_product),
    (SELECT MAX(load_dttm) FROM dwh.f_order)
)), NOW());
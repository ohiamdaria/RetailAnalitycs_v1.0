
-- set 1 if periods, else - set 2


-- Параметры функции:
--
-- метод расчета среднего чека (1 - за период, 2 - за количество)
-- первая и последняя даты периода (для 1 метода)
-- количество транзакций (для 2 метода)
-- коэффициент увеличения среднего чека
-- максимальный индекс оттока
-- максимальная доля транзакций со скидкой (в процентах)
-- допустимая доля маржи (в процентах)
-- CREATE FUNCTION average_check(method integer, first_date date, last_date date, count_of_transactions integer,
--                               coefficient_of_increase_check float, max_churn_rate float,
--                               max_share_of_transactions_with_discount float, acceptable_margin_share float)

-- CREATE MATERIALIZED VIEW information AS (
-- SELECT sp.customer_id,
--                                       sp.group_id,
--                                       sp.transaction_id,
--                                       sp.sku_summ_paid - sp.sku_discount AS sku_total_price,
--                                       sp.transaction_datetime,
--                                       gr.group_affinity_index::numeric,
--                                       gr.group_discount_share::numeric,
--                                       gr.group_churn_rate::numeric,
--                                       gr.group_margin::numeric
--                                FROM public.support_view AS sp
--                                INNER JOIN public.groups AS gr ON (sp.customer_id = gr.customer_id AND sp.group_id = gr.group_id)
-- );



SELECT * FROM date_of_analysis_formation;
DROP FUNCTION average_check(method integer, first_date date, last_date date, coefficient_of_increase_check numeric, max_churn_rate float);
DROP FUNCTION average_check(method integer, first_date date, last_date date, coefficient_of_increase_check numeric, max_churn_rate numeric,
                               max_share_of_transactions_with_discount numeric, acceptable_margin_share numeric);
SELECT public.average_check(1, '2018-08-21', '2021-08-21', 2.345, 12, 2, 10);
-- CREATE FUNCTION average_check(method integer, first_date date, last_date date, coefficient_of_increase_check numeric, max_churn_rate float)
CREATE FUNCTION average_check(method integer, first_date date, last_date date, coefficient_of_increase_check numeric, max_churn_rate numeric,
                               max_share_of_transactions_with_discount numeric, acceptable_margin_share numeric)
RETURNS TABLE
            (
--                 Customer_ID BIGINT,
--                 Required_Check_Measure float,
--                 Group_Name text,
--                 Offer_Discount_Depth float

                    customer_id BIGINT,
                    group_id BIGINT,
--                     transcationd_id BIGINT,
--                     acceptable_margin_share2 numeric,
                    group_affinity_index numeric
--                     group_discount_share numeric,
--                     group_churn_rate numeric,
--                     group_margin numeric
            )
AS
$$
DECLARE
BEGIN
    set session my.vars.id = method;
--    IF first_date < public.date_of_analysis_formation.analysis_formation THEN first_date = public.date_of_analysis_formation.analysis_formation;
--     IF last_date > CURRENT_DATE THEN last_date = current_date;
    RETURN QUERY (
        -- для первого пункта - поставить триггер на изменения переменной, чтобы изменялось представление группы? как сделать с миним затратами по времени

            WITH Method_of_Count AS (SELECT public.support_view.customer_id,
                                      public.support_view.group_id,
                                      public.support_view.transaction_id,
                                      public.support_view.sku_summ_paid - public.support_view.sku_discount AS sku_total_price
                               FROM public.support_view
                               WHERE (CASE WHEN method = 1 THEN public.support_view.transaction_datetime >= first_date AND public.support_view.transaction_datetime <= last_date
                                   END)),
            target_value_of_average_check AS (SELECT Method_of_Count.customer_id,
                                     Method_of_Count.group_id,
                                     SUM(Method_of_Count.sku_total_price) / COUNT(Method_of_Count.transaction_id) * coefficient_of_increase_check AS Average_Check
                               FROM Method_of_Count
                               GROUP BY Method_of_Count.customer_id, Method_of_Count.group_id),
--             -- target_value_of_average_check - 3 пункт
--
            group_for AS (SELECT  gr.customer_id,
                                          gr.group_id,
                                          gr.group_margin * 0.2 ::numeric,
                                          MAX(gr.group_affinity_index)::numeric AS group_affinity_index
                                  FROM groups AS gr
--                                   RIGHT JOIN target_value_of_average_check ON (gr.customer_id = target_value_of_average_check.customer_id AND
--                                                                                gr.group_id = target_value_of_average_check.group_id)
                                  WHERE gr.group_churn_rate <= 10 AND gr.group_discount_share < 10
                                  GROUP BY gr.customer_id, gr.group_id, gr.group_margin)
--             average_margin_group AS (SELECT group_for.customer_id,
--                                             group_for.group_id,
--                                             public.groups.group_margin * acceptable_margin_share::numeric
--                                      FROM group_for INNER JOIN public.groups ON (public.groups.customer_id = group_for.customer_id AND
--                                                                                public.groups.group_id = group_for.group_id))

            -- group_for - 4 пункт
            -- актуальная маржа по группе и средняя маржа - это одно и то же?
            SELECT * FROM group_for);
END;
$$
LANGUAGE plpgsql;


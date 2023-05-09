
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
--
--
-- Определение условия предложения
--
--
-- Выбор метода расчета среднего чека. Существует возможность
-- выбора метода расчета среднего чека – за определенный период времени
-- или за определенное количество последних транзакций. Метод расчета
-- вручную определяется пользователем.
--
--
-- Пользователь выбирает методику расчета по периоду, после чего
-- указывает первую и последнюю даты периода, за который
-- необходимо рассчитать средний чек для всей совокупности
-- клиентов, попавших в выборку. При этом последняя дата
-- указываемого периода должна быть позже первой, а указанный
-- период должен быть внутри общего анализируемого периода. В
-- случае указания слишком ранней или слишком поздней даты
-- система автоматически подставляет дату, соответственно, начала
-- или окончания анализируемого периода. Для расчета учитываются
-- все транзакции, совершенные каждым конкретным клиентом в
-- течение заданного периода.
--
--
-- Пользователь выбирает методику расчета по количеству последних
-- транзакций, после чего вручную указывает количество
-- транзакций, по которым необходимо рассчитать средний чек. Для
-- расчета среднего чека берется заданное пользователем
-- количество транзакций, начиная с самой последней в обратном
-- хронологическом порядке. В случае, если каким-либо клиентом из
-- выборки за весь анализируемый период совершено меньше
-- указанного количества транзакций, для анализа используется
-- имеющееся количество транзакций.
-- CREATE FUNCTION average_check(method integer, first_date date, last_date date, count_of_transactions integer,
--                               coefficient_of_increase_check float, max_churn_rate float,
--                               max_share_of_transactions_with_discount float, acceptable_margin_share float)
SELECT * FROM date_of_analysis_formation;
DROP FUNCTION average_check(method integer, first_date date, last_date date, coefficient_of_increase_check float);
SELECT public.average_check(1, '2018-08-21', '2021-08-21', 2.345);
CREATE FUNCTION average_check(method integer, first_date date, last_date date, coefficient_of_increase_check numeric, max_churn_rate float)
RETURNS TABLE
            (
--                 Customer_ID BIGINT,
--                 Required_Check_Measure float,
--                 Group_Name text,
--                 Offer_Discount_Depth float

                    customer_id BIGINT,
                    group_id BIGINT,
                    Average_Check numeric
            )
AS
$$
DECLARE
BEGIN
    set session my.vars.id = method;
--    IF first_date < public.date_of_analysis_formation.analysis_formation THEN first_date = public.date_of_analysis_formation.analysis_formation;
--     IF last_date > CURRENT_DATE THEN last_date = current_date;
    RETURN QUERY (
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
            sort_churn_rate AS (SELECT target_value_of_average_check.customer_id,
                                       target_value_of_average_check.group_id
                                FROM target_value_of_average_check INNER JOIN public.groups ON
                                      (target_value_of_average_check.customer_id = public.groups.customer_id AND target_value_of_average_check.group_id = public.groups.group_id)
                                WHERE public.groups.group_churn_rate < ),
            max_affinity_index AS (SELECT target_value_of_average_check.customer_id,
                                         -- target_value_of_average_check.group_id,
                                         MAX(public.groups.Group_Affinity_Index)
                                   FROM target_value_of_average_check INNER JOIN public.groups ON
                                      (target_value_of_average_check.customer_id = public.groups.customer_id AND target_value_of_average_check.group_id = public.groups.group_id)
                                  GROUP BY target_value_of_average_check.customer_id)
            SELECT * FROM max_affinity_index);
END;
$$
LANGUAGE plpgsql;

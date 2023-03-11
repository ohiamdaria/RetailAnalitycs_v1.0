-- Group_ID add, Group_Affinity_Index add

--  WITH group_id AS
--   (SELECT DISTINCT personal_data.customer_id,
--                          sku.group_id
--                     FROM personal_data INNER JOIN cards ON personal_data.customer_id = cards.customer_id
--                     INNER JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
--                     INNER JOIN checks ON transactions.transaction_id = checks.transaction_id
--                     INNER JOIN sku ON checks.sku_id = sku.sku_id
--     ORDER BY 1, 2)


-- CREATE MATERIALIZED VIEW groups AS (...)
-- CREATE OR REPLACE FUNCTION refresh_mat_view()
-- RETURNS TRIGGER LANGUAGE plpgsql
-- AS $$
-- BEGIN
--     REFRESH MATERIALIZED VIEW groups;
--     RETURN NULL;
-- end $$;
--
-- CREATE TRIGGER refresh_mat_view_personal_data
-- AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
-- ON personal_data FOR EACH STATEMENT
-- EXECUTE PROCEDURE refresh_mat_view();
--
-- CREATE TRIGGER refresh_mat_view_transactions
-- AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
-- ON transactions FOR EACH STATEMENT
-- EXECUTE PROCEDURE refresh_mat_view();
--
-- CREATE TRIGGER refresh_mat_view_checks
-- AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
-- ON checks FOR EACH STATEMENT
-- EXECUTE PROCEDURE refresh_mat_view();
--
-- CREATE TRIGGER refresh_mat_view_sku
-- AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
-- ON sku FOR EACH STATEMENT
-- EXECUTE PROCEDURE refresh_mat_view();

WITH group_id AS
  (SELECT DISTINCT personal_data.customer_id,
                         sku.group_id,
                         COUNT(transactions.transaction_id) AS count_of_group,
                         MAX(h."Transaction_DateTime") AS Last_Transaction_DateTime
                    FROM personal_data INNER JOIN cards ON personal_data.customer_id = cards.customer_id
                    INNER JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
                    INNER JOIN checks ON transactions.transaction_id = checks.transaction_id
                    INNER JOIN sku ON checks.sku_id = sku.sku_id
                    INNER JOIN periods p on cards.customer_id = p."Customer_ID"
                    INNER JOIN history h on checks.transaction_id = h."Transaction_ID"
                    WHERE transactions.transaction_datetime >= p."First_Group_Purchase_Date"
                          AND
                          transactions.transaction_datetime <= p."Last_Group_Purchase_Date"
                    GROUP BY personal_data.customer_id, sku.group_id
    ORDER BY 1, 2),
    all_groups AS (SELECT DISTINCT personal_data.customer_id,
                         COUNT(transactions.transaction_id) AS count_all
                    FROM personal_data INNER JOIN cards ON personal_data.customer_id = cards.customer_id
                    INNER JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
--                     INNER JOIN checks ON transactions.transaction_id = checks.transaction_id need?
                    INNER JOIN periods p on cards.customer_id = p."Customer_ID"
                    WHERE transactions.transaction_datetime >= p."First_Group_Purchase_Date"
                          AND
                          transactions.transaction_datetime <= p."Last_Group_Purchase_Date"
                    GROUP BY personal_data.customer_id
                    ORDER BY 1),
    Group_Affinity_Index AS (SELECT group_id.customer_id,
       group_id.count_of_group::float/all_groups.count_all::float
       FROM group_id INNER JOIN all_groups ON group_id.customer_id = all_groups.customer_id),
    Group_Churn_Rate AS (SELECT group_id.customer_id,
                         group_id.group_id,
                         (CASE WHEN periods."Group_Frequency" = 0 THEN 0
                         ELSE
                             (SELECT EXTRACT(DAY FROM (date_of_analysis_formation.analysis_formation - Last_Transaction_DateTime)))/periods."Group_Frequency"
                         -- if paste "::date" then the number of days increases by one
                         END)
                         FROM date_of_analysis_formation, group_id
                         INNER JOIN periods ON group_id.customer_id = periods."Customer_ID"
                         WHERE group_id.group_id = periods."Group_ID")
                        SELECT *FROM Group_Churn_Rate;

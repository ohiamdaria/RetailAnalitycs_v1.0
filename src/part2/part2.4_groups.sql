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

WITH base1  AS (SELECT DISTINCT personal_data.customer_id,
                      sku.group_id,
                      p."Group_Frequency",
                      checks.transaction_id,
                      h2."Transaction_DateTime"
                FROM personal_data INNER JOIN cards ON personal_data.customer_id = cards.customer_id
                    INNER JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
                    INNER JOIN checks ON transactions.transaction_id = checks.transaction_id
                    INNER JOIN sku ON checks.sku_id = sku.sku_id
                    INNER JOIN periods p on cards.customer_id = p."Customer_ID"
                    INNER JOIN history h2 on checks.transaction_id = h2."Transaction_ID"
                WHERE transactions.transaction_datetime >= p."First_Group_Purchase_Date"
                                          AND
                                          transactions.transaction_datetime <= p."Last_Group_Purchase_Date"
                ORDER BY 1, 2, 5),
    base AS (SELECT DISTINCT personal_data.customer_id,
                                                sku.group_id,
                                                transactions.transaction_id AS transaction_id,
                                                h2."Transaction_DateTime" AS Transaction_DateTime
                                FROM personal_data INNER JOIN cards ON personal_data.customer_id = cards.customer_id
                                INNER JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
                                INNER JOIN checks ON transactions.transaction_id = checks.transaction_id
                                INNER JOIN sku ON checks.sku_id = sku.sku_id
                                INNER JOIN periods p on cards.customer_id = p."Customer_ID"
                                INNER JOIN history h2 on checks.transaction_id = h2."Transaction_ID"
                                WHERE transactions.transaction_datetime >= p."First_Group_Purchase_Date"
                                          AND
                                          transactions.transaction_datetime <= p."Last_Group_Purchase_Date"
                                ORDER BY 1, 2, 4),
    all_groups AS (SELECT customer_id,
                          COUNT(transaction_id) AS count_all_transacations
                   FROM base
                   GROUP BY customer_id),
    Group_Affinity_Index AS (SELECT base.customer_id,
                        base.group_id,
                        COUNT(transaction_id)::float / count_all_transacations::float AS count_transactions_of_group
                 FROM base
                 INNER JOIN all_groups ON base.customer_id = all_groups.customer_id
                 GROUP BY base.customer_id, base.group_id, all_groups.count_all_transacations),
    Max_Transaction_DateTime AS (
        SELECT h."Group_ID",
               (SELECT
            EXTRACT(DAY FROM (date_of_analysis_formation.analysis_formation -  MAX(h."Transaction_DateTime"))))::float AS Max_Transaction_DateTime
        FROM date_of_analysis_formation, history h

        GROUP BY h."Group_ID", date_of_analysis_formation.analysis_formation
    ),
    Group_Churn_Rate AS (SELECT base.customer_id,
                                base.group_id,
                                Max_Transaction_DateTime.Max_Transaction_DateTime,
                                p."Group_Frequency",
                     (CASE WHEN p."Group_Frequency" = 0 THEN 0
                     ELSE
                         Max_Transaction_DateTime.Max_Transaction_DateTime/p."Group_Frequency"
                     -- if paste "::date" then the number of days increases by one
                     END) AS Group_Churn_Rate
                     FROM Max_Transaction_DateTime, base
                     INNER JOIN periods p on base.customer_id = p."Customer_ID"
                     WHERE base.group_id = p."Group_ID"
                     ORDER BY 1, 2),
    intervals_date AS (SELECT base.customer_id,
                         base.group_id,
                         base.transaction_id,
                         (Transaction_DateTime - (LAG(Transaction_DateTime) OVER
                            (PARTITION BY customer_id, group_id
                             ORDER BY customer_id, group_id, Transaction_DateTime))) AS intervals
                  FROM base),
    intervals AS (SELECT intervals_date.customer_id,
                         intervals_date.group_id,
                         periods."Group_Frequency" AS group_frequency,
                         intervals_date.transaction_id,
                         (SELECT EXTRACT(DAY FROM(intervals_date.intervals))) - periods."Group_Frequency" AS abs_deviation
                  FROM intervals_date
                  INNER JOIN periods ON periods."Customer_ID" = intervals_date.customer_id
                      WHERE periods."Group_ID" = intervals_date.customer_id),
    abs_deviation_plus AS (SELECT customer_id,
                             group_id,
                             CASE WHEN group_frequency = 0 THEN 0
                             ELSE (CASE WHEN abs_deviation < 0 THEN abs_deviation * (-1)
                             ELSE abs_deviation
                             END)::float / group_frequency::float
                             END AS relative_deviation
                           FROM intervals),
    Stability_Index AS (SELECT customer_id,
                             group_id,
                             AVG(relative_deviation)
                        FROM abs_deviation_plus
                        GROUP BY customer_id, group_id),
    


    SELECT * FROM Stability_Index
    ORDER BY 1, 2;

-- SELECT * FROM intervals;

--                       MAX(h."Transaction_DateTime") AS Last_Transaction_DateTime

-- add Group_ID, Group_Affinity_Index, Group_Churn_Rate











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
                         WHERE group_id.group_id = periods."Group_ID"),
    Group_Stability_Index AS   (SELECT DISTINCT personal_data.customer_id,
                                                sku.group_id,
                                                transactions.transaction_id AS transaction_id,
                                                h2."Transaction_DateTime" AS Transaction_DateTime
                                FROM personal_data INNER JOIN cards ON personal_data.customer_id = cards.customer_id
                                INNER JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
                                INNER JOIN checks ON transactions.transaction_id = checks.transaction_id
                                INNER JOIN sku ON checks.sku_id = sku.sku_id
                                INNER JOIN history h2 on checks.transaction_id = h2."Transaction_ID"
                                ORDER BY 1, 2, 4),
    intervals AS (SELECT customer_id,
                         group_id,
                         transaction_id,
                         Transaction_DateTime - (LAG(Transaction_DateTime) OVER
                            (PARTITION BY customer_id, group_id
                             ORDER BY customer_id, group_id, Transaction_DateTime)) AS intervals
                  FROM base
                  ),
    abs_deviation AS (SELECT customer_id,
                             group_id,
                             (SELECT EXTRACT(DAY FROM(intervals))) - periods."Group_Frequency" AS abs_deviation,
                             periods."Group_Frequency" as group_frequency
                      FROM intervals INNER JOIN periods ON periods."Customer_ID" = intervals.customer_id
                      WHERE periods."Group_ID" = intervals.customer_id),
    abs_deviation_plus AS (SELECT customer_id,
                             group_id,
                             CASE WHEN group_frequency = 0 THEN 0
                             ELSE (CASE WHEN abs_deviation < 0 THEN abs_deviation * (-1)
                             ELSE abs_deviation
                             END)::float / group_frequency::float
                             END AS relative_deviation
                           FROM abs_deviation),
    Stability_Index AS (SELECT customer_id,
                             group_id,
                             AVG(relative_deviation)
                        FROM abs_deviation_plus
                        GROUP BY customer_id, group_id)
    SELECT * FROM Stability_Index;
-- SELECT * FROM Group_Stability_Index


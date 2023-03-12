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
    max_transaction_datetime AS (
        SELECT h."Group_ID",
               (SELECT
            EXTRACT(DAY FROM (date_of_analysis_formation.analysis_formation -  MAX(h."Transaction_DateTime"))))::float AS max_transaction_datetime
        FROM date_of_analysis_formation, history h

        GROUP BY h."Group_ID", date_of_analysis_formation.analysis_formation
    ),
    Group_Churn_Rate AS (SELECT base.customer_id,
                                base.group_id,
                                max_transaction_datetime.max_transaction_datetime,
                                p."Group_Frequency",
                     (CASE WHEN p."Group_Frequency" = 0 THEN 0
                     ELSE
                         max_transaction_datetime.max_transaction_datetime/p."Group_Frequency"
                     -- if paste "::date" then the number of days increases by one
                     END) AS Group_Churn_Rate
                     FROM max_transaction_datetime, base
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
    intervals_days AS (SELECT intervals_date.customer_id,
                         intervals_date.group_id,
                         periods."Group_Frequency" AS group_frequency,
                         intervals_date.transaction_id,
                         (SELECT EXTRACT(DAY FROM(intervals_date.intervals))) - periods."Group_Frequency" AS abs_deviation
                  FROM intervals_date
                  INNER JOIN periods ON periods."Customer_ID" = intervals_date.customer_id
                      WHERE periods."Group_ID" = intervals_date.group_id),
    abs_deviation_plus AS (SELECT customer_id,
                             group_id,
                             CASE WHEN group_frequency = 0 THEN 0
                             ELSE (CASE WHEN abs_deviation < 0 THEN abs_deviation * (-1)
                             ELSE abs_deviation
                             END)::float / group_frequency::float
                             END AS relative_deviation
                           FROM intervals_days),
    Group_Stability_Index AS (SELECT customer_id,
                             group_id,
                             AVG(relative_deviation)
                        FROM abs_deviation_plus
                        GROUP BY customer_id, group_id),
    Group_Margin_period AS (SELECT base.customer_id,
                                   base.group_id,
                                   base.transaction_id,
                                   history."Group_Summ_Paid" - history."Group_Cost" AS Group_Margin
                            FROM date_of_analysis_formation, base
                            INNER JOIN history ON base.customer_id = history."Customer_ID"
                            WHERE base.group_id = history."Group_ID"
                              AND base.transaction_id = history."Transaction_ID"
                                  AND
                                  Transaction_DateTime <= date_of_analysis_formation.analysis_formation
                                  AND
                                  Transaction_DateTime >= date_of_analysis_formation.analysis_formation - interval '100 day' ),
    -- need to sum group margin if we have some products from one group?
    count_transactions AS (SELECT DISTINCT base.customer_id,
--                                    base.group_id,
                                   base.transaction_id,
                                   Transaction_DateTime
                            FROM date_of_analysis_formation, base
                            INNER JOIN history ON base.customer_id = history."Customer_ID"
                            WHERE base.group_id = history."Group_ID"
                                  AND
                                  Transaction_DateTime <= date_of_analysis_formation.analysis_formation
                            ORDER BY 1  ASC, 3 DESC),
    Group_Margin_count_transactions AS (SELECT x.customer_id,
                                               x.transaction_id,
                                               base.group_id,
                                               history."Group_Summ_Paid" - history."Group_Cost" AS Group_Margin
                                        FROM (SELECT
                                                     ROW_NUMBER() OVER (PARTITION BY customer_id ) AS r,
                                                     t.*
                                               FROM count_transactions t) x
                                        INNER JOIN base ON (x.customer_id = base.customer_id AND x.transaction_id = base.transaction_id)
                                        INNER JOIN history ON (x.customer_id = history."Customer_ID" AND base.group_id = history."Group_ID" AND x.transaction_id = history."Transaction_ID")
                                        WHERE x.r <= 2
                                        ORDER BY 1, 2), -- count of transactionsб
    transactions_with_discount AS (SELECT base.customer_id,
                                                base.group_id,
                                                base.transaction_id,
                                                SUM(sku_discount)
                                         FROM base INNER JOIN checks ON (base.transaction_id = checks.transaction_id)
                                         INNER JOIN periods ON (base.customer_id = periods."Customer_ID" AND base.group_id = periods."Group_ID")
                                         WHERE checks.sku_discount > 0
                                         GROUP BY base.customer_id, base.group_id, base.transaction_id
                                         ORDER BY 1, 2, 3),
    Group_Discount_Share AS (SELECT transactions_with_discount.customer_id,
                                                transactions_with_discount.group_id,
                                                COUNT(transactions_with_discount.transaction_id)::float/periods."Group_Purchase"::float
                                         FROM transactions_with_discount
                                         INNER JOIN periods ON (transactions_with_discount.customer_id = periods."Customer_ID" AND transactions_with_discount.group_id = periods."Group_ID")
                                         GROUP BY transactions_with_discount.customer_id, transactions_with_discount.group_id, periods."Group_Purchase"
                                         ORDER BY 1, 2),
    Group_Minimum_Discount AS (SELECT "Customer_ID",
                                      "Group_ID",
                                      "Group_Min_Discount"
                               FROM periods
                               WHERE "Group_Min_Discount" > 0),
    Group_Average_Discount AS (SELECT "Customer_ID",
                                      "Group_ID",
                                      "Group_Summ_Paid"::float/"Group_Summ"::float
                               FROM history)

SELECT * FROM Group_Average_Discount;

SELECT * FROM history
ORDER BY 1, 4;
SELECT * FROM periods;
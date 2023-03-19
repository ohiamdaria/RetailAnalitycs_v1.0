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
                    INNER JOIN checks ON transactions.transaction_id = checks.transaction_id
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
                  FROM Group_Stability_Index),
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

SELECT * FROM intervals;


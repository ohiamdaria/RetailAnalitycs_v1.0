DROP MATERIALIZED VIEW IF EXISTS customers;
DROP FUNCTION IF EXISTS end_date_for_analysis();
DROP FUNCTION IF EXISTS get_base_table();
DROP FUNCTION IF EXISTS get_base_customer_info();
DROP FUNCTION IF EXISTS get_primary_store_table();

--DROP TABLE IF EXISTS segment;
-- CALL import('Segment',',');

CREATE TABLE IF NOT EXISTS segment
(
    segment            BIGINT PRIMARY KEY,
    average_check      VARCHAR(20) NOT NULL,
    customer_frequency VARCHAR(20) NOT NULL,
    customer_churn     VARCHAR(20) NOT NULL
);

CREATE OR REPLACE FUNCTION end_date_for_analysis() RETURNS timestamp
AS
$$
BEGIN
    RETURN (SELECT MAX(analysis_formation)
            FROM date_of_analysis_formation);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_base_customer_info()
    RETURNS TABLE
            (
                customer_id           BIGINT,
                average_check         NUMERIC,
                frequency             DOUBLE PRECISION,
                inactive_period       DOUBLE PRECISION,
                sum_transaction_store BIGINT
            )
AS
$$
DECLARE
BEGIN
    RETURN QUERY (SELECT support_view.customer_id,
                         COALESCE(ROUND(SUM(transaction_summ) / NULLIF(COUNT(support_view.customer_id), 0)),
                                  0)                 AS average_check,
                         COALESCE(ROUND(extract(EPOCH FROM (MAX(transaction_datetime) - MIN(transaction_datetime)))::float
                             / 86400.0 / NULLIF(COUNT(transaction_id), 0)), 0)
                                                     AS frequency,
                         ROUND(extract(EPOCH FROM (end_date_for_analysis() - MAX(transaction_datetime)))::float /
                               86400.0)              AS inactive_period,
                         COUNT(transaction_store_id) AS sum_transaction_store
                  FROM support_view
                  GROUP BY support_view.customer_id);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_primary_store_table()
    RETURNS TABLE
            (
                customer_id BIGINT,
                store_id    BIGINT
            )
AS
$$
DECLARE
BEGIN
    RETURN QUERY (WITH uniq_customer_transaction_table AS (SELECT base.customer_id,
                                                                  base.transaction_store_id,
                                                                  COUNT(base.transaction_id) AS uniq_transaction
                                                           FROM support_view base
                                                           GROUP BY base.customer_id, base.transaction_store_id),
                       rank_store_table AS (SELECT base.customer_id,
                                                   base.transaction_datetime,
                                                   base.transaction_store_id,
                                                   RANK()
                                                   OVER (PARTITION BY base.customer_id ORDER BY base.transaction_datetime DESC) AS rank
                                            FROM support_view base),
                       last_three_store_table AS (SELECT rank_store_table.customer_id,
                                                         rank_store_table.transaction_store_id,
                                                         COUNT(rank_store_table.transaction_store_id) AS count
                                                  FROM rank_store_table
                                                  WHERE rank_store_table.rank <= 3
                                                  GROUP BY rank_store_table.customer_id,
                                                           rank_store_table.transaction_store_id),
                       part_store_table AS (SELECT uniq_customer_transaction_table.customer_id,
                                                   uniq_customer_transaction_table.transaction_store_id,
                                                   ROUND(COALESCE(
                                                               uniq_customer_transaction_table.uniq_transaction::float /
                                                               NULLIF(info.sum_transaction_store, 0),
                                                               0)) AS part
                                            FROM uniq_customer_transaction_table
                                                     LEFT JOIN support_view base ON base.transaction_store_id =
                                                                                        uniq_customer_transaction_table.transaction_store_id
                                                     LEFT JOIN get_base_customer_info() info
                                                               ON uniq_customer_transaction_table.customer_id = info.customer_id
                                            ORDER BY base.transaction_datetime DESC),
                       max_part_store_table AS (SELECT ps.customer_id,
                                                       ps.transaction_store_id,
                                                       MAX(part) AS max
                                                FROM part_store_table ps
                                                GROUP BY ps.customer_id, ps.transaction_store_id),
                       range_part_store_table AS (SELECT st.customer_id,
                                                         st.transaction_store_id,
                                                         RANK() OVER (PARTITION BY st.customer_id ORDER BY st.max DESC) AS runk,
                                                         (SELECT MAX(base.transaction_datetime)
                                                          FROM support_view base
                                                          WHERE base.transaction_store_id = st.transaction_store_id)    AS max_date
                                                  FROM max_part_store_table st),
                       primary_store
                           AS (SELECT DISTINCT ON (last_three_store_table.customer_id) last_three_store_table.customer_id,
                                                                                       CASE
                                                                                           WHEN last_three_store_table.count = 3
                                                                                               THEN last_three_store_table.transaction_store_id
                                                                                           WHEN rs.runk = 1
                                                                                               THEN rs.transaction_store_id
                                                                                           END AS store_id
                               FROM last_three_store_table
                                        JOIN (SELECT range_part_store_table.customer_id,
                                                     range_part_store_table.transaction_store_id,
                                                     range_part_store_table.max_date,
                                                     range_part_store_table.runk
                                              FROM range_part_store_table
                                              WHERE runk = 1
                                              ORDER BY max_date DESC) rs
                                             ON last_three_store_table.customer_id = rs.customer_id)
                  SELECT primary_store.customer_id,
                         primary_store.store_id
                  FROM primary_store);
END;
$$ LANGUAGE plpgsql;

CREATE MATERIALIZED VIEW customers
            (
             "Customer_ID",
             "Customer_Average_Check",
             "Customer_Average_Check_Segment",
             "Customer_Frequency",
             "Customer_Frequency_Segment",
             "Customer_Inactive_Period",
             "Customer_Churn_Rate",
             "Customer_Churn_Segment",
             "Customer_Segment",
             "Customer_Primary_Store")
AS
WITH churn_rate_table AS (SELECT customer_id,
                                 ROUND(COALESCE(inactive_period /
                                                NULLIF(frequency, 0), 0)) AS churn_rate
                          FROM get_base_customer_info()),
     all_segments_table AS (SELECT ci.customer_id,
                                   churn_rate_table.churn_rate,
                                   CASE
                                       WHEN (percent_rank() OVER (ORDER BY average_check DESC) <= 0.1) THEN 'High'
                                       WHEN (percent_rank() OVER (ORDER BY average_check DESC) <= 0.35) THEN 'Medium'
                                       ELSE 'Low' END    AS average_segment,
                                   CASE
                                       WHEN (percent_rank() OVER (ORDER BY frequency DESC) <= 0.1) THEN 'Often'
                                       WHEN (percent_rank() OVER (ORDER BY frequency DESC) <= 0.35) THEN 'Occasionally'
                                       ELSE 'Rarely' END AS frequency_segment,
                                   CASE
                                       WHEN churn_rate < 2 THEN 'Low'
                                       WHEN churn_rate >= 2 AND churn_rate < 5 THEN 'Medium'
                                       ELSE 'High' END   AS churn_segment
                            FROM get_base_customer_info() ci
                                     LEFT JOIN churn_rate_table ON ci.customer_id = churn_rate_table.customer_id
                            ORDER BY average_check DESC),
     number_segment_table AS (SELECT all_segments_table.customer_id,
                                     segment.segment,
                                     all_segments_table.average_segment,
                                     all_segments_table.frequency_segment,
                                     all_segments_table.churn_segment
                              FROM all_segments_table
                                       LEFT JOIN segment ON (segment.average_check = all_segments_table.average_segment
                                  AND segment.customer_frequency = all_segments_table.frequency_segment
                                  AND segment.customer_churn = all_segments_table.churn_segment))

SELECT base_info.customer_id,
       base_info.average_check,
       all_segments_table.average_segment,
       base_info.frequency,
       all_segments_table.frequency_segment,
       base_info.inactive_period,
       all_segments_table.churn_rate,
       all_segments_table.churn_segment,
       number_segment_table.segment,
       primary_store.store_id
FROM get_base_customer_info() base_info
         LEFT JOIN all_segments_table ON base_info.customer_id = all_segments_table.customer_id
         LEFT JOIN number_segment_table ON base_info.customer_id = number_segment_table.customer_id
         LEFT JOIN get_primary_store_table() primary_store ON base_info.customer_id = primary_store.customer_id
ORDER BY base_info.customer_id;


BEGIN;
SELECT "Customer_ID",
       "Customer_Average_Check",
       "Customer_Average_Check_Segment",
       "Customer_Frequency",
       "Customer_Frequency_Segment",
       "Customer_Inactive_Period",
       "Customer_Churn_Rate",
       "Customer_Churn_Segment",
       "Customer_Segment",
       "Customer_Primary_Store"
FROM customers;
END;

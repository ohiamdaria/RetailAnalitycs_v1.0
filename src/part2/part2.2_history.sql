CREATE VIEW history AS
SELECT customer_id                  AS "Customer_ID",
       transaction_id               AS "Transaction_ID",
       transaction_datetime         AS "Transaction_DateTime",
       group_id                     AS "Group_ID",
       ROUND(SUM(support_view.sku_purchase_price * support_view.sku_amount), 2)    AS "Group_Cost",
       ROUND(SUM(support_view.sku_summ), 2)      AS "Group_Summ",
       ROUND(SUM(support_view.sku_summ_paid), 2) AS "Group_Summ_Paid"
FROM support_view
GROUP BY customer_id,
         transaction_id,
         transaction_datetime,
         group_id;

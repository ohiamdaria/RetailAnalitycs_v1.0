CREATE VIEW history AS
SELECT customer_id                  AS "Customer_ID",
       transaction_id               AS "Transaction_ID",
       transaction_datetime         AS "Transaction_DateTime",
       group_id                     AS "Group_ID",
       ROUND(SUM(Group_Cost), 2)    AS "Group_Cost",
       ROUND(SUM(sku_summ), 2)      AS "Group_Summ",
       ROUND(SUM(sku_summ_paid), 2) AS "Group_Summ_Paid"
FROM support_view
GROUP BY customer_id,
         transaction_id,
         transaction_datetime,
         group_id;

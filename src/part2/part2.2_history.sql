CREATE VIEW history AS
WITH helpers
         AS
         (SELECT public.cards.customer_id,
                 public.transactions.transaction_id,
                 public.transactions.transaction_datetime,
                 public.sku.group_id,
                 public.checks.sku_summ,
                 public.checks.sku_summ_paid,
                 public.stores.sku_purchase_price * checks.sku_amount AS sku_cost
          FROM public.transactions
                   INNER JOIN cards on cards.customer_card_id = transactions.customer_card_id
                   INNER JOIN checks on transactions.transaction_id = checks.transaction_id
                   INNER JOIN sku on sku.sku_id = checks.sku_id
                   INNER JOIN stores on sku.sku_id = stores.sku_id AND
                                        stores.transaction_store_id = transactions.transaction_store_id)
SELECT customer_id                  AS "Customer_ID",
       transaction_id               AS "Transaction_ID",
       transaction_datetime         AS "Transaction_DateTime",
       group_id                     AS "Group_ID",
       ROUND(sku_cost, 2)      AS "Group_Cost",
       ROUND(sku_summ, 2)      AS "Group_Summ",
       ROUND(sku_summ_paid, 2) AS "Group_Summ_Paid"
FROM helpers;
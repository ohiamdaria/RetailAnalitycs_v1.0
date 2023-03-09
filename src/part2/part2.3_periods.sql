WITH helpers AS
    (
        SELECT
            cards.customer_id,
            sku.group_id,
            transactions.transaction_datetime,
            checks.sku_discount,
            checks.sku_summ,
            public.stores.sku_purchase_price * checks.sku_amount AS Group_Cost
        FROM transactions
        INNER JOIN cards on cards.customer_card_id = transactions.customer_card_id
        INNER JOIN sku on sku.sku_id = transactions.transaction_id
        INNER JOIN checks on sku.sku_id = checks.sku_id
            INNER JOIN stores on sku.sku_id = stores.sku_id AND stores.transaction_store_id = transactions.transaction_store_id
    )
SELECT
    helpers.customer_id AS "Customer_ID",
    helpers.group_id AS "Group_ID",
    MIN(helpers.transaction_datetime) AS "First_Group_Purchase_Date",
    MAX(helpers.transaction_datetime) AS "Last_Group_Purchase_Date",
    COUNT(group_id)  AS "Group_Purchase"
FROM helpers
GROUP BY customer_id, group_id;
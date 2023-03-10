CREATE MATERIALIZED VIEW history AS
WITH helpers
         AS
         (SELECT public.cards.customer_id,
                 public.transactions.transaction_id,
                 public.transactions.transaction_datetime,
                 public.sku.group_id,
                 public.checks.sku_summ,
                 public.checks.sku_summ_paid,
                 public.stores.sku_purchase_price * checks.sku_amount AS Group_Cost
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
       ROUND(SUM(Group_Cost), 2)    AS "Group_Cost",
       ROUND(SUM(sku_summ), 2)      AS "Group_Summ",
       ROUND(SUM(sku_summ_paid), 2) AS "Group_Summ_Paid"
FROM helpers
GROUP BY customer_id, transaction_id, transaction_datetime, group_id;

CREATE OR REPLACE FUNCTION update_history() RETURNS trigger AS
$$
BEGIN
    REFRESH MATERIALIZED VIEW history;
    RETURN NULL;
END;
$$
    LANGUAGE plpgsql;

CREATE TRIGGER update_history_trigger_transactions
    AFTER INSERT OR UPDATE OR DELETE
    ON public.transactions
    FOR EACH ROW
EXECUTE FUNCTION update_history();

CREATE TRIGGER update_history_trigger_sku
    AFTER INSERT OR UPDATE OR DELETE
    ON public.sku
    FOR EACH ROW
EXECUTE FUNCTION update_history();

CREATE TRIGGER update_history_trigger_cards
    AFTER INSERT OR UPDATE OR DELETE
    ON public.cards
    FOR EACH ROW
EXECUTE FUNCTION update_history();

CREATE TRIGGER update_history_trigger_checks
    AFTER INSERT OR UPDATE OR DELETE
    ON public.checks
    FOR EACH ROW
EXECUTE FUNCTION update_history();

CREATE TRIGGER update_history_trigger_stores
    AFTER INSERT OR UPDATE OR DELETE
    ON public.stores
    FOR EACH ROW
EXECUTE FUNCTION update_history();
CREATE MATERIALIZED VIEW periods AS
WITH helpers AS
         (SELECT public.transactions.transaction_datetime,
                 public.cards.customer_id,
                 public.sku.group_id,
                 public.checks.sku_summ,
                 public.checks.sku_discount
          FROM public.transactions
                   JOIN cards
                        ON cards.customer_card_id = transactions.customer_card_id
                   JOIN checks
                        ON checks.transaction_id = transactions.transaction_id
                   JOIN sku
                        ON sku.sku_id = checks.sku_id)
SELECT customer_id                                                                          AS "Customer_ID",
       group_id                                                                             AS "Group_ID",
       MIN(transaction_datetime)                                                            AS "First_Group_Purchase_Date",
       MAX(transaction_datetime)                                                            AS "Last_Group_Purchase_Date",
       COUNT(*)                                                                             AS "Group_Purchase",
       ((MAX(transaction_datetime)::date - MIN(transaction_datetime)::date) + 1) / COUNT(*) AS "Group_Frequency",
       ROUND(COALESCE(MIN(CASE
                              WHEN sku_discount = 0 THEN NULL
                              ELSE sku_discount / sku_summ END), 0), 2)                     AS "Group_Min_Discount"
FROM helpers
GROUP BY customer_id, group_id;

CREATE OR REPLACE FUNCTION update_periods() RETURNS trigger AS
$$
BEGIN
    REFRESH MATERIALIZED VIEW periods;
    RETURN NULL;
END;
$$
    LANGUAGE plpgsql;

CREATE TRIGGER update_periods_trigger_transactions
    AFTER INSERT OR UPDATE OR DELETE
    ON public.transactions
    FOR EACH ROW
EXECUTE FUNCTION update_periods();

CREATE TRIGGER update_periods_trigger_sku
    AFTER INSERT OR UPDATE OR DELETE
    ON public.sku
    FOR EACH ROW
EXECUTE FUNCTION update_periods();

CREATE TRIGGER update_periods_trigger_cards
    AFTER INSERT OR UPDATE OR DELETE
    ON public.cards
    FOR EACH ROW
EXECUTE FUNCTION update_periods();

CREATE TRIGGER update_periods_trigger_checks
    AFTER INSERT OR UPDATE OR DELETE
    ON public.checks
    FOR EACH ROW
EXECUTE FUNCTION update_periods();
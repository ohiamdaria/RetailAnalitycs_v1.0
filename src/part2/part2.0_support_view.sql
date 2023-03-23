CREATE MATERIALIZED VIEW support_view AS
SELECT public.cards.customer_id,
       public.transactions.transaction_id,
       public.transactions.transaction_summ,
       public.transactions.transaction_store_id,
       public.transactions.customer_card_id,
       public.checks.sku_id,
       public.sku.group_id,
       public.checks.sku_amount,
       public.checks.sku_summ,
       public.checks.sku_summ_paid,
       public.checks.sku_discount,
       public.stores.sku_purchase_price,
--        public.stores.sku_purchase_price * public.checks.sku_amount AS Group_Cost,
       public.stores.sku_retail_price,
       public.transactions.transaction_datetime,
       public.date_of_analysis_formation.analysis_formation
FROM transactions
         JOIN cards
              ON cards.customer_card_id = transactions.customer_card_id
         JOIN checks
              ON checks.transaction_id = transactions.transaction_id
         JOIN sku
              ON sku.sku_id = checks.sku_id
         JOIN stores
              ON stores.transaction_store_id = transactions.transaction_store_id
                  AND stores.sku_id = sku.sku_id,
     date_of_analysis_formation;


CREATE OR REPLACE FUNCTION update_support_view() RETURNS trigger AS
$$
BEGIN
    REFRESH MATERIALIZED VIEW public.support_view;
    RETURN NULL;
END;
$$
    LANGUAGE plpgsql;

CREATE TRIGGER update_support_view_trigger_transactions
    AFTER INSERT OR UPDATE OR DELETE
    ON public.transactions
    FOR EACH ROW
EXECUTE FUNCTION update_support_view();

CREATE TRIGGER update_support_view_trigger_sku
    AFTER INSERT OR UPDATE OR DELETE
    ON public.sku
    FOR EACH ROW
EXECUTE FUNCTION update_support_view();

CREATE TRIGGER update_support_view_trigger_cards
    AFTER INSERT OR UPDATE OR DELETE
    ON public.cards
    FOR EACH ROW
EXECUTE FUNCTION update_support_view();

CREATE TRIGGER update_support_view_trigger_checks
    AFTER INSERT OR UPDATE OR DELETE
    ON public.checks
    FOR EACH ROW
EXECUTE FUNCTION update_support_view();

CREATE TRIGGER update_support_view_trigger_checks
    AFTER INSERT OR UPDATE OR DELETE
    ON public.stores
    FOR EACH ROW
EXECUTE FUNCTION update_support_view();
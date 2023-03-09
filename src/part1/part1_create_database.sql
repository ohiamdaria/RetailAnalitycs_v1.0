CREATE TABLE personal_data
(
    customer_id            BIGINT PRIMARY KEY,
    customer_name          varchar(255)
        CHECK (customer_name ~ '^[A-ZА-Я][a-zа-яё -]+$'),
    customer_surname       varchar(255)
        CHECK (customer_surname ~ '^[A-ZА-Я][a-zа-яё -]+$'),
    customer_primary_email varchar(255)
        CHECK (customer_primary_email ~ '^[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$'),
    customer_primary_phone varchar(255)
        CHECK (customer_primary_phone ~ '^[+][7][0-9]{10}')
);


CREATE TABLE cards
(
    customer_card_id BIGINT PRIMARY KEY,
    customer_id      BIGINT NOT NULL REFERENCES personal_data (customer_id)
);

CREATE INDEX IF NOT EXISTS idx_customer_id ON cards USING btree(customer_id);

COMMENT ON COLUMN cards.customer_id IS 'Одному клиенту может принадлежать несколько карт';

CREATE TABLE transactions
(
    transaction_id       BIGINT PRIMARY KEY,
    customer_card_id     BIGINT REFERENCES cards (customer_card_id),
    transaction_summ     NUMERIC,
    transaction_datetime timestamp WITHOUT TIME ZONE,
    transaction_store_id BIGINT
);

CREATE INDEX IF NOT EXISTS idx_transactions_time_card ON transactions USING btree(transaction_datetime, customer_card_id);

COMMENT ON COLUMN transactions.transaction_summ IS 'Сумма транзакции в рублях (полная стоимость покупки без учета скидок)';
COMMENT ON COLUMN transactions.transaction_datetime IS 'Дата и время совершения транзакции';
COMMENT ON COLUMN transactions.transaction_store_id IS 'Магазин, в котором была совершена транзакция';

CREATE TABLE groups_sku
(
    group_id   BIGINT PRIMARY KEY,
    group_name varchar(255)
        CHECK (group_name ~ '^[A-ZА-Яa-zа-яё0-9 -\[\]\\\^\$\.\|\?\*\+\(\)]+$')
);

CREATE TABLE sku
(
    sku_id   BIGINT PRIMARY KEY,
    sku_name varchar
        CHECK (sku_name ~ '^[A-ZА-Яa-zа-яё0-9 -\[\]\\\^\$\.\|\?\*\+\(\)]+$'),
    group_id BIGINT NOT NULL REFERENCES groups_sku (group_id)
);

CREATE INDEX IF NOT EXISTS idx_group_id ON sku USING btree(group_id);

COMMENT ON COLUMN sku.group_id IS 'Идентификатор группы родственных товаров, к которой относится товар (например, одинаковые йогурты одного производителя и объема, но разных вкусов). Указывается один идентификатор для всех товаров в группе';

CREATE TABLE checks
(
    transaction_id BIGINT NOT NULL REFERENCES transactions (transaction_id),
    sku_id         BIGINT NOT NULL REFERENCES sku (sku_id),
    sku_amount     NUMERIC,
    sku_summ       NUMERIC,
    sku_summ_paid  NUMERIC,
    sku_discount   NUMERIC
);

COMMENT ON COLUMN checks.sku_amount IS 'Указание, какое количество товара было куплено';
COMMENT ON COLUMN checks.sku_summ IS 'Сумма покупки фактического объема данного товара в рублях (полная стоимость без учета скидок и бонусов)';
COMMENT ON COLUMN checks.sku_summ_paid IS 'Фактически оплаченная сумма покупки данного товара, не включая сумму предоставленной скидки';
COMMENT ON COLUMN checks.sku_discount IS 'Размер предоставленной на товар скидки в рублях';

CREATE TABLE stores
(
    transaction_store_id BIGINT,
    sku_id               BIGINT REFERENCES sku(sku_id),
    sku_purchase_price   NUMERIC
        CHECK (sku_purchase_price >= 0),
    sku_retail_price     NUMERIC
        CHECK (sku_retail_price >= 0)
);

COMMENT ON COLUMN stores.sku_purchase_price IS 'Закупочная стоимость товара для данного магазина';
COMMENT ON COLUMN stores.sku_retail_price IS 'Стоимость продажи товара без учета скидок для данного магазина';

CREATE TABLE date_of_analysis_formation
(
    analysis_formation timestamp DEFAULT current_date
);

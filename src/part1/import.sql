-- Absolute path to the project
SET path_to_datasets.const TO '/Users/warbirdo/Desktop/RetailAnalitycs_v1.0/datasets/';

DROP PROCEDURE IF EXISTS import(tablename varchar, separator char);

CREATE OR REPLACE PROCEDURE import(IN tablename varchar, IN separator char) AS $$
    BEGIN
            EXECUTE format('COPY %s FROM %L DELIMITER %L CSV;', tablename,
                (current_setting('path_to_datasets.const') || tablename || '.tsv'), separator);
    END
$$ LANGUAGE plpgsql;

CALL import('personal_data',E'\t');
CALL import('Cards', E'\t');
CALL import('Transactions', E'\t');

-- DELETE FROM personal_data WHERE customer_id between 1 and 1000;


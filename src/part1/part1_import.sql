--
-- If import from datasets for Transactions table doesn't works use: SET datestyle = 'european';
--

-- Absolute path to the project
SET path_to_datasets.const TO '/Users/warbirdo/Desktop/RetailAnalitycs_v1.0/datasets/';

DROP PROCEDURE IF EXISTS import(tablename varchar, separator char);
DROP PROCEDURE IF EXISTS import_mini(tablename varchar, separator char);


CREATE OR REPLACE PROCEDURE import(IN tablename varchar, IN separator char) AS $$
    BEGIN
            EXECUTE format('COPY %s FROM %L DELIMITER %L CSV;', tablename,
                (current_setting('path_to_datasets.const') || tablename || '.tsv'), separator);
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE import_mini(IN tablename varchar, IN separator char) AS $$
    BEGIN
            EXECUTE format('COPY %s FROM %L DELIMITER %L CSV;', tablename,
                (current_setting('path_to_datasets.const') || tablename || '_Mini.tsv'), separator);
    END
$$ LANGUAGE plpgsql;

CALL import('personal_data',E'\t');
CALL import('Cards', E'\t');
CALL import('Transactions', E'\t');
CALL import('Groups_SKU', E'\t');
CALL import('SKU', E'\t');
CALL import('Checks', E'\t');
CALL import('Stores', E'\t');
CALL import('date_of_analysis_formation', E'\t');


-- CALL import_mini('personal_data',E'\t');
-- CALL import_mini('Cards', E'\t');
-- CALL import_mini('Transactions', E'\t');
-- CALL import_mini('Groups_SKU', E'\t');
-- CALL import_mini('SKU', E'\t');
-- CALL import_mini('Checks', E'\t');
-- CALL import_mini('Stores', E'\t');
-- CALL import_mini('date_of_analysis_formation', E'\t');

-- DELETE FROM personal_data WHERE customer_id between 1 and 1000;


SET path_to_export.const TO '/Users/warbirdo/Desktop/RetailAnalitycs_v1.0/src/export/';

DROP PROCEDURE IF EXISTS export(tablename varchar, separator char);

CREATE OR REPLACE PROCEDURE export(IN tablename varchar, IN separator char) AS $$
    BEGIN
            EXECUTE format('COPY %s TO %L DELIMITER %L CSV HEADER;', tablename,
                (current_setting('path_to_export.const') || tablename || '.fsv'), separator);
    END
$$ LANGUAGE plpgsql;


CALL export('personal_data', E'\t');
CALL export('cards', E'\t');
CALL export('transactions', E'\t');
CALL export('groups_SKU', E'\t');
CALL export('sku', E'\t');
CALL export('checks', E'\t');
CALL export('stores', E'\t');
CALL export('date_of_analysis_formation', E'\t');
SET path_to_project.const TO '/home/vladimir/Desktop/RetailAnalitycs_v1.0/';

CREATE OR REPLACE PROCEDURE import(IN tablename varchar, IN path text, IN separator char) AS $$
    BEGIN
            EXECUTE format('COPY %s FROM ''%s'' DELIMITER ''%s'' CSV HEADER;', tablename, path, separator);
    END;
$$ LANGUAGE plpgsql;

CALL import('transactions', '/home/vladimir/Desktop/RetailAnalitycs_v1.0/datasets/Transactions.tsv','.');
SET path_to_project.const TO '/home/vladimir/Рабочий стол/RetailAnalitycs_v1.0/';

CREATE PROCEDURE import_data(tablename varchar, delimeter char)
AS
$$
BEGIN
    EXECUTE format('COPY %s FROM %L WITH CSV DELIMITER %L HEADER;', $1, $2, $3);
END;
$$ LANGUAGE plpgsql;

CALL import_data('cards',current_setting('path_to_project.const')||'datasets/Cards'||'.tsv',',');
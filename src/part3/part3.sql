DROP ROLE IF EXISTS administrator;
DROP ROLE IF EXISTS visitor;

-- Выдача прав для администратора.
CREATE ROLE administrator LOGIN PASSWORD 'admin_password' SUPERUSER;

-- Выдача прав для посетителя.
CREATE ROLE visitor LOGIN PASSWORD 'visitor_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO visitor;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO visitor;

-- Проверка.
SELECT * FROM pg_roles where left(rolname, 2) <> 'pg';
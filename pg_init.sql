-- Init on Linux :
-- 1) login as postgres user : sudo su postgres 
-- 2) start script : psql -f pg_init.sql
 
CREATE ROLE test WITH PASSWORD 'test' NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN;
CREATE DATABASE test OWNER test encoding 'UTF8';
GRANT ALL PRIVILEGES ON DATABASE test TO test;

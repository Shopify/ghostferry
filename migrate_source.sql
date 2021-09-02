CREATE TABLE IF NOT EXISTS customers_new LIKE customers;
ALTER TABLE customers_new ADD COLUMN foo varchar(255);


RENAME TABLE customers TO customers_old, customers_new TO customers;

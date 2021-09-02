DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS customers_new;
DROP TABLE IF EXISTS customers_old;

CREATE TABLE IF NOT EXISTS customers (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  tenant_id bigint(20) NOT NULL,
  email varbinary(128) DEFAULT NULL,
  PRIMARY KEY (`id`)
)

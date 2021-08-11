drop table products;

CREATE TABLE IF NOT EXISTS `products` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `shop_id` bigint(20) DEFAULT NULL,
  `handle` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX index_products_on_shop_id ON products (shop_id);


insert into products (shop_id, handle) values (3, 'foobar');

insert into products (shop_id, handle) values (1, 'foobar');
insert into products (shop_id, handle) values (1, 'foobar');
insert into products (shop_id, handle) values (1, 'foobar');

insert into products (shop_id, handle) values (5, 'foobar');
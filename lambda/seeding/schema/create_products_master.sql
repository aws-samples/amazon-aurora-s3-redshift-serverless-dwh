CREATE TABLE products_master (
  product_id int auto_increment PRIMARY KEY,
  product_name varchar(70) NOT NULL,
  product_line varchar(50) NOT NULL,
  product_scale varchar(10) NOT NULL,
  product_vendor varchar(50) NOT NULL,
  product_description text NOT NULL,
  quantity_stock smallint(6) NOT NULL,
  price int UNSIGNED NOT NULL,
  MSRP int UNSIGNED NOT NULL,
  create_date timestamp NOT NULL,
  update_date timestamp NOT NULL,
  delete_date timestamp NOT NULL
);
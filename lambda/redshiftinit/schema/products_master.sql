CREATE TABLE IF NOT EXISTS products_master (
  product_id int NULL,
  product_name varchar(70) NOT NULL,
  product_line varchar(50) NOT NULL,
  product_scale varchar(10) NOT NULL,
  product_vendor varchar(50) NOT NULL,
  product_description text NOT NULL,
  quantity_stock int NOT NULL,
  price int NOT NULL,
  MSRP int NOT NULL,
  create_date timestamp NOT NULL,
  update_date timestamp NOT NULL,
  delete_date timestamp NOT NULL
);
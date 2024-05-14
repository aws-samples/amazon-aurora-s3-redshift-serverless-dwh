CREATE TABLE order_details_master (
  order_id int,
  product_id int NOT NULL,
  quantity_ordered int UNSIGNED NOT NULL,
  price_each int UNSIGNED NOT NULL,
  create_date timestamp NOT NULL,
  update_date timestamp NOT NULL,
  delete_date timestamp NOT NULL,
  PRIMARY KEY (order_id, product_id)
);
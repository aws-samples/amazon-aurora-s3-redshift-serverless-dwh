CREATE TABLE IF NOT EXISTS order_details_master (
  order_id int NULL,
  product_id int NOT NULL,
  quantity_ordered int NOT NULL,
  price_each int NOT NULL,
  create_date timestamp NOT NULL,
  update_date timestamp NOT NULL,
  delete_date timestamp NOT NULL
);
CREATE TABLE IF NOT EXISTS orders_master (
  order_id int NULL,
  order_date date NOT NULL,
  required_date date NOT NULL,
  shipped_date date NULL,
  status varchar(15) NOT NULL,
  comments text,
  create_date timestamp NOT NULL,
  update_date timestamp NOT NULL,
  delete_date timestamp NOT NULL
);
begin transaction;

create temp table paied_point_temp
as select *
from products_master, orders_master, order_details_master
where products_master.product_id = order_details_master.products_id
and orders_master.order_id = order_details_master.order_id
;

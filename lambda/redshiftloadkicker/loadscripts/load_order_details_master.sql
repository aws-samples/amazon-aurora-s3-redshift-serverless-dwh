begin transaction;

--ロード対象のテーブルと同一構成の空の一時テーブルを作成
create temp table order_details_master_temp
as
select * from order_details_master where 0=1;

--一時テーブルにデータをロード
--s3のURIとIAM Roleは呼び出し元のプログラムからパラメータ渡しされることを想定
copy order_details_master_temp
from 's3://<DATASOURCE_BUCKET_NAME>/<DATE>/order_details_master/order_details_master.csv.part_00000'
iam_role '<ROLEARN_TO_READ_DATASOURCE>'
format CSV
IGNOREHEADER 1
;

--本テーブルにマージ
merge into order_details_master
using order_details_master_temp t
    on order_details_master.product_id = t.product_id
    and order_details_master.order_id = t.order_id
when matched then update set 
    quantity_ordered=t.quantity_ordered,
    price_each=t.price_each,
    create_date=t.create_date,
    update_date=t.update_date,
    delete_date=t.delete_date
when not matched then insert
    (
        order_id,
        product_id,
        quantity_ordered,
        price_each,
        create_date,
        update_date,
        delete_date
    ) values (
        t.order_id,
        t.product_id,
        t.quantity_ordered,
        t.price_each,
        t.create_date,
        t.update_date,
        t.delete_date
    )
;

end transaction;


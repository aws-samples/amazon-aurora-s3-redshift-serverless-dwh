begin transaction;

--ロード対象のテーブルと同一構成の空の一時テーブルを作成
create temp table products_master_temp
as
select * from products_master where 0=1;

--一時テーブルにデータをロード
--s3のURIとIAM Roleは呼び出し元のプログラムからパラメータ渡しされることを想定
copy products_master_temp
from 's3://<DATASOURCE_BUCKET_NAME>/<DATE>/products_master/products_master.csv.part_00000'
iam_role '<ROLEARN_TO_READ_DATASOURCE>'
format CSV
IGNOREHEADER 1
;

--本テーブルにマージ
merge into products_master
using products_master_temp t
    on products_master.product_id = t.product_id
when matched then update set 
    product_name = t.product_name,
    product_line = t.product_line,
    product_scale = t.product_scale,
    product_vendor = t.product_vendor,
    product_description = t.product_description,
    quantity_stock = t.quantity_stock,
    price = t.price,
    MSRP = t.MSRP,
    create_date = t.create_date,
    update_date = t.update_date,
    delete_date = t.delete_date
when not matched then insert
    (
        product_id,
        product_name,
        product_line,
        product_scale,
        product_vendor,
        product_description,
        quantity_stock,
        price,
        MSRP,
        create_date,
        update_date,
        delete_date
    ) values (
        t.product_id,
        t.product_name,
        t.product_line,
        t.product_scale,
        t.product_vendor,
        t.product_description,
        t.quantity_stock,
        t.price,
        t.MSRP,
        t.create_date,
        t.update_date,
        t.delete_date
    )
;

end transaction;


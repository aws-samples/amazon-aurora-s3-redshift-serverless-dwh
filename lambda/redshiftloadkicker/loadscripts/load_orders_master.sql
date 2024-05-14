begin transaction;

--ロード対象のテーブルと同一構成の空の一時テーブルを作成
create temp table orders_master_temp
as
select * from orders_master where 0=1;

--一時テーブルにデータをロード
--s3のURIとIAM Roleは呼び出し元のプログラムからパラメータ渡しされることを想定
copy orders_master_temp
from 's3://<DATASOURCE_BUCKET_NAME>/<DATE>/orders_master/orders_master.csv.part_00000'
iam_role '<ROLEARN_TO_READ_DATASOURCE>'
format CSV
IGNOREHEADER 1
;

--本テーブルにマージ
merge into orders_master
using orders_master_temp t
    on orders_master.order_id = t.order_id
when matched then update set 
    order_date=t.order_date,
    required_date=t.required_date,
    shipped_date=t.shipped_date,
    status=t.status,
    comments=t.comments,
    create_date=t.create_date,
    update_date=t.update_date,
    delete_date=t.delete_date
when not matched then insert
    (
        order_id,
        order_date,
        required_date,
        shipped_date,
        status,
        comments,
        create_date,
        update_date,
        delete_date
    ) values (
        t.order_id,
        t.order_date,
        t.required_date,
        t.shipped_date,
        t.status,
        t.comments,
        t.create_date,
        t.update_date,
        t.delete_date
    )
;

end transaction;


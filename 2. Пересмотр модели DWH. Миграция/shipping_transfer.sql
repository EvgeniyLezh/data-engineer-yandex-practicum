-- 3. Справочник типов доставки.

create table shipping_transfer
(transfer_type_id serial not null,
transfer_type text not null,
transfer_model text not null,
shipping_transfer_rate numeric(14,3) null,
primary key (transfer_type_id))
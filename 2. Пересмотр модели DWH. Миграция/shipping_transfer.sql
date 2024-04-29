drop table if exists public.shipping_transfer;
create table shipping_transfer
(transfer_type_id serial not null,
transfer_type text not null,
transfer_model text not null,
shipping_transfer_rate numeric(14,3) null,
primary key (transfer_type_id));

-- Заполняем

insert into public.shipping_transfer
(transfer_type, transfer_model, shipping_transfer_rate)

select
cast(description[1] as text) as transfer_type,
cast(description[2] as text) as transfer_model,
cast(shipping_transfer_rate as numeric(14,3)) shipping_transfer_rate
from (select distinct regexp_split_to_array(shipping_transfer_description, ':+') as description, shipping_transfer_rate from public.shipping) ship;

-- Проверяем

select
transfer_type_id,
transfer_type,
transfer_model,
shipping_transfer_rate
from public.shipping_transfer
limit 10;

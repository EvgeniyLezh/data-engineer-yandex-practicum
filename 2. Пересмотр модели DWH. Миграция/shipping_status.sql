-- 5. Таблица со статусами о доставке

drop table if exists public.shipping_status;
create table shipping_status
(shippingid int8 not null,
status text not null,
state text not null,
shipping_start_fact_datetime timestamp null,
shipping_end_fact_datetime timestamp null,
primary key (shippingid));
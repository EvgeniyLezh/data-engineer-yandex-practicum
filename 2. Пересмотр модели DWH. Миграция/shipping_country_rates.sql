-- 1. Справочник стоимости доставки в страны.

drop table if exists public.shipping_country_rates;
create table shipping_country_rates
(shipping_country_id serial not null,
shipping_country text null,
shipping_country_base_rate numeric(14,3) null,
primary key (shipping_country_id));

-- Заполняем

insert into public.shipping_country_rates
(shipping_country, shipping_country_base_rate)

select distinct
shipping_country,
shipping_country_base_rate
from public.shipping

-- Проверяем

select
shipping_country_id,
shipping_country,
shipping_country_base_rate
from public.shipping_country_rates
limit 10;

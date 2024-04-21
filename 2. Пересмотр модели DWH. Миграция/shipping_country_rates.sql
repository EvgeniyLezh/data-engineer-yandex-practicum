-- создаем справочник цен по странам

create table shipping_country_rates
(shipping_country_id int8 not null,
shipping_country text null,
shipping_country_base_rate numeric(14,3),
primary key (shipping_country_id))

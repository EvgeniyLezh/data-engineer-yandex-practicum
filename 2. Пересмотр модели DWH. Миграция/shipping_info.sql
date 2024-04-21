-- 4. Таблица с инфо о доставках

drop table if exists public.shipping_info
create table shipping_info
(shippingid int8 not null,
vendorid int8 null,
payment_amount numeric(14,2) null,
shipping_plan_datetime timestamp null,
transfer_type_id serial null,
shipping_country_id int8 null,
agreementid int8 null,
primary key (shippingid),
foreign key (shipping_country_id) references public.shipping_country_rates(shipping_country_id) on update cascade,
foreign key (agreementid) references public.shipping_agreement(agreementid) on update cascade,
foreign key (transfer_type_id) references public.shipping_transfer(transfer_type_id) on update cascade)
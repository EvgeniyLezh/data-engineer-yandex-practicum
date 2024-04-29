-- 4. Таблица с инфо о доставках

drop table if exists public.shipping_info;
create table shipping_info
(shippingid int8 not null,
vendorid int8 not null,
payment_amount numeric(14,2) null,
shipping_plan_datetime timestamp null,
transfer_type_id serial not null,
shipping_country_id int8 not null,
agreementid int8 not null,
primary key (shippingid),
foreign key (shipping_country_id) references public.shipping_country_rates(shipping_country_id) on update cascade,
foreign key (agreementid) references public.shipping_agreement(agreementid) on update cascade,
foreign key (transfer_type_id) references public.shipping_transfer(transfer_type_id) on update cascade);

-- Заполняем

insert into public.shipping_info
(shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)

select distinct
s.shippingid,
s.vendorid,
s.payment_amount,
s.shipping_plan_datetime,
st.transfer_type_id,
cr.shipping_country_id,
sa.agreementid
from public.shipping s
inner join public.shipping_transfer st on
	cast((regexp_split_to_array(s.shipping_transfer_description, ':+'))[1] as text) = st.transfer_type and
	cast((regexp_split_to_array(s.shipping_transfer_description, ':+'))[2] as text) = st.transfer_model and
	cast(s.shipping_transfer_rate as numeric(14,3)) = st.shipping_transfer_rate
inner join public.shipping_country_rates cr on
	s.shipping_country = cr.shipping_country and s.shipping_country_base_rate = cr.shipping_country_base_rate
inner join public.shipping_agreement sa on
	cast((regexp_split_to_array(s.vendor_agreement_description, ':+'))[2] as text) = sa.agreement_number and
	cast((regexp_split_to_array(s.vendor_agreement_description, ':+'))[3] as numeric(14,3)) = sa.agreement_rate and
	cast((regexp_split_to_array(s.vendor_agreement_description, ':+'))[4] as numeric(14,3)) = sa.agreement_commission

-- Проверяем

select shippingid, vendorid, payment_amount as payment, shipping_plan_datetime, transfer_type_id as transf_id, shipping_country_id as s_countr_id, agreementid as agr_id
from public.shipping_info
limit 10

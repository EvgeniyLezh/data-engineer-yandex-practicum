-- Создадим витрину данных.
drop view if exists public.shipping_datamart;
create view public.shipping_datamart as

select
si.shippingid,
si.vendorid,
st.transfer_type,
date_part('day',(ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime)) as full_day_at_shipping,
case
	when ss.shipping_end_fact_datetime is null then null
	when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1 else 0
end is_delay,
case
	when ss.status_finish = 1 then 1 else 0
end is_shipping_finish,
case
	when ss.shipping_end_fact_datetime is null then null
	when ss.shipping_end_fact_datetime > si.shipping_plan_datetime
	then date_part('day', (ss.shipping_end_fact_datetime - si.shipping_plan_datetime)) else 0
end delay_day_at_shipping,
si.payment_amount,
(si.payment_amount * (scr.shipping_country_base_rate + st.shipping_transfer_rate + sa.agreement_rate))::numeric(14,2) as vat,
(si.payment_amount * sa.agreement_commission)::numeric(14,2) as profit
from public.shipping_info si
left join public.shipping_transfer st on st.transfer_type_id = si.transfer_type_id
left join (
	select distinct shippingid, shipping_end_fact_datetime, shipping_start_fact_datetime,
		max(case when status = 'finished' then 1 else 0 end) over(partition by shippingid) status_finish
	from public.shipping_status) ss on si.shippingid = ss.shippingid
left join public.shipping_country_rates scr on scr.shipping_country_id = si.shipping_country_id
left join public.shipping_agreement sa on sa.agreementid = si.agreementid

-- Проверяем

select shippingid, vendorid, transfer_type, full_day_at_shipping, is_delay, is_shipping_finish, delay_day_at_shipping, payment_amount, vat, profit
from public.shipping_datamart
limit 5

-- Проверим процент задержек


select
vendorid,
count(is_delay) as ship,
sum(is_delay) as ship_delay,
round(sum(is_delay)::numeric / count(is_delay)::numeric * 100, 2) as percent_delay
from public.shipping_datamart
where is_delay is not null
group by vendorid
order by percent_delay desc
limit 10;

select
count(is_delay) as ship,
sum(is_delay) as ship_delay,
round(sum(is_delay)::numeric / count(is_delay)::numeric * 100, 2) as percent_delay
from public.shipping_datamart
where is_delay is not null
order by percent_delay desc
limit 10;

-- Проверим процент возвратов

select
ret.vendorid,
ret.ship,
ret.ship_returned,
round(ret.ship_returned / ret.ship * 100, 2) as percent_returned from
(select
vendorid,
sum(case
			when shippingid in
				(select distinct shippingid
				from public.shipping_status
				where state = 'returned')
			then 1 else 0 end)::numeric ship_returned,
count(shippingid)::numeric ship
from public.shipping_datamart
group by vendorid) ret
order by percent_returned desc
limit 10;

В среднем по всем поставщикам

select
sum(ret.ship),
sum(ret.ship_returned),
round(sum(ret.ship_returned) / sum(ret.ship) * 100, 2) as percent_returned from
(select
vendorid,
sum(case
			when shippingid in
				(select distinct shippingid
				from public.shipping_status
				where state = 'returned')
			then 1 else 0 end)::numeric ship_returned,
count(shippingid)::numeric ship
from public.shipping_datamart
group by vendorid) ret
order by percent_returned desc
limit 10;

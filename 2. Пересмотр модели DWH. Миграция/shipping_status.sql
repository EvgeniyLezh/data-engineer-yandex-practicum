-- 5. Таблица со статусами о доставке

drop table if exists public.shipping_status;
create table shipping_status
(shippingid int8 not null,
status text not null,
state text not null,
shipping_start_fact_datetime timestamp null,
shipping_end_fact_datetime timestamp null);
create index shipping_status_i on public.shipping_status(shippingid);

-- Заполняем

insert into public.shipping_status
(shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)


with cte_shipping as
(select shippingid, status, state, max(state_datetime) as max_state_datetime
from public.shipping
group by shippingid, status, state)

select distinct
cte_s.shippingid,
cte_s.status,
cte_s.state,
max(
  case when cte_s1.state = 'booked' then cte_s1.max_state_datetime end) as shipping_start_fact_datetime,
max(
  case when cte_s2.state = 'recieved' then cte_s2.max_state_datetime end) as shipping_end_fact_datetime
from cte_shipping cte_s
left join cte_shipping cte_s1 on cte_s1.shippingid = cte_s.shippingid
left join cte_shipping cte_s2 on cte_s2.shippingid = cte_s.shippingid
group by cte_s.shippingid, cte_s.status, cte_s.state
order by cte_s.shippingid;

-- Проверяем

select shippingid, "status", "state", shipping_start_fact_datetime, shipping_end_fact_datetime
from public.shipping_status
limit 10;

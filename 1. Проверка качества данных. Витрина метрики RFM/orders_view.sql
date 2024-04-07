-- Заменяем статус из таблицы order, на данные из таблицы orderstatuslog

create or replace view analysis.orders as
(select
o.order_id,
o.order_ts,
o.user_id,
o.bonus_payment,
o.payment,
o."cost",
o.bonus_grant,
stat_log.status_id status
from production.orders o
inner join
	(select ord_stat.order_id, ord_stat.status_id
	from
	(select
	order_id,
	status_id,
	dttm,
	max(dttm) over (partition by order_id) dttm_max
	from production.orderstatuslog
	order by order_id) ord_stat
	where dttm = dttm_max) stat_log on stat_log.order_id = o.order_id);

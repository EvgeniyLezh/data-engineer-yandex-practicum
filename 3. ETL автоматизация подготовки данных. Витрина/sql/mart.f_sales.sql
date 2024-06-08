insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, order_refunded)
select
	dc.date_id,
	item_id,
	customer_id,
	city_id,
	case when status = 'refunded' then quantity * -1 else quantity end quantity,
	case when status = 'refunded' then payment_amount * -1 else payment_amount end payment_amount,
	case when status = 'shipped' then 0 else 1 end order_refunded
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';

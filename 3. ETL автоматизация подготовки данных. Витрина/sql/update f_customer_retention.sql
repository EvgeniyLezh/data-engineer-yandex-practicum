delete from mart.f_customer_retention
where mart.f_customer_retention.period_id =
   (select distinct mart.d_calendar.week_of_year from mart.d_calendar where mart.d_calendar.date_actual = '{{ds}}');

insert into mart.f_customer_retention
(new_customers_count, returning_customers_count, refunded_customer_count, period_name, period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
select
	count(distinct case when t.count_order = 1 then t.customer_id end) as new_customers_count,
	count(distinct case when t.count_order > 1 then t.customer_id end) as returning_customers_count,
	count(distinct case when t.order_refunded != 0 then t.customer_id end) as refunded_customer_count,
	t.period_name,
	t.period_id,
	t.item_id,
	sum(case when t.count_order = 1 then t.payment_amount end) as new_customers_revenue,
	sum(case when t.count_order = 1 then t.payment_amount end) as returning_customers_revenue,
	sum(case when t.order_refunded != 0 then t.order_refunded_count else 0 end) as customers_refunded
from(
	select
	'weekly' as period_name, -- наименование периода
	cal.week_of_year as period_id, -- номер недели в году
	fsal.item_id,
	fsal.customer_id,
	count(fsal.id) count_order,
	max(fsal.order_refunded) order_refunded,
	sum(fsal.order_refunded) order_refunded_count,
	sum(payment_amount) payment_amount,
	sum(quantity) quantity
	from mart.f_sales fsal
	join mart.d_calendar cal on cal.date_id = fsal.date_id
	group by period_name, period_id, item_id, customer_id
	order by period_id, fsal.item_id, fsal.customer_id) t
group by t.period_name, t.period_id, t.item_id

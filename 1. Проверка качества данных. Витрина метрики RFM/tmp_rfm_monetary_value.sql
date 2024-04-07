-- Создание и наполнение таблицы tmp_rfm_monetary_value.

create table analysis.tmp_rfm_monetary_value (
 user_id int not null primary key,
 monetary_value int not null check(monetary_value >= 1 and monetary_value <= 5));

insert into analysis.tmp_rfm_monetary_value
(user_id, monetary_value)

with cte_monetary_value as(
select
ord.user_id,
SUM(cost) cost_sum
from analysis.orders ord
left join analysis.orderstatuses ost on ord.status = ost.id
where ost.key = 'Closed'
group by ord.user_id)

select
user_id,
ntile(5) over (order by cost_sum) monetary_value
from cte_monetary_value;

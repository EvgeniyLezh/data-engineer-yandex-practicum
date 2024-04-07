-- Создание и наполнение таблицы tmp_rfm_frequency.

create table analysis.tmp_rfm_frequency (
 user_id INT not null primary key,
 frequency INT not null check(frequency >= 1 and frequency <= 5));

insert into analysis.tmp_rfm_frequency
(user_id, frequency)

with cte_frequency as(
select
ord.user_id,
COUNT(order_id) order_id_cnt
from analysis.orders ord
left join analysis.orderstatuses ost on ord.status = ost.id
where ost.key = 'Closed'
group by ord.user_id)

select
user_id,
ntile(5) over (order by order_id_cnt) frequency
from cte_frequency;

-- Создание и наполнение таблицы tmp_rfm_recency.

create analysis.tmp_rfm_recency (
 user_id int not null primary key,
 recency int not null check(recency >= 1 and recency <= 5)
);

insert into analysis.tmp_rfm_recency
(user_id,
recency)

with cte_recency as(
select
ord.user_id,
(select (current_timestamp::timestamp) - max(ord.order_ts)) order_ts
from analysis.orders ord
left join analysis.orderstatuses ost on ord.status = ost.id
where ost.key = 'Closed'
group by ord.user_id)

select
user_id,
ntile(5) over (order by order_ts desc) recency
from cte_recency;

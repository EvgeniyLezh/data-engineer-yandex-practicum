-- Загружаем данные в итоговую таблицу analysis.dm_rfm_segments

insert into analysis.dm_rfm_segments
(user_id, recency, frequency, monetary_value)

select
rec.user_id,
rec.recency,
fr.frequency,
mon.monetary_value
from analysis.tmp_rfm_recency rec
full join (select user_id, frequency from analysis.tmp_rfm_frequency) fr on fr.user_id = rec.user_id
full join (select user_id, monetary_value from analysis.tmp_rfm_monetary_value) mon on mon.user_id = rec.user_id;

-- Посмотрим на первые 10 строк по минимальным user_id

select user_id, recency, frequency, monetary_value
from analysis.dm_rfm_segments
order by user_id
limit 10;

id  r   f   m
0	1	3	4
1	4	3	3
2	2	3	5
3	2	3	3
4	4	3	3
5	5	5	5
6	1	3	5
7	4	3	2
8	1	1	3
9	1	2	2

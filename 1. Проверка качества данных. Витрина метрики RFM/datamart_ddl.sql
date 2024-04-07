-- табица витрины данных
create table analysis.dm_rfm_segments
(user_id int not null primary key,
recency int not null,
frequency int not null,
monetary_value int not null)

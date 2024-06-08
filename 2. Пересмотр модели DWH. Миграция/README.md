# Пересмотр модели DWH. Миграция

### Задача
1. Провести миграцию в отдельные логические таблицы; <P><P>
2. Создать витрину данных с анализом заказов интернет-магазина.

### Исходный вид.

![2. Пересмотр модели DWH. Миграция/Исходный вид модели.png](https://github.com/EvgeniyLezh/data-engineer-yandex-practicum/blob/98aaa80b89518ecb1f5e6fbefab18d42d5b1c890/2.%20%D0%9F%D0%B5%D1%80%D0%B5%D1%81%D0%BC%D0%BE%D1%82%D1%80%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8%20DWH.%20%D0%9C%D0%B8%D0%B3%D1%80%D0%B0%D1%86%D0%B8%D1%8F/%D0%98%D1%81%D1%85%D0%BE%D0%B4%D0%BD%D1%8B%D0%B9%20%D0%B2%D0%B8%D0%B4%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8.png)

Описание исходных данных таблицы shipping.

### Итоговый вид.

![2. Пересмотр модели DWH. Миграция/Итоговый вид модели.png](https://github.com/EvgeniyLezh/data-engineer-yandex-practicum/blob/aeabfd7a6d47a5353ab387207720239f5608e2f6/2.%20%D0%9F%D0%B5%D1%80%D0%B5%D1%81%D0%BC%D0%BE%D1%82%D1%80%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8%20DWH.%20%D0%9C%D0%B8%D0%B3%D1%80%D0%B0%D1%86%D0%B8%D1%8F/%D0%98%D1%82%D0%BE%D0%B3%D0%BE%D0%B2%D1%8B%D0%B9%20%D0%B2%D0%B8%D0%B4%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8.png)


## Создадим и заполним необходимые таблицы.

### 1. Справочник стоимости доставки в страны.

Типы данных укажем на основании таблицы источника (shipping)

Добавлено ограничение по PK и null значениям, а также индекс по id

```sql
drop table if exists public.shipping_country_rates;
create table shipping_country_rates
(shipping_country_id serial not null,
shipping_country text null,
shipping_country_base_rate numeric(14,3) null,
primary key (shipping_country_id));
create index shipping_country_rates_i on public.shipping_country_rates(shipping_country_id);
```

Заполним уникальными парами значений shipping_country и shipping_country_base_rate

```sql
insert into public.shipping_country_rates
(shipping_country, shipping_country_base_rate)

select distinct
shipping_country,
shipping_country_base_rate
from public.shipping
```
Проверяем

```sql
select shipping_country_id, shipping_country, shipping_country_base_rate
from public.shipping_country_rates
limit 10;
```

|shipping_country_id| shipping_country| shipping_country_base_rate|
|----------|---------|---------|
|1	|usa	|0.020
|2	|norway	|0.040
|3	|germany	|0.010
|4	|russia	|0.030

### 2. Справочник тарифов доставки вендора по договору.

#### Описание:
- agreement_number - номер договора бухгалтерии
- agreement_rate - ставка налога за стоимость доставки товара для вендора
- agreement_commission - комиссия, то есть доля в платеже являющаяся доходом компании от сделки

```sql
drop table if exists public.shipping_agreement;
create table shipping_agreement
(agreementid int8 not null,
agreement_number text not null,
agreement_rate numeric(14,3) not null,
agreement_commission numeric(14,3) not null,
primary key (agreementid));
create index shipping_agreement_i on public.shipping_agreement(agreementid);
```

Информация для заполнения находится в столбце vendor_agreement_description (таблицы shipping), где данные записаны с помощью разделителя ":", поэтому нам потребуется регулярное выражение.

```sql
select s.vendor_agreement_description from shipping s limit 5
```
|vendor_agreement_description|
|----------|
|0:vspn-4092:0.14:0.02|
|1:vspn-366:0.13:0.01|
|2:vspn-4148:0.01:0.01|
|3:vspn-3023:0.05:0.01|
|3:vspn-3023:0.05:0.01|

Перейдем к заполнению таблицы.

```sql
insert into public.shipping_agreement
(agreementid, agreement_number, agreement_rate, agreement_commission)

select
cast(description[1] as int8) as agreementid,
cast(description[2] as text) as agreement_number,
cast(description[3] as numeric(14,3)) as agreement_rate,
cast(description[4] as numeric(14,3)) as agreement_commission
from
(select distinct regexp_split_to_array(vendor_agreement_description, ':+') as description from public.shipping) ship;
```

Проверяем

```sql
select agreementid, agreement_number, agreement_rate, agreement_commission
from public.shipping_agreement
limit 5;
```
|agreementid| agreement_rate| agreement_number| agreement_commission|
|----------|---------|---------|---------|
|32	|vspn-1730	|0.120	|0.020
|47	|vspn-3444	|0.070	|0.030
|19	|vspn-9037	|0.070	|0.020
|59	|vspn-7141	|0.070	|0.010
|51	|vspn-5162	|0.140	|0.020

### 3. Справочник типов доставки.

#### Описание:
- transfer_type - тип доставки 1p означает, что компания берёт ответственность за доставку на себя, 3p — что за отправку ответственен вендор.
- transfer_model - модель доставки, то есть способ, которым заказ доставляется до точки:
  - car — машиной,
  - train — поездом,
  - ship — кораблем,
  - airplane — самолетом,
  - multiple — комбинированной доставкой
- shipping_transfer_rate - процент стоимости доставки для вендора в зависимости от типа и модели доставки, который взимается интернет-магазином для покрытия расходов.

```sql
drop table if exists public.shipping_transfer;
create table shipping_transfer
(transfer_type_id serial not null,
transfer_type text not null,
transfer_model text not null,
shipping_transfer_rate numeric(14,3) null,
primary key (transfer_type_id));
create index shipping_transfer_i on public.shipping_transfer(transfer_type_id);
```
Заполним таблицу информацией из столбцов shipping_transfer_description и shipping_transfer_rate.

```sql
insert into public.shipping_transfer
(transfer_type, transfer_model, shipping_transfer_rate)

select
cast(description[1] as text) as transfer_type,
cast(description[2] as text) as transfer_model,
cast(shipping_transfer_rate as numeric(14,3)) shipping_transfer_rate
from
(select distinct
regexp_split_to_array(shipping_transfer_description, ':+') as description, shipping_transfer_rate from public.shipping) ship;
```

Проверяем

```sql
select transfer_type_id, transfer_type, transfer_model,
shipping_transfer_rate
from public.shipping_transfer
limit 5;
```

|transfer_type_id| transfer_type| transfer_model| shipping_transfer_rate|
|----------|---------|---------|---------|
|1	|3p	|ship	|0.025
|2	|1p	|multiplie	|0.050
|3	|3p	|train	|0.020
|4	|3p	|airplane	|0.035
|5	|1p	|ship	|0.030

Из таблицы видно, что наиболее дорогая комбинированная доставка за счет компании, а наиболее дешевая поездом силами вендора.

### 4. Таблица со справочной информацией о доставках.

#### Описание:

Таблица содержит id доставки, вендора, договора, вида и страны доставки, а также связанную справочную информацию и расшифровки.

Добавлены внешние ключи к таблицам со странами, договорами и видами транспорта, а также первичный ключ по идентификатору доставки.

```sql
drop table if exists public.shipping_info;
create table shipping_info
(shippingid int8 not null,
vendorid int8 null,
payment_amount numeric(14,2) null,
shipping_plan_datetime timestamp null,
transfer_type_id serial null,
shipping_country_id int8 null,
agreementid int8 null,
primary key (shippingid),
foreign key (shipping_country_id) references public.shipping_country_rates(shipping_country_id) on update cascade,
foreign key (agreementid) references public.shipping_agreement(agreementid) on update cascade,
foreign key (transfer_type_id) references public.shipping_transfer(transfer_type_id) on update cascade);
```

Заполним таблицу взяв необходимые данные из уже ранее созданных shipping_country_rates, shipping_agreement, shipping_transfer используя JOIN и константную информацию из shipping.

```sql
insert into public.shipping_info
(shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)

select distinct
s.shippingid,
s.vendorid,
s.payment_amount,
s.shipping_plan_datetime,
st.transfer_type_id,
cr.shipping_country_id,
sa.agreementid
from public.shipping s
inner join public.shipping_transfer st on
	cast((regexp_split_to_array(s.shipping_transfer_description, ':+'))[1] as text) = st.transfer_type and
	cast((regexp_split_to_array(s.shipping_transfer_description, ':+'))[2] as text) = st.transfer_model and
	cast(s.shipping_transfer_rate as numeric(14,3)) = st.shipping_transfer_rate
inner join public.shipping_country_rates cr on
	s.shipping_country = cr.shipping_country and s.shipping_country_base_rate = cr.shipping_country_base_rate
inner join public.shipping_agreement sa on
	cast((regexp_split_to_array(s.vendor_agreement_description, ':+'))[2] as text) = sa.agreement_number and
	cast((regexp_split_to_array(s.vendor_agreement_description, ':+'))[3] as numeric(14,3)) = sa.agreement_rate and
	cast((regexp_split_to_array(s.vendor_agreement_description, ':+'))[4] as numeric(14,3)) = sa.agreement_commission
```

Проверяем

```sql
select shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid
from public.shipping_info
limit 5;
```

|shippingid| vendorid| payment_amount| shipping_plan_datetime|  transfer_type_id| shipping_country_id| agreementid|
|----------|---------|---------|---------|---------|---------|---------|
|1	|1	|6.06	|2021-09-15 16:43:42.434	|6	|4	|0
|2	|1	|21.93	|2021-12-12 10:49:50.468	|6	|1	|1
|3	|1	|3.10	|2021-10-27 10:33:16.659	|7	|2	|2
|4	|3	|8.57	|2021-09-21 10:14:30.148	|6	|3	|3
|5	|3	|1.50	|2022-01-02 21:21:08.844	|6	|2	|3

### 5. Таблица со статусами о доставке.

#### Описание:

Таблица содержит информацию о статусе доставки заказа (in_progress — в процессе, finished — завершена), а также промежуточные точки (включая время начальной точки и конечной):
- booked (пер. «заказано»);
- fulfillment — заказ доставлен на склад отправки;
- queued (пер. «в очереди») — заказ в очереди на запуск доставки;
- transition (пер. «передача») — запущена доставка заказа;
- pending (пер. «в ожидании») — заказ доставлен в пункт выдачи;
- received (пер. «получено») — покупатель забрал заказ;
- returned (пер. «возвращено») — покупатель возвратил заказ;

```sql
drop table if exists public.shipping_status;
create table shipping_status
(shippingid int8 not null,
status text not null,
state text not null,
shipping_start_fact_datetime timestamp null,
shipping_end_fact_datetime timestamp null,
primary key (shippingid));
create index shipping_status_i on public.shipping_status(shippingid);
```

Заполним таблицу. Для удобства можно использовать cte (с максимальной датой статуса) и коррелированным подзапросом, чтобы найти время по выбранным state = booked и state = recieved.

```sql
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
```

Проверяем

```sql
select shippingid, "status", "state", shipping_start_fact_datetime, shipping_end_fact_datetime
from public.shipping_status
limit 5;
```

|shippingid| status| state| shipping_start_fact_datetime| shipping_end_fact_datetime|
|----------|---------|---------|---------|---------|
|1	|finished	|recieved	|2021-09-05 06:42:34.249	|2021-09-15 04:26:57.690
|1	|in_progress	|booked	|2021-09-05 06:42:34.249	|2021-09-15 04:26:57.690
|1	|in_progress	|fulfillment	|2021-09-05 06:42:34.249	|2021-09-15 04:26:57.690
|1	|in_progress	|pending	|2021-09-05 06:42:34.249	|2021-09-15 04:26:57.690
|1	|in_progress	|queued	|2021-09-05 06:42:34.249	|2021-09-15 04:26:57.690

## Формируем витрину данных

Требуется создать представление для аналитики на основании готовых таблиц.

Описание:

- shippingid;
- vendorid;
- transfer_type - тип доставки;
- full_day_at_shipping — количество полных дней, в течение которых длилась доставка;
- is_delay — статус, показывающий просрочена ли доставка;
- is_shipping_finish — статус, показывающий, что доставка завершена;
- delay_day_at_shipping — количество дней, на которые была просрочена доставка;
- payment_amount — сумма платежа пользователя;
- vat — итоговый налог на доставку;
- profit — итоговый доход компании с доставки.

```sql
create view public.shipping_datamart as

select
si.shippingid,
si.vendorid,
st.transfer_type,
date_part('day',(ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime)) as full_day_at_shipping,
case
  when ss.shipping_end_fact_datetime is null then null
  when ss.shipping_end_fact_datetime > si.shipping_plan_datetime
  then 1 else 0
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
    max(case when status = 'finished' then 1 else 0 end)
    over(partition by shippingid) status_finish
	from public.shipping_status) ss on si.shippingid = ss.shippingid
left join public.shipping_country_rates scr on scr.shipping_country_id = si.shipping_country_id
left join public.shipping_agreement sa on sa.agreementid = si.agreementid
```

Проверяем

```sql
select shippingid, vendorid, transfer_type, full_day_at_shipping, is_delay, is_shipping_finish, delay_day_at_shipping, payment_amount, vat, profit
from public.shipping_datamart
limit 5
```

|shippingid| vendorid| transfer_type| full_day_at_shipping| is_delay| is_shipping_finish| delay_day_at_shipping| payment_amount| vat| profit|
|----------|---------|---------|---------|---------|---------|---------|---------|---------|---------|
|4341	|2	|1p	|20.0	|0	|1	|0.0	|9.65	|1.83	|0.29
|1183	|3	|1p	|14.0	|1	|1	|7.0	|12.28	|1.41	|0.12
|15680	|2	|3p	|7.0	|0	|1	|0.0	|4.61	|0.83	|0.14
|16921	|3	|1p	|5.0	|0	|1	|0.0	|3.47	|0.36	|0.03
|8367	|1	|1p	|1.0	|0	|1	|0.0	|14.59	|0.95	|0.15

Проанализируем процент просрочки доставки по заказам

```sql
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
```

По таблице видно, что наихудший результат у vendorid = 3 - 32% задержек, такой поставщик неблагонадежен.

В среднем процент возвратов составил 13%

|vendorid| ship| ship_delay| percent_delay|
|----------|---------|---------|---------|
|3	|17675	|5637	|31.89
|15	|9	|1	|11.11
|21	|28	|3	|10.71
|6	|30	|2	|6.67
|19	|16	|1	|6.25
|5	|35	|2	|5.71
|7	|41	|2	|4.88
|1	|17492	|551	|3.15
|2	|17704	|523	|2.95
|9	|1	|0	|0.00

Проанализируем процент просрочки доставки по заказам

```sql
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
```

Худшие результат у vendorid 21, он имеет 50% возвратов, что говорит о плохом качестве товара, при этом средний процент возвратов по всем поставщикам = 1.5%

|vendorid| ship| ship_returned| percent_returned|
|----------|---------|---------|---------|
|21	|28	|14	|50.00
|15	|9	|1	|11.11
|7	|41	|3	|7.32
|1	|17850	|267	|1.50
|3	|18046	|267	|1.48
|2	|18055	|258	|1.43
|5	|35	|0	|0.00
|18	|11	|0	|0.00
|16	|4	|0	|0.00
|11	|4	|0	|0.00

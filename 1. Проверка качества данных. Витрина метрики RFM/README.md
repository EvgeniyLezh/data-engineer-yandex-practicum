# Проверка качества данных. Витрина метрики RFM.

### Задача
1. Проверить качество исходных данных (пропуски, повторы,    форматы, некорректные записи); <P><P>
2. Создать витрину для RFM-классификации пользователей.

### Что такое RFM
RFM (от англ. Recency, Frequency, Monetary Value) — способ сегментации клиентов, при котором анализируют их лояльность: как часто, на какие суммы и когда в последний раз тот или иной клиент покупал что-то. На основе этого выбирают клиентские категории, на которые стоит направить маркетинговые усилия.

Каждого клиента оценивают по трём факторам:
- Recency (пер. «давность») — сколько времени прошло с момента последнего заказа;
- Frequency (пер. «частота») — количество заказов;
- Monetary Value (пер. «денежная ценность») — сумма затрат клиента.

## 1.1. Требование к целевой витрине

1. Витрина должна располагаться в той же базе в схеме analysis;
2. Наименование dm_rfm_segments;
3. Глубина с начала 2022 года;
4. Структура:
   - user_id
   - recency (число от 1 до 5)
   - frequency (число от 1 до 5)
   - monetary_value (число от 1 до 5)
5. В анализе участвуют заказы со статусом Closed;
6. Обновления не нужны;
7. Ограничений доступа не требуется.

## 1.2. Структура исходных данных

Для расчета метрик потребуются следующие данные:

|Таблица| Поле |
|----------|---------|
|users|id|
|orders|user_id|
|orders|order_ts|
|orders|order_id|
|orders|payment|
|orders|status|
|orders|order_ts|
|orderstatuses|id|
|orderstatuses|key|

## 1.3. Анализ качества данных

### Таблица orderitems (позиции в заказах)

- Установлен PK (id) и условие уникальности (order_id и product_id);

- Значения не могут быть null;

- Проверка по логическому выражению (price >= 0; quantity > 0; discount >= 0 and discount <= price);

- Определены FK к таблицам orders (order_id) и products (product_id).

Дубликаты и 0 значения отсутствуют.

Не участвует в формировнии витрины.

### Таблица orders (заказы)

- Установлен PK (order_id);

Проверяем на дубликаты (сравнивая общее количество записей с количеством уникальных значений по ключу)

```sql
select
count(*) as total,
count(distinct order_id) as uniq
from production.orders;;
```
|total| uniq |
|----------|---------|
|10000|10000|

Дубликаты отсутствуют.

- Значения не могут быть null;

- Проверка по логическому выражению (cost = (payment + bonus_payment));

```sql
select
count(*) as total,
sum(case when cost = (payment + bonus_payment) then 1 end) as true_cost
from production.orders;
```
Значения рассчитаны согласно условию.

```sql
select
distinct os."key"
from production.orders o
left join production.orderstatuses os on o.status = os.id
```
Все заказы находятся в статусе "Closed" или "Cancelled"

Показатель bonus_payment = 0, оплата бонусами не производилась.

### Таблица orderstatuses (статусы заказов)

- Установлен PK (id);

Возможные статусы: "Open", "Cooking", "Delivering", "Closed", "Cancelled".

Дубликаты отсутствуют.

### Таблица orderstatuslog (логи статусов по заказам)

- Установлен PK (id) и условие уникальности (order_id и status_id);
- Значения не могут быть null.

Не участвует в формировнии витрины.

### Таблица products (продукты и цены)

- Установлен PK (id);
- Значения не могут быть null;
- Проверка по логическому выражению (price >= 0).

```sql
select id, name, price
from production.products
where price <= 0;
```
Строки с 0 ценой отсутствуют.

### Таблица users (данные покупателей)

- Установлен PK (id);
- Допускается значение null для поля name, но обязательно для login (ФИО);
- Отсутствует проверка на уникальность для поля login и name.

```sql
select login, count(1)
from users
group by "login"
having count(1) > 1
```
```sql
select login, count(1)
from users
group by "login"
having count(1) > 1
```
Дубликаты отсутствуют.

Перепутаны наименования полей в базе, по стандарту должно быть login - псевдоним, а name - ФИО, в нашем случае наоборот.


## 1.4. Подготовка витрины данных

### 1.4.1. Представление для таблиц из базы production
orderitems
```sql
create view analysis.orderitems as
(select id, product_id, order_id, "name", price, discount, quantity
from production.orderitems);
```
orders
```sql
create view analysis.orders as
(select order_id, order_ts, user_id, bonus_payment, payment, "cost", bonus_grant, status
from production.orders);
```
orderstatuses
```sql
create view analysis.orderstatuses as
(select id, "key"
from production.orderstatuses);
```
orderstatuslog
```sql
create view analysis.orderstatuslog as
(select id, order_id, status_id, dttm
from production.orderstatuslog);
```
products
```sql
create view analysis.products as
(select id, "name", price
from production.products);
```
users
```sql
create view analysis.users as
(select id, "name", login
from production.users);
```

### 1.4.2. Создаем таблицу для витрины данных (схема analysis)
```sql
create table analysis.dm_rfm_segments
(user_id int not null primary key,
recency int not null check(recency >= 1 and recency <= 5),
frequency int not null check(frequency >= 1 and frequency <= 5),
monetary_value int not null check(monetary_value >= 1 and monetary_value <= 5));
```

### 1.4.3. Пишем SQL-запрос для заполнения витрины

Для каждого показателя создадим отдельную таблицу и запишем в неё расчет.

- recency (по прошедшему времени от последнего заказа)

```sql
create analysis.tmp_rfm_recency (
 user_id int not null primary key,
 recency int not null check(recency >= 1 and recency <= 5));

insert into analysis.tmp_rfm_recency
(user_id, recency)

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
```

- frequency (по количеству заказов)

```sql
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
```

- monetary_value (по сумме затрат клиента)

```sql
create table analysis.tmp_rfm_monetary_value (
 user_id int not null primary key,
 monetary_value int not null check(monetary_value >= 1 and monetary_value <= 5));

insert into analysis.tmp_rfm_monetary_value
(user_id, monetary_value)

with cte_monetary_value as(
select
ord.user_id,
sum(cost) cost_sum
from analysis.orders ord
left join analysis.orderstatuses ost on ord.status = ost.id
where ost.key = 'Closed'
group by ord.user_id)

select
user_id,
ntile(5) over (order by cost_sum) monetary_value
from cte_monetary_value;
 ```

Когда метрики рассчитаны и записаны в отведенные для этого таблицы, можем начать наполнение витрины.

```sql
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
 ```

Выберем первые 10 строк по минимальным user_id.

```sql
select user_id, recency, frequency, monetary_value
from analysis.dm_rfm_segments
order by user_id
limit 10;
```

|user_id| recency | frequency | monetary_value |
|----------|---------|---------|---------|
|0	|1	|3	|4
|1	|4	|3	|3
|2	|2	|3	|5
|3	|2	|3	|3
|4	|4	|3	|3
|5	|5	|5	|5
|6	|1	|3	|5
|7	|4	|3	|2
|8	|1	|1	|3
|9	|1	|2	|2

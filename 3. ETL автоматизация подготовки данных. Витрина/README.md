# ETL автоматизация подготовки данных.

### Задача
1. Адаптаировать пайплайн под действующие задачи бизнеса, обеспечить обратную совместимость; <P><P>
2. Построить витрину данных для исследования возвращаемости клиентов;

### Корректируем функцию загрузки инкремента в таблицу (DAG)

Добавлен пункт с подключением к БД и удалением данных за день загрузки (str_del)

```python
def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    print(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    print(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    print(response.content)

    df = pd.read_csv(local_filename)
    df=df.drop('id', axis=1)
    df=df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    str_del = f"delete FROM {pg_schema}.{pg_table} WHERE date_time::date = '{date}'"
    engine.execute(str_del)

    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    print(f'{row_count} rows was inserted')
```

### Внесем изменения в структуру таблиц.

В исходные данные по продажам был добавлен статус отмены заказов и возврата средств.

Добавим поле в stage (user_order_log) и mart зоны (f_sales).

```sql
alter table staging.user_order_log add column status varchar(100) default 'shipped' not null;
```
```sql
alter table mart.f_sales add column order_refunded int4 not null;
```
Изменим процедуру загрузки таблицы f_sales. Для корректности последующих расчетов возвраты заносим со знаком -

```sql
delete from mart.f_sales
where f_sales.date_id in
    (select d_calendar.date_id from mart.d_calendar where mart.d_calendar.date_actual = '{{ds}}');

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
```

### Витрина данных. Возвращаемость клиентов.

#### Схема:

1. new_customers_count — кол-во новых клиентов (тех, которые сделали только один
заказ за рассматриваемый промежуток времени).
2. returning_customers_count — кол-во вернувшихся клиентов (тех,
которые сделали только несколько заказов за рассматриваемый промежуток времени).
3. refunded_customer_count — кол-во клиентов, оформивших возврат за
рассматриваемый промежуток времени.
4. period_name — weekly.
5. period_id — идентификатор периода (номер недели или номер месяца).
6. item_id — идентификатор категории товара.
7. new_customers_revenue — доход с новых клиентов.
8. returning_customers_revenue — доход с вернувшихся клиентов.
9. customers_refunded — количество возвратов клиентов.

#### Создадим таблицу mart.f_customer_retention

```sql
drop table if exists mart.f_customer_retention;
create table mart.f_customer_retention
(new_customers_count int,
returning_customers_count int,
refunded_customer_count int,
period_name varchar(50) default 'weekly' not null,
period_id int not null,
item_id int not null,
new_customers_revenue numeric(10,2),
returning_customers_revenue numeric(10,2),
customers_refunded int);
create index f_customer_retention_idx on mart.f_customer_retention (item_id);
```

#### Наполним витрину данными

```sql
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
	'weekly' as period_name,
	cal.week_of_year as period_id,
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
```

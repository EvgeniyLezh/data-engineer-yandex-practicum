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

Добавлено ограничение по PK и null значениям.

```sql
drop table if exists public.shipping_country_rates;
create table shipping_country_rates
(shipping_country_id serial not null,
shipping_country text null,
shipping_country_base_rate numeric(14,3) null,
primary key (shipping_country_id));
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
```

Информация для заполнения находится в столбце vendor_agreement_description (таблицы shipping), где данные записаны с помощью разделителя ":", поэтому нам потребуется регулярное выражение.

```sql
insert into public.shipping_agreement
(agreementid, agreement_number, agreement_rate, agreement_commission)

select
cast(description[1] as int8) as agreementid,
cast(description[2] as text) as agreement_rate,
cast(description[3] as numeric(14,3)) as agreement_number,
cast(description[4] as numeric(14,3)) as agreement_commission
from (select distinct regexp_split_to_array(vendor_agreement_description, ':+') as description from public.shipping) ship;
```

Проверяем

```sql
select agreementid, agreement_number, agreement_rate, agreement_commission
from public.shipping_agreement
limit 10;
```
|agreementid| agreement_rate| agreement_number| agreement_commission|
|----------|---------|---------|---------|
|32	|vspn-1730	|0.120	|0.020
|47	|vspn-3444	|0.070	|0.030
|19	|vspn-9037	|0.070	|0.020
|59	|vspn-7141	|0.070	|0.010
|51	|vspn-5162	|0.140	|0.020
|49	|vspn-5533	|0.040	|0.030
|38	|vspn-2557	|0.140	|0.020
|46	|vspn-5986	|0.120	|0.030
|16	|vspn-3829	|0.080	|0.030
|56	|vspn-6399	|0.120	|0.010

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
```
Заполним таблица информацией из столбцов shipping_transfer_description и shipping_transfer_rate.

```sql
insert into public.shipping_transfer
(transfer_type, transfer_model, shipping_transfer_rate)

select
cast(description[1] as text) as transfer_type,
cast(description[2] as text) as transfer_model,
cast(shipping_transfer_rate as numeric(14,3)) shipping_transfer_rate
from (select distinct regexp_split_to_array(shipping_transfer_description, ':+') as description, shipping_transfer_rate from public.shipping) ship;
```

Проверяем

```sql
select transfer_type_id, transfer_type, transfer_model,
shipping_transfer_rate
from public.shipping_transfer
limit 10;
```

|transfer_type_id| transfer_type| transfer_model| shipping_transfer_rate|
|----------|---------|---------|---------|
|1	|3p	|ship	|0.025
|2	|1p	|multiplie	|0.050
|3	|3p	|train	|0.020
|4	|3p	|airplane	|0.035
|5	|1p	|ship	|0.030
|6	|1p	|train	|0.025
|7	|1p	|airplane	|0.040
|8	|3p	|multiplie	|0.045

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

### 5. Таблица со статусами о доставке.

```sql
drop table if exists public.shipping_status;
create table shipping_status
(shippingid int8 not null,
status text not null,
state text not null,
shipping_start_fact_datetime timestamp null,
shipping_end_fact_datetime timestamp null,
primary key (shippingid));
```

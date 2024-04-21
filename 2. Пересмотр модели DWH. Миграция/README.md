# Пересмотр модели DWH. Миграция

### Задача
1. Провести миграцию в отдельные логические таблицы; <P><P>
2. Создать витрину данных с анализом заказов интернет-магазина.

### Исходный вид.

![2. Пересмотр модели DWH. Миграция/Исходный вид модели.png](https://github.com/EvgeniyLezh/data-engineer-yandex-practicum/blob/98aaa80b89518ecb1f5e6fbefab18d42d5b1c890/2.%20%D0%9F%D0%B5%D1%80%D0%B5%D1%81%D0%BC%D0%BE%D1%82%D1%80%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8%20DWH.%20%D0%9C%D0%B8%D0%B3%D1%80%D0%B0%D1%86%D0%B8%D1%8F/%D0%98%D1%81%D1%85%D0%BE%D0%B4%D0%BD%D1%8B%D0%B9%20%D0%B2%D0%B8%D0%B4%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8.png)

### Итоговый вид.

![2. Пересмотр модели DWH. Миграция/Итоговый вид модели.png](https://github.com/EvgeniyLezh/data-engineer-yandex-practicum/blob/aeabfd7a6d47a5353ab387207720239f5608e2f6/2.%20%D0%9F%D0%B5%D1%80%D0%B5%D1%81%D0%BC%D0%BE%D1%82%D1%80%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8%20DWH.%20%D0%9C%D0%B8%D0%B3%D1%80%D0%B0%D1%86%D0%B8%D1%8F/%D0%98%D1%82%D0%BE%D0%B3%D0%BE%D0%B2%D1%8B%D0%B9%20%D0%B2%D0%B8%D0%B4%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8.png)

## Создадим необходимые таблицы.

### 1. Справочник стоимости доставки в страны.

```sql
drop table if exists public.shipping_country_rates;
create table shipping_country_rates
(shipping_country_id int8 not null,
shipping_country text null,
shipping_country_base_rate numeric(14,3) null,
primary key (shipping_country_id));
 ```

### 2. Справочник тарифов доставки вендора по договору.

```sql
drop table if exists public.shipping_agreement;
create table shipping_agreement
(agreementid int8 not null,
agreement_number text null,
agreement_rate numeric(14,3) null,
agreement_commission numeric(14,3) null,
primary key (agreementid));
 ```

### 3. Справочник типов доставки.

```sql
drop table if exists public.shipping_transfer;
create table shipping_transfer
(transfer_type_id serial not null,
transfer_type text not null,
transfer_model text not null,
shipping_transfer_rate numeric(14,3) null,
primary key (transfer_type_id));
 ```

### 4. Таблица со справочной информацией о доставках.

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

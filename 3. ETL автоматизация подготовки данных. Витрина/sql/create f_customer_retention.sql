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

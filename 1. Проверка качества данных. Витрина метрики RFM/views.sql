-- orderitems --
create view analysis.orderitems as
(select id, product_id, order_id, "name", price, discount, quantity
from production.orderitems);

-- orders --
create view analysis.orders as
(select order_id, order_ts, user_id, bonus_payment, payment, "cost", bonus_grant, status
from production.orders);

-- orderstatuses --
create view analysis.orderstatuses as
(select id, "key" from production.orderstatuses);

-- orderstatuslog --
create view analysis.orderstatuslog as
(select id, order_id, status_id, dttm
from production.orderstatuslog);

-- products --
create view analysis.products as
(select id, "name", price
from production.products);

-- users --
create view analysis.users as
(select id, "name", login
from production.users);

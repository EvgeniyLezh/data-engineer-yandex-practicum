-- 2. Справочник тарифов вендора по договору.

drop table if exists public.shipping_agreement;
create table shipping_agreement
(agreementid int8 not null,
agreement_number text not null,
agreement_rate numeric(14,3) not null,
agreement_commission numeric(14,3) not null,
primary key (agreementid));

insert into public.shipping_agreement
(agreementid, agreement_number, agreement_rate, agreement_commission)

select
cast(description[1] as int8) as agreementid,
cast(description[2] as text) as agreement_rate,
cast(description[3] as numeric(14,3)) as agreement_number,
cast(description[4] as numeric(14,3)) as agreement_commission
from (select distinct regexp_split_to_array(vendor_agreement_description, ':+') as description from public.shipping) ship;

select agreementid, agreement_number, agreement_rate, agreement_commission
from public.shipping_agreement
limit 10;

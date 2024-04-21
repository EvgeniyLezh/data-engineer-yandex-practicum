-- 2. Справочник тарифов вендора по договору

create table shipping_agreement
(agreementid int8 not null,
agreement_number text null,
agreement_rate numeric(14,3) null,
agreement_commission numeric(14,3) null,
primary key (agreementid))
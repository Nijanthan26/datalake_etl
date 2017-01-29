
--Invoice fact, need to convert to spark jobs
drop table if exists antuit_pricing.invoice_fact;
create table antuit_pricing.invoice_fact
(
legacy_source_system string,
enterprise_id string,
customer_name string,
cost_center string,
workday_location string,
legacy_customer_code string,
legacy_warehouse_id string,
invoice_id string,
invoice_date string,
total_invoice_amt double
);

--HIGHJUMP invoice details
insert into antuit_pricing.invoice_fact
select distinct
tab.legacy_source_system
,xref.LIN_CUSTOMER_ENTERPRISE_ID__C
,xref.LIN_SURVIVOR_CUSTOMER_NAME__C
,costc.LIN_WORKDAY_COST_CENTER__C
,costc.LIN_WORKDAY_LOCATION_ID__C
,xref.LIN_LEGACY_CUSTOMER_CODE__C
,tab.wh_id
,tab.invoice_id
,tab.generated_date as invoice_date
,tab.Total_Invoice_amt
from
(select
'HIGHJUMP' as legacy_source_system
,inv_amt.customer_code
,inv_amt.invoice_id
,inv_amt.wh_id
,inv_amt.generated_date
,sum(inv_amt.charge_amount) as Total_Invoice_amt 
from														
(select 
inv.invoice_id
,inv.generated_date
,chg.charge_id
,chg.chargeback_id
,chg.wh_id
,chg.charge_amount
,cit.contract_id
,cm.customer_id
,cust.customer_code														
from 
antuit_stage.hj_t_bmm_invoice inv 
inner join antuit_stage.hj_t_bmm_charge chg on inv.invoice_id = chg.invoice_id														
left join antuit_stage.hj_t_bmm_contract_invoice_type cit on inv.contract_invoice_type_id = cit.contract_invoice_type_id														
inner join antuit_stage.hj_t_bmm_contract_master cm on cit.contract_id = cm.contract_id 														
left join antuit_stage.hj_t_bmm_customer cust on cm.customer_id = cust.customer_id														
) inv_amt														
group by 
inv_amt.customer_code
,inv_amt.generated_date
,inv_amt.invoice_id
,inv_amt.wh_id) tab																											
left join (select distinct LIN_CUSTOMER_ENTERPRISE_ID__C,LIN_SURVIVOR_CUSTOMER_NAME__C,LIN_SOURCE_SYSTEM_NAME__C,LIN_LEGACY_CUSTOMER_CODE__C from antuit_pricing.customer_xref where LIN_SOURCE_SYSTEM_NAME__C ='HIGHJUMP') xref
on (concat(tab.legacy_source_system, nvl(tab.customer_code,cast('' as string))))=(concat(nvl(xref.LIN_SOURCE_SYSTEM_NAME__C,cast('' as string)),nvl(xref.LIN_LEGACY_CUSTOMER_CODE__C,cast('' as string))))
left join (select distinct uid,LIN_WORKDAY_COST_CENTER__C,LIN_WORKDAY_LOCATION_ID__C from antuit_pricing.costcenter_xref) costc
on concat(tab.legacy_source_system,nvl((case when tab.wh_id like '0%' then cast(cast(tab.wh_id as int) as string) else cast(tab.wh_id as string) end),cast('' as string)))=costc.uid


--MRS invoice details
insert into antuit_pricing.invoice_fact
select
distinct
tab.legacy_source_system
,xref.LIN_CUSTOMER_ENTERPRISE_ID__C
,xref.LIN_SURVIVOR_CUSTOMER_NAME__C
,costc.LIN_WORKDAY_COST_CENTER__C
,costc.LIN_WORKDAY_LOCATION_ID__C
,xref.LIN_LEGACY_CUSTOMER_CODE__C
,tab.facilityid
,tab.finvoice
,tab.fdatestamp
,tab.inv_amt
from
(select
'MRS' as legacy_source_system
,fcustcode
,facilityid
,finvoice
,fdatestamp
,sum(forigamt) as inv_amt
from
antuit_stage.dl_mrs_billhist_delta 
group by 
fcustcode
,facilityid
,finvoice
,fdatestamp
) tab
left join (select distinct LIN_CUSTOMER_ENTERPRISE_ID__C,LIN_SURVIVOR_CUSTOMER_NAME__C,LIN_SOURCE_SYSTEM_NAME__C,LIN_LEGACY_CUSTOMER_CODE__C from antuit_pricing.customer_xref where LIN_SOURCE_SYSTEM_NAME__C ='MRS') xref
on (concat(tab.legacy_source_system, nvl(tab.fcustcode,cast('' as string))))=(concat(nvl(xref.LIN_SOURCE_SYSTEM_NAME__C,cast('' as string)),nvl(xref.LIN_LEGACY_CUSTOMER_CODE__C,cast('' as string))))
left join (select distinct uid,LIN_WORKDAY_COST_CENTER__C,LIN_WORKDAY_LOCATION_ID__C from antuit_pricing.costcenter_xref) costc
on concat(tab.legacy_source_system,nvl((case when tab.facilityid like '0%' then cast(cast(tab.facilityid as int) as string) else cast(tab.facilityid as string) end),cast('' as string)))=costc.uid

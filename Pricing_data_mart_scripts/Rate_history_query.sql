
--Rate History Fact, need to convert to spark jobs
drop table if exists antuit_pricing.rate_history;
create table antuit_pricing.rate_history
(
legacy_source_system string
,Enterprise_Id string
,Normalised_Client_name string
,Cost_Center string
,Workday_Location string
,Normalised_Charge_Code string
,Normalised_charge_name string
,legacy_customer_code string
,legacy_chargeback_desc string
,legacy_warehouse_id string
,legacy_charge_code string
,Charge_effective_date string
,Charge_expiry_date string
,Increment double
,rate double
,uom string
)

--MRS Rate
insert into antuit_pricing.rate_history
select distinct 
fnl.legacy_source_system
,fnl.lin_customer_enterprise_id__c
,fnl.lin_survivor_customer_name__c
,fnl.lin_workday_cost_center__c
,fnl.lin_workday_location_id__c
,fnl.lin_consolidated_charge_code__c		
,fnl.lin_consolidated_charge_name__c
,fnl.fcustcode
,fnl.fnotes
,fnl.facilityid
,fnl.fgl
,fnl.invoice_min_date
,fnl.invoice_max_date
,fnl.fbasis
,fnl.frate
,fnl.line_billed_by
from
(
select distinct
a.*
,xref.LIN_SOURCE_SYSTEM_NAME__C
,xref.LIN_CUSTOMER_ENTERPRISE_ID__C
,xref.LIN_SURVIVOR_CUSTOMER_NAME__C
,costc.LIN_WORKDAY_COST_CENTER__C
,costc.LIN_WORKDAY_LOCATION_ID__C
,chxref.LIN_CONSOLIDATED_CHARGE_CODE__C
,chxref.LIN_CONSOLIDATED_CHARGE_NAME__C
from
(
select 
'MRS' as legacy_source_system
,tab.fcustcode
,tab.facilityid
,tab.fnotes
,tab.fgl
,tab.num_days_cust_billed
,min(tab.invoice_date) invoice_min_date
,max(tab.invoice_date) invoice_max_date
,tab.fbilltype
,tab.line_billed_by
,tab.frate
,tab.fbasis
from
(
	select
	count(*) as counts
	,a.invoice_date
	,a.fcustcode
	,a.facilityid
	,a.fnotes
	,a.fgl
	,a.num_days_cust_billed
	,a.fbilltype
	,a.line_billed_by
	,case when upper(a.fbilltype) = 'IS' then min(a.Initial_handling_Rate) end as fis_rate
	,case when upper(a.fbilltype) = 'IS' then a.fis_type end as fis_type
	,case when upper(a.fbilltype) = 'IS' then a.fis_basis end as fis_basis
	,case when upper(a.fbilltype) = 'IS' then a.fis_days end as fis_days
	,case when upper(a.fbilltype) = 'RS' then min(a.Recurring_Storage_Rate) end as frs_rate
	,case when upper(a.fbilltype) = 'RS' then a.frs_type end as frs_type
	,case when upper(a.fbilltype) = 'RS' then a.frs_basis end as frs_basis
	,case when upper(a.fbilltype) = 'RS' then a.frs_days end as frs_days
	,case when upper(a.fbilltype) = 'HI' then min(a.Handling_in_Rate) end as fhi_rate
	,case when upper(a.fbilltype) = 'HI' then a.fhi_type end as fhi_type
	,case when upper(a.fbilltype) = 'HO' then min(a.Handling_Out_Rate) end as fho_rate
	,case when upper(a.fbilltype) = 'HO' then a.fho_type end as fho_type
	,a.frate
	,a.fbasis
	from
	(
	select  
		bh.finvoice, bh.fidate as Invoice_date, bh.fbillfrom, bh.flastbill,bh.fcustcode,bh.facilityid,bh.flot,
		bh.fprodgroup,bh.fbatch,bh.fsequence, bh.ftrack,
		bh.fproduct,bh.fowner,bh.fsuplrprod, bh.fqty,bh.fweight,bh.fwgttype,bh.fcube,bh.fpal,bh.fpricecode,
		bh.fbreakcode,bh.fbreakfact,bh.fmisccode, bh.fnotes, bh.fdebit,bh.fcredit, bh.forigamt, bh.fgl,
		bh.fnumdays as Num_Days_Cust_billed, bh.farbatch,bh.fbilltype,bh.frate,bh.fbasis,bh.fbilledby as Line_billed_by,
		bh.fdatestamp as Date_Invoice_Posted,
		prc.fdescrip as Generic_description, 
		prc.fsplitcut,prc.fis_type,prc.fis_rate as Initial_handling_Rate,
		prc.fis_basis, prc.fis_days,prc.frs_type,prc.frs_rate as Recurring_Storage_Rate,prc.frs_basis,prc.frs_days,
		prc.fhi_type, prc.fhi_rate as Handling_in_Rate, prc.fho_type,prc.fho_rate as Handling_Out_Rate,
		prc.fin_min as Minimum_Initial_Amt, prc.finmiaplby as MinInitial_appld_aganist,
		cust.fcompany as Company_Name, cust.fmininvchg as Minimum_Invoice_Charge
	from 
		antuit_stage.mrs_billhist bh 
		inner join antuit_stage.mrs_price prc on
		bh.fcustcode = prc.fcustcode and
		bh.facilityid = prc.facilityid and
		bh.fpricecode = prc.fpricecode
		left join antuit_stage.mrs_customer cust on
		bh.fcustcode = cust.fcustcode and
		bh.facilityid = cust.facilityid
	) a
	group by
	a.invoice_date
	,a.fcustcode
	,a.facilityid
	,a.fnotes
	,a.fgl
	,a.num_days_cust_billed
	,a.fbilltype
	,a.line_billed_by
	,case when upper(a.fbilltype) = 'IS' then a.fis_type end
	,case when upper(a.fbilltype) = 'IS' then a.fis_basis end
	,case when upper(a.fbilltype) = 'IS' then a.fis_days end
	,case when upper(a.fbilltype) = 'RS' then a.frs_type end
	,case when upper(a.fbilltype) = 'RS' then a.frs_basis end
	,case when upper(a.fbilltype) = 'RS' then a.frs_days end
	,case when upper(a.fbilltype) = 'HI' then a.fhi_type end
	,case when upper(a.fbilltype) = 'HO' then a.fho_type end
	,a.frate
	,a.fbasis
	)tab
group by 
tab.fcustcode
,tab.facilityid
,tab.fnotes
,tab.fgl
,tab.num_days_cust_billed
,tab.fbilltype
,tab.line_billed_by
,tab.frate
,tab.fbasis
order by
tab.fcustcode
,tab.facilityid
,min(tab.invoice_date)
,tab.fbilltype
) a
left join (select distinct uid,LIN_CUSTOMER_ENTERPRISE_ID__C,LIN_SURVIVOR_CUSTOMER_NAME__C,LIN_SOURCE_SYSTEM_NAME__C from antuit_pricing.customer_xref where LIN_SOURCE_SYSTEM_NAME__C ='MRS') xref
on concat(a.legacy_source_system, nvl((case when a.facilityid like '0%' then cast(cast(a.facilityid as int) as string) else cast(a.facilityid as string) end),cast('' as string)),
nvl((case when a.fcustcode like '0%' then cast(cast(a.fcustcode as int) as string) else cast(a.fcustcode as string) end),cast('' as string)))= xref.UID
left join (select distinct uid,LIN_CONSOLIDATED_CHARGE_CODE__C,LIN_CONSOLIDATED_CHARGE_NAME__C from antuit_pricing.chargecode_xref) chxref
on concat(a.legacy_source_system,nvl((case when a.fgl like '0%' then cast(cast(a.fgl as int) as string) else cast(a.fgl as string) end),cast('' as string)))=chxref.uid
left join (select distinct uid,LIN_WORKDAY_COST_CENTER__C,LIN_WORKDAY_LOCATION_ID__C from antuit_pricing.costcenter_xref) costc
on concat(a.legacy_source_system,nvl((case when a.facilityid like '0%' then cast(cast(a.facilityid as int) as string) else cast(a.facilityid as string) end),cast('' as string)))=costc.uid
) fnl


--HIGHJUMP Rate
insert into antuit_pricing.rate_history
select distinct
fnl.legacy_source_system
,fnl.lin_customer_enterprise_id__c
,fnl.lin_survivor_customer_name__c
,fnl.lin_workday_cost_center__c
,fnl.lin_workday_location_id__c
,fnl.lin_consolidated_charge_code__c		
,fnl.lin_consolidated_charge_name__c
,fnl.customer_code
,fnl.chargeback_type
,fnl.wh_id
,fnl.chargeback_code 
,fnl.effective_date		
,fnl.expiry_date	
,fnl.increment
,fnl.rate
,fnl.uom
from
(
select distinct
a.*
--,xref.LIN_SOURCE_SYSTEM_NAME__C
,xref.LIN_CUSTOMER_ENTERPRISE_ID__C
,xref.LIN_SURVIVOR_CUSTOMER_NAME__C
,costc.LIN_WORKDAY_COST_CENTER__C
,costc.LIN_WORKDAY_LOCATION_ID__C
,chxref.LIN_CONSOLIDATED_CHARGE_CODE__C
,chxref.LIN_CONSOLIDATED_CHARGE_NAME__C
from
(
SELECT DISTINCT
'HIGHJUMP' as legacy_source_system
,tbc.customer_code
,tbcitc.chargeback_code
,tbcm.wh_id
,tbct.chargeback_type
,COALESCE(tbcr.rate,tbpmp.rate,tbpmcp.rate) AS Rate
,ISNULL(tbcr.weight_increment,tbpmp.rate_increment) as Increment
,tbcr.uom
,tbcitc.effective_date
,tbcitc.expiry_date
FROM
antuit_stage.hj_t_bmm_customer tbc
INNER JOIN
antuit_stage.hj_t_bmm_contract_master tbcm
    ON tbc.wh_id = tbcm.wh_id
        AND tbc.customer_id = tbcm.customer_id
INNER JOIN
antuit_stage.hj_t_bmm_contract_invoice_type tbcit
    ON tbcm.contract_id = tbcit.contract_id
INNER JOIN
antuit_stage.hj_t_bmm_cont_inv_type_chargeback tbcitc
    ON tbcit.contract_invoice_type_id = tbcitc.contract_invoice_type_id
INNER JOIN
antuit_stage.hj_t_bmm_chargeback_type tbct
    ON tbcitc.chargeback_type_id = tbct.chargeback_type_id
LEFT JOIN
antuit_stage.hj_t_bmm_chargeback_rate tbcr
    ON tbcitc.chargeback_id = tbcr.chargeback_id
LEFT JOIN
antuit_stage.hj_t_bmm_chargeback_transaction tbctran
    ON tbcitc.chargeback_id = tbctran.chargeback_id
LEFT JOIN
antuit_stage.hj_t_bmm_param_manual_prompt tbpmp
    ON tbcitc.chargeback_id = tbpmp.chargeback_id
LEFT OUTER JOIN    
antuit_stage.hj_t_bmm_param_manual_csr_prompt tbpmcp
    ON tbcitc.chargeback_id = tbpmcp.chargeback_id
LEFT JOIN
antuit_stage.hj_t_bmm_param_wms_event tbpwe
    ON tbcitc.chargeback_id = tbpwe.chargeback_id
LEFT JOIN
antuit_stage.hj_t_bmm_param_uom_anniversary tbpua
    ON tbcitc.chargeback_id = tbpua.chargeback_id
LEFT JOIN
antuit_stage.hj_t_bmm_param_uom_storage tbpus
    ON tbcitc.chargeback_id = tbpus.chargeback_id
LEFT JOIN
antuit_stage.hj_v_bmm_transaction vbt
    ON tbctran.transaction_code = vbt.tran_type
ORDER BY
tbcitc.chargeback_code
) a
left join (select distinct LIN_CUSTOMER_ENTERPRISE_ID__C,LIN_SURVIVOR_CUSTOMER_NAME__C,LIN_SOURCE_SYSTEM_NAME__C,LIN_LEGACY_CUSTOMER_CODE__C from antuit_pricing.customer_xref where LIN_SOURCE_SYSTEM_NAME__C ='HIGHJUMP') xref
on (concat(a.legacy_source_system, nvl(a.customer_code,cast('' as string))))=(concat(nvl(xref.LIN_SOURCE_SYSTEM_NAME__C,cast('' as string)),nvl(xref.LIN_LEGACY_CUSTOMER_CODE__C,cast('' as string))))
left join (select distinct uid,LIN_CONSOLIDATED_CHARGE_CODE__C,LIN_CONSOLIDATED_CHARGE_NAME__C from antuit_pricing.chargecode_xref) chxref
on concat(a.legacy_source_system,nvl((case when a.chargeback_code like '0%' then cast(cast(a.chargeback_code as int) as string) else cast(a.chargeback_code as string) end),cast('' as string)))=chxref.uid
left join (select distinct uid,LIN_WORKDAY_COST_CENTER__C,LIN_WORKDAY_LOCATION_ID__C from antuit_pricing.costcenter_xref) costc
on concat(a.legacy_source_system,nvl((case when a.wh_id like '0%' then cast(cast(a.wh_id as int) as string) else cast(a.wh_id as string) end),cast('' as string)))=costc.uid
) fnl
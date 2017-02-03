package com.lineage.db

import com.lineage.RowHash
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._



object commonPricingStageExecutor {
  
  
	def main(args: Array[String]): Unit = {
			    val conf = new SparkConf().setAppName("sqlrun")
					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc)
					import sqlContext.implicits._
					
					val url = "jdbc:postgresql://metricsone.cpslmao02wkq.us-west-2.rds.amazonaws.com:5432/postgres"
					val username = "root"
					val password = "TiMLRHdCNLftYOLskOkF"
					val prop = new java.util.Properties
					
					prop.setProperty("user",username)
					prop.setProperty("password",password)
  
					val mrsStagingDf1 = sqlContext.sql("""select  
nvl((case when matc.facilityid like '0%' then cast(cast(matc.facilityid as int) as string) else cast(matc.facilityid as string) end),cast('' as string)) as facilityid 
,nvl((case when matc.fcustcode like '0%' then cast(cast(matc.fcustcode as int) as string) else cast(matc.fcustcode as string) end),cast('' as string)) as fcustcode 
,matc.flot 
,matc.finvoice 
,matc.fidate as fdatestamp
,matc.forigamt 
,matc.frate 
,matc.fqty as fqty_billed 
,matc.fweight as fweight_billed 
,matc.fbasis 
,matc.fgl  
,matc.fnumdays
,matc.fbilltype
,matc.fnotes
,(case when upper(matc.fbilltype)='RS' then e.frs_days
when upper(matc.fbilltype)='IS' then e.fis_days else matc.fnumdays end) as ren_pd
,matc.fbilledby 
from 
(select distinct d.*,c.facilityid as facilityid2,c.ftrack2,c.fcustcode as fcustcode2
from antuit_stage.dl_mrs_billhist_delta d
inner join (select distinct facilityid,ftrack2,fcustcode from staging.mrs_inv_mst_delta) c
on 
(d.facilityid=c.facilityid
and d.fcustcode=c.fcustcode
and d.ftrack=c.ftrack2)) matc
left join antuit_stage.dl_mrs_price_delta e on (
matc.fcustcode = e.fcustcode and 
matc.facilityid = e.facilityid and
matc.fpricecode = e.fpricecode)""")

					val mrsStagingDf2 = sqlContext.sql("""select 
nvl((case when matc.facilityid like '0%' then cast(cast(matc.facilityid as int) as string) else cast(matc.facilityid as string) end),cast('' as string)) as facilityid 
,nvl((case when matc.fcustcode like '0%' then cast(cast(matc.fcustcode as int) as string) else cast(matc.fcustcode as string) end),cast('' as string)) as fcustcode 
,matc.flot 
,matc.finvoice 
,matc.fidate as fdatestamp
,matc.forigamt 
,matc.frate 
,matc.fqty as fqty_billed 
,matc.fweight as fweight_billed 
,matc.fbasis 
,matc.fgl  
,matc.fnumdays
,matc.fbilltype
,matc.fnotes
,(case when upper(matc.fbilltype)='RS' then e.frs_days
when upper(matc.fbilltype)='IS' then e.fis_days else matc.fnumdays end) as ren_pd
,matc.fbilledby 
from 
(select distinct d.*,c.facilityid as facilityid2,c.ftrack2,c.fcustcode as fcustcode2
from antuit_stage.dl_mrs_billhist_delta d
left outer join (select distinct facilityid,ftrack2,fcustcode from staging.mrs_inv_mst_delta) c
on 
(d.facilityid=c.facilityid
and d.fcustcode=c.fcustcode
and d.ftrack=c.ftrack2)
where c.ftrack2 is null
) matc
left join antuit_stage.dl_mrs_price_delta e on (
matc.fcustcode = e.fcustcode and 
matc.facilityid = e.facilityid and
matc.fpricecode = e.fpricecode)""")


val mrsStagingDf = mrsStagingDf1.unionAll(mrsStagingDf2)

val hjPreStaging1Df = sqlContext.sql("""select distinct
d.wh_id,
m.customer_code,
d.lot_number,
d.invoice_id,
inv.generated_date,
d.charge_amount,
chrate.rate,
d.report_qty,
d.report_weight,
chrate.weight_increment,
f.chargeback_code, 
cast(null as string) as hj_ren_pd,
d.uom,
g.chargeback_type
from
(select distinct charge_id from antuit_stage.hj_t_bmm_charge_event_ref) c
left join antuit_stage.hj_t_bmm_charge d on (c.charge_id = d.charge_id)
left join antuit_stage.hj_t_bmm_chargeback_rate chrate on (d.chargeback_rate_id = chrate.chargeback_rate_id)	
left join (select chargeback_id,contract_invoice_type_id,chargeback_type_id,chargeback_code from antuit_stage.hj_t_bmm_cont_inv_type_chargeback) f on (d.chargeback_id = f.chargeback_id and chrate.chargeback_id = f.chargeback_id) 
left join (select invoice_id,contract_invoice_type_id,generated_date from antuit_stage.hj_t_bmm_invoice) inv on (d.invoice_id=inv.invoice_id)
left join antuit_stage.hj_t_bmm_contract_invoice_type k on (inv.contract_invoice_type_id = k.contract_invoice_type_id and f.contract_invoice_type_id=k.contract_invoice_type_id) 
left join (select contract_id,customer_id from antuit_stage.hj_t_bmm_contract_master) l on (k.contract_id = l.contract_id) 
left join (select customer_code,customer_id from antuit_stage.hj_t_bmm_customer) m on (l.customer_id = m.customer_id)	
left join (select distinct chargeback_type_id,chargeback_type from antuit_stage.hj_t_bmm_chargeback_type where chargeback_type='WMS Event') g on (f.chargeback_type_id = g.chargeback_type_id)""")

hjPreStaging1Df.registerTempTable("hj_pre_staging_1")

mrsStagingDf.registerTempTable("mrs_staging")

val hjStagingDf1 = sqlContext.sql("select distinct * from hj_pre_staging_1")


val hjPreStaging2Df = sqlContext.sql("""select distinct
chg.wh_id
,cust.customer_code
,chg.lot_number
,chg.invoice_id
,inv.generated_date
,chg.charge_amount
,chg.rate
,chg.report_qty
,chg.report_weight
,cbrate.weight_increment
,citc.chargeback_code
,case when freq.every_x is null then pua.anniversary_period else freq.every_x end as ren_pd
,chg.uom
,ct.chargeback_type
from
antuit_stage.hj_t_bmm_charge chg 
LEFT join antuit_stage.hj_t_bmm_cont_inv_type_chargeback citc on (chg.chargeback_id = citc.chargeback_id)
left join antuit_stage.hj_t_bmm_chargeback_type ct on (citc.chargeback_type_id = ct.chargeback_type_id)
left join antuit_stage.hj_t_bmm_chargeback_rate cbrate on (citc.chargeback_id = cbrate.chargeback_id and cbrate.chargeback_rate_id=chg.chargeback_rate_id)
left join antuit_stage.hj_t_bmm_invoice inv on (chg.invoice_id = inv.invoice_id)
left join antuit_stage.hj_t_bmm_contract_invoice_type cit on (cit.contract_invoice_type_id = inv.contract_invoice_type_id and citc.contract_invoice_type_id=cit.contract_invoice_type_id)
left join antuit_stage.hj_t_bmm_contract_master cm on (cit.contract_id = cm.contract_id)
left join antuit_stage.hj_t_bmm_customer cust on (cm.customer_id = cust.customer_id)
left join antuit_stage.hj_t_bmm_param_uom_storage pus on (citc.chargeback_id = pus.chargeback_id)
left join antuit_stage.hj_t_bmm_frequency freq on (citc.chargeback_id = freq.chargeback_id)
left join antuit_stage.hj_t_bmm_param_uom_anniversary pua on (citc.chargeback_id = pua.chargeback_id)
left join antuit_stage.hj_t_bmm_param_recurring pr on (citc.chargeback_id = pr.chargeback_id)
left join antuit_stage.hj_t_bmm_param_manual_csr_prompt mcp on (citc.chargeback_id = mcp.chargeback_id)
left join antuit_stage.hj_t_bmm_param_manual_prompt mp on (citc.chargeback_id = mp.chargeback_id)
left join antuit_stage.hj_t_bmm_param_manual_adhoc ma on (citc.chargeback_id = ma.chargeback_id)
where lower(ct.chargeback_type) <>'wms event'""")

hjPreStaging2Df.registerTempTable("hj_pre_staging_2")

//val hjStagingDf2 = sqlContext.sql("select * from hj_pre_staging_2")

val hjStagingDf = hjStagingDf1.unionAll(hjPreStaging2Df)

hjStagingDf.registerTempTable("hj_staging")


val mrsCustXrefDf = sqlContext.sql("""select
tab.*
,acxref.currencyisocode
,acxref.NAME
,acxref.BILLINGSTREET
,acxref.BILLINGCITY
,acxref.BILLINGSTATE
,acxref.BILLINGPOSTALCODE
,acxref.BILLINGCOUNTRY
,acxref.PHONE
,acxref.FAX
,acxref.ACCOUNTNUMBER
,acxref.WEBSITE 
from
(select 
stg.*
,concat('MRS',stg.facilityid, nvl(stg.fcustcode,cast('' as string))) as UID_stg
,'MRS' as LIN_SOURCE_SYSTEM_NAME__C
,xref.LIN_CUSTOMER_ENTERPRISE_ID__C
,xref.LIN_SURVIVOR_CUSTOMER_NAME__C
,xref.LIN_ACCOUNT__C
,chxref.LIN_CONSOLIDATED_CHARGE_CODE__C
,chxref.LIN_CONSOLIDATED_CHARGE_NAME__C
,costc.LIN_WORKDAY_COST_CENTER__C
,costc.LIN_WORKDAY_LOCATION_ID__C
from 
mrs_staging stg
left join (select distinct uid,LIN_ACCOUNT__C,LIN_CUSTOMER_ENTERPRISE_ID__C,LIN_SURVIVOR_CUSTOMER_NAME__C,LIN_SOURCE_SYSTEM_NAME__C from antuit_pricing.customer_xref where LIN_SOURCE_SYSTEM_NAME__C ='MRS') xref
on concat('MRS', nvl((case when stg.facilityid like '0%' then cast(cast(stg.facilityid as int) as string) else cast(stg.facilityid as string) end),cast('' as string)),
nvl((case when stg.fcustcode like '0%' then cast(cast(stg.fcustcode as int) as string) else cast(stg.fcustcode as string) end),cast('' as string)))= xref.UID
left join (select distinct uid,LIN_CONSOLIDATED_CHARGE_CODE__C,LIN_CONSOLIDATED_CHARGE_NAME__C from antuit_pricing.chargecode_xref where lin_legacy_system__c='MRS') chxref
on concat('MRS',nvl((case when stg.fgl like '0%' then cast(cast(stg.fgl as int) as string) else cast(stg.fgl as string) end),cast('' as string)))=chxref.uid
left join (select distinct uid,LIN_WORKDAY_COST_CENTER__C,LIN_WORKDAY_LOCATION_ID__C from antuit_pricing.costcenter_xref where legacy_lin_system__c='MRS') costc
on concat('MRS',nvl((case when stg.facilityid like '0%' then cast(cast(stg.facilityid as int) as string) else cast(stg.facilityid as string) end),cast('' as string)))=costc.uid
) tab
LEFT JOIN antuit_pricing.account_revised_xref ACXREF
ON (tab.LIN_ACCOUNT__C=acxref.id)""")

mrsCustXrefDf.registerTempTable("mrs_cust_xref")

val hjCustRefDf = sqlContext.sql("""select 
tab.*
,acxref.currencyisocode
,acxref.NAME
,acxref.BILLINGSTREET
,acxref.BILLINGCITY
,acxref.BILLINGSTATE
,acxref.BILLINGPOSTALCODE
,acxref.BILLINGCOUNTRY
,acxref.PHONE
,acxref.FAX
,acxref.ACCOUNTNUMBER
,acxref.WEBSITE 
from
(
select 
stg.*
,concat('HIGHJUMP', nvl(stg.customer_code,cast('' as string))) as UID_stg
,cast('HIGHJUMP' as string) as LIN_SOURCE_SYSTEM_NAME__C
,xref.LIN_CUSTOMER_ENTERPRISE_ID__C
,xref.LIN_SURVIVOR_CUSTOMER_NAME__C
,xref.LIN_ACCOUNT__C
,chxref.LIN_CONSOLIDATED_CHARGE_CODE__C
,chxref.LIN_CONSOLIDATED_CHARGE_NAME__C
,costc.LIN_WORKDAY_COST_CENTER__C
,costc.LIN_WORKDAY_LOCATION_ID__C
from 
hj_staging stg	
left join (select distinct LIN_ACCOUNT__C,LIN_CUSTOMER_ENTERPRISE_ID__C,LIN_SURVIVOR_CUSTOMER_NAME__C,LIN_SOURCE_SYSTEM_NAME__C,LIN_LEGACY_CUSTOMER_CODE__C from antuit_pricing.customer_xref where LIN_SOURCE_SYSTEM_NAME__C ='HIGHJUMP') xref
on (concat('HIGHJUMP', nvl(stg.customer_code,cast('' as string))))=(concat(nvl(xref.LIN_SOURCE_SYSTEM_NAME__C,cast('' as string)),nvl(xref.LIN_LEGACY_CUSTOMER_CODE__C,cast('' as string))))
left join (select distinct uid,LIN_CONSOLIDATED_CHARGE_CODE__C,LIN_CONSOLIDATED_CHARGE_NAME__C,lin_legacy_charge_code__c from antuit_pricing.chargecode_xref where lin_legacy_system__c='HIGHJUMP') chxref
on concat('HIGHJUMP',nvl((case when stg.chargeback_code like '0%' then cast(cast(stg.chargeback_code as int) as string) else cast(stg.chargeback_code as string) end),cast('' as string)))=chxref.uid
left join (select distinct uid,LIN_WORKDAY_COST_CENTER__C,LIN_WORKDAY_LOCATION_ID__C from antuit_pricing.costcenter_xref where legacy_lin_system__c='HIGHJUMP') costc
on concat('HIGHJUMP',nvl((case when stg.wh_id like '0%' then cast(cast(stg.wh_id as int) as string) else cast(stg.wh_id as string) end),cast('' as string)))=costc.uid
) tab
LEFT JOIN antuit_pricing.account_revised_xref ACXREF
ON (tab.LIN_ACCOUNT__C=acxref.id)""")

hjCustRefDf.registerTempTable("hj_cust_xref")


val commonPricingStageDf1 = sqlContext.sql("""select
lin_source_system_name__c  
,nvl(lin_customer_enterprise_id__c ,"NA") as lin_customer_enterprise_id__c
,nvl(lin_workday_cost_center__c ,"NA") as lin_workday_cost_center__c
,nvl(lin_workday_location_id__c ,"NA") as lin_workday_location_id__c
,nvl(cast(lin_consolidated_charge_code__c as string) , "NA") as lin_consolidated_charge_code__c
,nvl(lin_consolidated_charge_name__c ,"NA") as lin_consolidated_charge_name__c
,nvl(facilityid ,"NA") as facility_id 
,regexp_replace(nvl(fcustcode,"NA"),"[\u0000-\u001f]", "NA") as customer_id
,nvl(flot ,"NA") as lot_number
,nvl(finvoice ,"NA") as invoice
,nvl(fdatestamp ,"NA") as invoice_date
,nvl(forigamt , 0) as total_invoiced_amount
,nvl(frate ,0) as rate_charged
,nvl(fqty_billed ,0) as billed_qty
,nvl(fweight_billed  ,0) as billed_weight
,nvl(fbasis ,0) as basis
,nvl(fgl ,"NA") as charge_code
,nvl(cast(ren_pd as string) ,"NA") as ren_pd
,nvl(fbilledby ,"NA") as unit_of_measurement
,nvl(currencyisocode,"NA") as currencyisocode
,nvl(lin_survivor_customer_name__c ,"NA") as lin_survivor_customer_name__c
,nvl(billingstreet ,"NA") as billingstreet
,nvl(billingcity ,"NA") as billingcity
,nvl(billingstate ,"NA") as billingstate
,nvl(billingpostalcode ,"NA") as billingpostalcode
,nvl(billingcountry ,"NA") as billingcountry
,nvl(phone ,"NA") as phone 
,nvl(fax ,"NA") as fax
,nvl(accountnumber ,"NA") as accountnumber
,nvl(website ,"NA") as website
from
mrs_cust_xref""")

val commonPricingStageDf2 = sqlContext.sql("""select
lin_source_system_name__c
,nvl(lin_customer_enterprise_id__c , "NA" ) as lin_customer_enterprise_id__c
,nvl(lin_workday_cost_center__c , "NA" ) as lin_workday_cost_center__c
,nvl(lin_workday_location_id__c , "NA" ) as lin_workday_location_id__c
,nvl(cast(lin_consolidated_charge_code__c as string),"NA") as lin_consolidated_charge_code__c
,nvl(lin_consolidated_charge_name__c , "NA" ) as lin_consolidated_charge_name__c
,nvl(wh_id  , "NA" ) as facility_id
,regexp_replace(nvl(customer_code,"NA"),"[\u0000-\u001f]", "NA") as customer_id
,nvl(lot_number , "NA" ) as lot_number
,nvl(cast(invoice_id as string),"NA") as invoice
,nvl(generated_date  , "NA" ) as invoice_date
,nvl(charge_amount  , 0 ) as total_invoiced_amount
,nvl(rate   , 0 ) as rate_charged
,nvl(report_qty , 0 ) as billed_qty 
,nvl(report_weight  , 0 ) as billed_weight
,nvl(weight_increment , 0 ) as basis 
,nvl(chargeback_code  , "NA" ) as charge_code
,nvl(cast(hj_ren_pd as string) , "NA" ) as ren_pd 
,nvl(uom  , "NA" )as unit_of_measurement
,nvl(currencyisocode,"NA") as currencyisocode
,nvl(lin_survivor_customer_name__c , "NA" ) as lin_survivor_customer_name__c
,nvl(billingstreet , "NA" ) as billingstreet
,nvl(billingcity , "NA" ) as billingcity
,nvl(billingstate , "NA" ) as billingstate
,nvl(billingpostalcode , "NA" ) as billingpostalcode
,nvl(billingcountry , "NA" ) as billingcountry
,nvl(phone , "NA" ) as phone
,nvl(fax , "NA" ) as fax
,nvl(accountnumber , "NA" ) as accountnumber
,nvl(website , "NA" ) as website
from
hj_cust_xref""")

//val commonPricingStageDF = commonPricingStageDf1.unionAll(commonPricingStageDf2)

//println("Total count is :............................................................."+(commonPricingStageDf1.count + commonPricingStageDf2.count))

commonPricingStageDf1.write.mode("append").jdbc(url, "common_pricing_stage_test", prop)

//commonPricingStageDf1.registerTempTable("commonPricingStage1")
//commonPricingStageDf2.registerTempTable("commonPricingStage2")

//sqlContext.sql("create table default.commonPricingStage1 as select * from commonPricingStage1")
//sqlContext.sql("create table default.commonPricingStage2 as select * from commonPricingStage2")
//commonPricingStageDF.registerTempTable("common_pricing_stage")

commonPricingStageDf2.write.mode("append").jdbc(url, "common_pricing_stage_test", prop)

val chargeDimDf = sqlContext.sql("""select distinct
cast(lin_consolidated_charge_code__c as string) as normalised_charge_code
,lin_consolidated_charge_name__c as normalised_charge_name
from 
antuit_pricing.chargecode_xref""")

chargeDimDf.write.jdbc(url, "charge_dim_test", prop)

val costCenterDimDf = sqlContext.sql("""select distinct
lin_workday_cost_center__c as cost_center
,lin_workday_location_id__c as workday_location
from 
antuit_pricing.costcenter_xref""")

costCenterDimDf.write.jdbc(url, "cost_center_dim_test", prop)

val customerDimDf = sqlContext.sql("""select distinct
lin_customer_enterprise_id__c AS enterprise_id
,lin_survivor_customer_name__c as normalised_client_name
from
antuit_pricing.customer_xref""")

customerDimDf.write.jdbc(url, "customer_dim_test", prop)

	}
	
}
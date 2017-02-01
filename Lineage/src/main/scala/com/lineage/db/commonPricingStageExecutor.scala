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
  
					val mrsStagingDf = sqlContext.sql("""select distinct nvl((case when a.facilityid like '0%' then cast(cast(a.facilityid as int) as string) else cast(a.facilityid as string) end),cast('' as string)) facilityid 
,nvl((case when a.fcustcode like '0%' then cast(cast(a.fcustcode as int) as string) else cast(a.fcustcode as string) end),cast('' as string)) fcustcode 
,c.flot 
,d.finvoice 
,a.fdatestamp 
,d.forigamt 
,d.frate 
,d.fqty as fqty_billed 
,d.fweight as fweight_billed 
,d.fbasis 
,d.fgl  
,d.fnumdays
,d.fbilltype
,d.fnotes
,(case when upper(d.fbilltype)='RS' then e.frs_days when upper(d.fbilltype)='IS' then e.fis_days else d.fnumdays end) as ren_pd
,d.fbilledby from antuit_stage.dl_mrs_phy_trn_delta a left join antuit_stage.dl_mrs_phy_mst_delta pm on (concat(a.fbatch
,a.fsequence) = pm.ftrack and
a.fserial = pm.fserial and
a.fcustcode = pm.fcustcode and
a.facilityid = pm.facilityid
)
left join antuit_stage.dl_mrs_inv_trn_delta b on
(a.facilityid = b.facilityid and
a.fcustcode = b.fcustcode and 
concat(a.fbatch
,a.fsequence) = concat(b.fbatch
,b.fsequence) and
a.frectype = b.frectype and a.ftrack = b.ftrack) left join antuit_stage.dl_mrs_inv_mst_delta c on
(b.facilityid = c.facilityid and
b.fcustcode = c.fcustcode and
concat(b.fbatch
,b.fsequence) = concat(c.fbatch
,c.fsequence) and
b.frectype = c.frectype and
b.fprodgroup = c.fprodgroup and
b.fproduct = c.fproduct)  
left join antuit_stage.dl_mrs_billhist_delta d on 
(c.facilityid = d.facilityid and
c.fcustcode = d.fcustcode and 
concat(c.fbatch
,c.fsequence) = concat(d.fbatch
,d.fsequence) and 
c.flot = d.flot and
c.fprodgroup = d.fprodgroup and 
c.fproduct = d.fproduct and
c.fpricecode = d.fpricecode
) left join antuit_stage.dl_mrs_price_delta e on (
c.fcustcode = e.fcustcode and 
c.facilityid = e.facilityid and
c.fpricecode = e.fpricecode
)""")




val hjPreStaging1Df = sqlContext.sql("""select 
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
antuit_stage.hj_t_bmm_charge_event_ref c
inner join antuit_stage.hj_t_bmm_charge d on (c.charge_id = d.charge_id)
inner join antuit_stage.hj_t_bmm_chargeback_rate chrate on (d.chargeback_rate_id = chrate.chargeback_rate_id)	
inner join antuit_stage.hj_t_bmm_cont_inv_type_chargeback f on (d.chargeback_id = f.chargeback_id and chrate.chargeback_id = f.chargeback_id) 
inner join antuit_stage.hj_t_bmm_invoice inv on (d.invoice_id=inv.invoice_id)
inner join antuit_stage.hj_t_bmm_contract_invoice_type k on (inv.contract_invoice_type_id = k.contract_invoice_type_id and f.contract_invoice_type_id=k.contract_invoice_type_id) 
inner join antuit_stage.hj_t_bmm_contract_master l on (k.contract_id = l.contract_id) 
inner join antuit_stage.hj_t_bmm_customer m on (l.customer_id = m.customer_id)	
inner join (select distinct chargeback_type_id,chargeback_type from antuit_stage.hj_t_bmm_chargeback_type where chargeback_type='WMS Event') g on (f.chargeback_type_id = g.chargeback_type_id)""")

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
inner join antuit_stage.hj_t_bmm_cont_inv_type_chargeback citc on (chg.chargeback_id = citc.chargeback_id)
inner join antuit_stage.hj_t_bmm_chargeback_type ct on (citc.chargeback_type_id = ct.chargeback_type_id)
inner join antuit_stage.hj_t_bmm_chargeback_rate cbrate on (citc.chargeback_id = cbrate.chargeback_id and cbrate.chargeback_rate_id=chg.chargeback_rate_id)
inner join antuit_stage.hj_t_bmm_invoice inv on (chg.invoice_id = inv.invoice_id)
inner join antuit_stage.hj_t_bmm_contract_invoice_type cit on (cit.contract_invoice_type_id = inv.contract_invoice_type_id and citc.contract_invoice_type_id=cit.contract_invoice_type_id)
inner join antuit_stage.hj_t_bmm_contract_master cm on (cit.contract_id = cm.contract_id)
inner join antuit_stage.hj_t_bmm_customer cust on (cm.customer_id = cust.customer_id)
left join antuit_stage.hj_t_bmm_param_uom_storage pus on (citc.chargeback_id = pus.chargeback_id)
left join antuit_stage.hj_t_bmm_frequency freq on (citc.chargeback_id = freq.chargeback_id)
left join antuit_stage.hj_t_bmm_param_uom_anniversary pua on (citc.chargeback_id = pua.chargeback_id)
left join antuit_stage.hj_t_bmm_param_recurring pr on (citc.chargeback_id = pr.chargeback_id)
left join antuit_stage.hj_t_bmm_param_manual_csr_prompt mcp on (citc.chargeback_id = mcp.chargeback_id)
left join antuit_stage.hj_t_bmm_param_manual_prompt mp on (citc.chargeback_id = mp.chargeback_id)
left join antuit_stage.hj_t_bmm_param_manual_adhoc ma on (citc.chargeback_id = ma.chargeback_id)
where lower(ct.chargeback_type) <>'wms event' """)

hjPreStaging2Df.registerTempTable("hj_pre_staging_2")

//val hjStagingDf2 = sqlContext.sql("select * from hj_pre_staging_2")

val hjStagingDf = hjStagingDf1.unionAll(hjPreStaging2Df)

hjStagingDf.registerTempTable("hj_staging")


val mrsCustXrefDf = sqlContext.sql("""select distinct
tab.*
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
(select distinct
stg.*
,concat('MRS',stg.facilityid, nvl(stg.fcustcode,cast('' as string))) as UID_stg
--,xref.UID
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
left join (select distinct uid,LIN_CONSOLIDATED_CHARGE_CODE__C,LIN_CONSOLIDATED_CHARGE_NAME__C from antuit_pricing.chargecode_xref) chxref
on concat('MRS',nvl((case when stg.fgl like '0%' then cast(cast(stg.fgl as int) as string) else cast(stg.fgl as string) end),cast('' as string)))=chxref.uid
left join (select distinct uid,LIN_WORKDAY_COST_CENTER__C,LIN_WORKDAY_LOCATION_ID__C from antuit_pricing.costcenter_xref) costc
on concat('MRS',nvl((case when stg.facilityid like '0%' then cast(cast(stg.facilityid as int) as string) else cast(stg.facilityid as string) end),cast('' as string)))=costc.uid
) tab
LEFT JOIN antuit_pricing.account_revised_xref ACXREF
ON (tab.LIN_ACCOUNT__C=acxref.id)""")

mrsCustXrefDf.registerTempTable("mrs_cust_xref")

val hjCustRefDf = sqlContext.sql("""select distinct
tab.*
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
select distinct
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
left join (select distinct uid,LIN_CONSOLIDATED_CHARGE_CODE__C,LIN_CONSOLIDATED_CHARGE_NAME__C from antuit_pricing.chargecode_xref) chxref
on concat('HIGHJUMP',nvl((case when stg.chargeback_code like '0%' then cast(cast(stg.chargeback_code as int) as string) else cast(stg.chargeback_code as string) end),cast('' as string)))=chxref.uid
left join (select distinct uid,LIN_WORKDAY_COST_CENTER__C,LIN_WORKDAY_LOCATION_ID__C from antuit_pricing.costcenter_xref) costc
on concat('HIGHJUMP',nvl((case when stg.wh_id like '0%' then cast(cast(stg.wh_id as int) as string) else cast(stg.wh_id as string) end),cast('' as string)))=costc.uid
) tab
LEFT JOIN antuit_pricing.account_revised_xref ACXREF
ON (tab.LIN_ACCOUNT__C=acxref.id)""")

hjCustRefDf.registerTempTable("hj_cust_xref")


val commonPricingStageDf1 = sqlContext.sql("""select distinct
lin_source_system_name__c
,lin_customer_enterprise_id__c
,lin_workday_cost_center__c
,lin_workday_location_id__c
,cast(lin_consolidated_charge_code__c as string)
,lin_consolidated_charge_name__c
,facilityid as facility_id
,nvl(fcustcode,"null") as customer_id
,flot as lot_number
,finvoice as invoice
,fdatestamp as invoice_date
,forigamt as total_invoiced_amount
,frate as rate_charged
,fqty_billed as billed_qty
,fweight_billed as billed_weight
,fbasis as basis
,fgl as charge_code
,ren_pd
,fbilledby as unit_of_measurement
,cast(null as string) as currencyisocode
,lin_survivor_customer_name__c
,billingstreet
,billingcity
,billingstate
,billingpostalcode
,billingcountry
,phone
,fax
,accountnumber
,website
from
mrs_cust_xref""")

val commonPricingStageDf2 = sqlContext.sql("""select distinct
lin_source_system_name__c
,lin_customer_enterprise_id__c
,lin_workday_cost_center__c
,lin_workday_location_id__c
,cast(lin_consolidated_charge_code__c as string)
,lin_consolidated_charge_name__c
,wh_id as facility_id
,nvl(customer_code,"null") as customer_id
,lot_number
,cast(invoice_id as string) as invoice
,generated_date as invoice_date
,charge_amount as total_invoiced_amount
,rate as rate_charged
,report_qty as billed_qty
,report_weight as billed_weight
,weight_increment as basis
,chargeback_code as charge_code
,hj_ren_pd as ren_pd
,uom as unit_of_measurement
,cast(null as string) as currencyisocode
,lin_survivor_customer_name__c
,billingstreet
,billingcity
,billingstate
,billingpostalcode
,billingcountry
,phone
,fax
,accountnumber
,website
from
hj_cust_xref""")

//val commonPricingStageDF = commonPricingStageDf1.unionAll(commonPricingStageDf2)

//println("Total count is :............................................................."+(commonPricingStageDf1.count + commonPricingStageDf2.count))

commonPricingStageDf1.write.mode("append").jdbc(url, "go2", prop)

//commonPricingStageDF.registerTempTable("common_pricing_stage")

commonPricingStageDf2.write.mode("append").jdbc(url, "go2", prop)

	}
	
}
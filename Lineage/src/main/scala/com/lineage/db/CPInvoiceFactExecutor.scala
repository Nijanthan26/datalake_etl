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
import java.util.Calendar

object CPInvoiceFactExecutor {
  	def main(args: Array[String]): Unit = {
  	  	        val conf = new SparkConf().setAppName("InvoiceFact")
					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc)
					import sqlContext.implicits._

					val url = "jdbc:postgresql://metricsone.cpslmao02wkq.us-west-2.rds.amazonaws.com:5432/postgres"
					val username = "root"
					val password = "TiMLRHdCNLftYOLskOkF"
					val prop = new java.util.Properties
					prop.setProperty("user",username)
					prop.setProperty("password",password)
					
					val invoice_factHJ = sqlContext.sql(""" insert into antuit_pricing.invoice_fact
select distinct
tab.legacy_source_system as legacy_source_system
,xref.LIN_CUSTOMER_ENTERPRISE_ID__C as enterprise_id
,xref.LIN_SURVIVOR_CUSTOMER_NAME__C as customer_name
,costc.LIN_WORKDAY_COST_CENTER__C as cost_center
,costc.LIN_WORKDAY_LOCATION_ID__C as workday_location
,xref.LIN_LEGACY_CUSTOMER_CODE__C as legacy_customer_code
,tab.wh_id as legacy_warehouse_id
,cast(tab.invoice_id as string) as invoice_id
,tab.generated_date as invoice_date
,tab.Total_Invoice_amt  as total_invoice_amt
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
inner join antuit_stage.hj_t_bmm_charge chg on inv.invoice_id = chg.invoice_id left join antuit_stage.hj_t_bmm_contract_invoice_type cit on inv.contract_invoice_type_id = cit.contract_invoice_type_id inner join antuit_stage.hj_t_bmm_contract_master cm on cit.contract_id = cm.contract_idleft join antuit_stage.hj_t_bmm_customer cust on cm.customer_id = cust.customer_id
) inv_amt
group by 
inv_amt.customer_code
,inv_amt.generated_date
,inv_amt.invoice_id
,inv_amt.wh_id) tab
left join (select distinct LIN_CUSTOMER_ENTERPRISE_ID__C,LIN_SURVIVOR_CUSTOMER_NAME__C,LIN_SOURCE_SYSTEM_NAME__C,LIN_LEGACY_CUSTOMER_CODE__C from antuit_pricing.customer_xref where LIN_SOURCE_SYSTEM_NAME__C ='HIGHJUMP') xref
on (concat(tab.legacy_source_system, nvl(tab.customer_code,cast('' as string))))=(concat(nvl(xref.LIN_SOURCE_SYSTEM_NAME__C,cast('' as string)),nvl(xref.LIN_LEGACY_CUSTOMER_CODE__C,cast('' as string))))
left join (select distinct uid,LIN_WORKDAY_COST_CENTER__C,LIN_WORKDAY_LOCATION_ID__C from antuit_pricing.costcenter_xref where legacy_lin_system__c='HIGHJUMP') costc
on concat(tab.legacy_source_system,nvl((case when tab.wh_id like '0%' then cast(cast(tab.wh_id as int) as string) else cast(tab.wh_id as string) end),cast('' as string)))=costc.uid """)
 
 
 
 		val invoice_factMRS = sqlContext.sql(""" select distinct
tab.legacy_source_system as legacy_source_system
,xref.LIN_CUSTOMER_ENTERPRISE_ID__C as enterprise_id
,xref.LIN_SURVIVOR_CUSTOMER_NAME__C as customer_name
,costc.LIN_WORKDAY_COST_CENTER__C as cost_center
,costc.LIN_WORKDAY_LOCATION_ID__C as workday_location
,xref.LIN_LEGACY_CUSTOMER_CODE__C as  legacy_customer_code
,tab.facilityid as legacy_warehouse_id
,tab.finvoice as invoice_id
,tab.fdatestamp as invoice_date
,tab.inv_amt  as total_invoice_amt
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
left join (select distinct uid,LIN_WORKDAY_COST_CENTER__C,LIN_WORKDAY_LOCATION_ID__C from antuit_pricing.costcenter_xref where legacy_lin_system__c='MRS') costc
on concat(tab.legacy_source_system,nvl((case when tab.facilityid like '0%' then cast(cast(tab.facilityid as int) as string) else cast(tab.facilityid as string) end),cast('' as string)))=costc.uid""")

invoice_factHJ.write.mode("append").jdbc(url, "invoice_fact_imp", prop)
invoice_factMRS.write.mode("append").jdbc(url, "invoice_fact_imp", prop)

  	}
}
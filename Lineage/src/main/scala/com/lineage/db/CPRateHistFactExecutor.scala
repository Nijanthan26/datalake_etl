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

object CPRateHistFactExecutor {

	def main(args: Array[String]): Unit = {
			    
	        val conf = new SparkConf().setAppName("RateHist")
					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc)
					import sqlContext.implicits._

					val url = "jdbc:postgresql://metricsone.cpslmao02wkq.us-west-2.rds.amazonaws.com:5432/postgres"
					val username = "root"
					val password = "TiMLRHdCNLftYOLskOkF"
					val prop = new java.util.Properties
					prop.setProperty("user",username)
					prop.setProperty("password",password)
					
					
					val rate_history_factMRS = sqlContext.sql(""" 
select distinct
fnl.legacy_source_system as legacy_source_system
,fnl.lin_customer_enterprise_id__c as enterprise_id
,fnl.lin_survivor_customer_name__c as normalised_client_name
,fnl.lin_workday_cost_center__c as cost_center
,fnl.lin_workday_location_id__c as workday_location
,fnl.lin_consolidated_charge_code__c	as normalised_charge_code	
,fnl.lin_consolidated_charge_name__c as normalised_charge_name
,fnl.fcustcode as legacy_customer_code
,fnl.facilityid as legacy_warehouse_id
,fnl.fgl as legacy_charge_code
,fnl.invoice_min_date  as charge_effective_date
,fnl.invoice_max_date as charge_expiry_date
,fnl.fbasis as increment
,fnl.frate as rate
,fnl.line_billed_by as uom
from
(
select 
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
,case when upper(a.fbilltype) = 'IS' then min(a.Initial_handling_Rate) else null end as fis_rate
,case when upper(a.fbilltype) = 'IS' then a.fis_type else null end as fis_type
,case when upper(a.fbilltype) = 'IS' then a.fis_basis else null end as fis_basis
,case when upper(a.fbilltype) = 'IS' then a.fis_days else null end as fis_days
,case when upper(a.fbilltype) = 'RS' then min(a.Recurring_Storage_Rate) else null end as frs_rate
,case when upper(a.fbilltype) = 'RS' then a.frs_type else null end as frs_type
,case when upper(a.fbilltype) = 'RS' then a.frs_basis else null end as frs_basis
,case when upper(a.fbilltype) = 'RS' then a.frs_days else null end as frs_days
,case when upper(a.fbilltype) = 'HI' then min(a.Handling_in_Rate) else null end as fhi_rate
,case when upper(a.fbilltype) = 'HI' then a.fhi_type else null end as fhi_type
,case when upper(a.fbilltype) = 'HO' then min(a.Handling_Out_Rate) else null end as fho_rate
,case when upper(a.fbilltype) = 'HO' then a.fho_type else null end as fho_type
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
left join antuit_stage.mrs_price prc on
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
,case when upper(a.fbilltype) = 'IS' then a.fis_type else null end
,case when upper(a.fbilltype) = 'IS' then a.fis_basis else null end
,case when upper(a.fbilltype) = 'IS' then a.fis_days else null end
,case when upper(a.fbilltype) = 'RS' then a.frs_type else null end
,case when upper(a.fbilltype) = 'RS' then a.frs_basis else null end
,case when upper(a.fbilltype) = 'RS' then a.frs_days else null end
,case when upper(a.fbilltype) = 'HI' then a.fhi_type else null end
,case when upper(a.fbilltype) = 'HO' then a.fho_type else null end
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
left join (select distinct uid,LIN_CONSOLIDATED_CHARGE_CODE__C,LIN_CONSOLIDATED_CHARGE_NAME__C from antuit_pricing.chargecode_xref where lin_legacy_system__c='MRS') chxref
on concat(a.legacy_source_system,nvl((case when a.fgl like '0%' then cast(cast(a.fgl as int) as string) else cast(a.fgl as string) end),cast('' as string)))=chxref.uid
left join (select distinct uid,LIN_WORKDAY_COST_CENTER__C,LIN_WORKDAY_LOCATION_ID__C from antuit_pricing.costcenter_xref where legacy_lin_system__c='MRS') costc
on concat(a.legacy_source_system,nvl((case when a.facilityid like '0%' then cast(cast(a.facilityid as int) as string) else cast(a.facilityid as string) end),cast('' as string)))=costc.uid
) fnl
""")


rate_history_factMRS.write.mode("append").jdbc(url, "rate_history_fact", prop)
	}
}
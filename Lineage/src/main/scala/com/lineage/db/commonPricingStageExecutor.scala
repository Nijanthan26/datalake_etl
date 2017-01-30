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
  
					val mrsStagingDf = sqlContext.sql("""select distinct nvl((case when a.facilityid like '0%' then cast(cast(a.facilityid as int) as string) else cast(a.facilityid as string) end),cast('' as string))
--a.facilityid 
,nvl((case when a.fcustcode like '0%' then cast(cast(a.fcustcode as int) as string) else cast(a.fcustcode as string) end),cast('' as string))
--,a.fcustcode 
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

mrsStagingDf.registerTempTable("mrs_staging")

val res1 = sqlContext.sql("select count(*) from mrs_staging")
println (res1)
	}
	
}
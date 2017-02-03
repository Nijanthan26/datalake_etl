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

object pricingMart1 {


	def main(args: Array[String]): Unit = {
	  
	  	    		val cal = Calendar.getInstance()
          val Date =cal.get(Calendar.DATE )
          val Month1 =cal.get(Calendar.MONTH )
          val Month = Month1+1
         /* val Hour = cal.get(Calendar.HOUR_OF_DAY)
          val min = cal.get(Calendar.MINUTE)
          val second = cal.get(Calendar.SECOND) 
          */
          val Year = cal.get(Calendar.YEAR)
          val timeStamp = Date+"-"+Month+"-"+Year
			    val conf = new SparkConf().setAppName("billHist"+timeStamp)
					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc)
					import sqlContext.implicits._

					val url = "jdbc:postgresql://metricsone.cpslmao02wkq.us-west-2.rds.amazonaws.com:5432/postgres"
					val username = "root"
					val password = "TiMLRHdCNLftYOLskOkF"
					
			//		val data = sqlContext.sql("select transaction_date,product,price,payment_type,name,city,state,country,account_created,last_login,latitude,longitude from accelos.salessample")
			//		data.registerTempTable("salessample_temp1")

					//val res1 = sqlContext.sql("select product, count(*) from salessample_temp1 group by product")

					val prop = new java.util.Properties
					prop.setProperty("user",username)
					prop.setProperty("password",password)

				//	res1.write.jdbc(url, "test3", prop)
					
val chargeDimDf = sqlContext.sql("""select distinct
cast(lin_consolidated_charge_code__c as string) as normalised_charge_code
,lin_consolidated_charge_name__c as normalised_charge_name
,concat('billhist_',CURRENT_TIMESTAMP) as updated_date
from 
antuit_pricing.chargecode_xref""")

chargeDimDf.write.jdbc(url, "temp_delete", prop)

/*val costCenterDimDf = sqlContext.sql("""select distinct
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
					

					//res1.write.mode("append").jdbc(url, "test3", prop)
*/

	}

}
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
			    val conf = new SparkConf().setAppName("sqlrun")
					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc)
					import sqlContext.implicits._

					val url = "jdbc:postgresql://metricsone.cpslmao02wkq.us-west-2.rds.amazonaws.com:5432/postgres"
					val username = "root"
					val password = "TiMLRHdCNLftYOLskOkF"
					
					val data = sqlContext.sql("select transaction_date,product,price,payment_type,name,city,state,country,account_created,last_login,latitude,longitude from accelos.salessample")
					data.registerTempTable("salessample_temp1")

					val res1 = sqlContext.sql("select product, count(*) from salessample_temp1 group by product")

					val prop = new java.util.Properties
					prop.setProperty("user",username)
					prop.setProperty("password",password)

					res1.write.jdbc(url, "test3", prop)

					

					res1.write.mode("append").jdbc(url, "test3", prop)


	}

}
package com.lineage
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
object aclclear {
    	def main(args: Array[String]): Unit = {
			
  	  
  	  val tableName = args(0);
  	  val conf = new SparkConf().setAppName("Alter global comp code "+tableName)
					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc)
					import sqlContext.implicits._
					
					
					val tabledf = sqlContext.sql("select * from  "+tableName) 
					val dfNoglcode = tabledf.drop("global_comp_code")
				
					dfNoglcode.registerTempTable("tabledfNoglcode")
					val finaldf = sqlContext.sql("select  tab.*, concat(tab.comp_code,concat('-',tab.source))  as global_comp_code from  tabledfNoglcode  tab") // Load the delta data from Impala
					finaldf.write.format("parquet").saveAsTable(tableName+"_temp")
		
	}
}
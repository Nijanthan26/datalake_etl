package com.lineage.db
 import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
object ExportPostgres{
  
  
 
  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val url = "jdbc:postgresql://metricsone.cpslmao02wkq.us-west-2.rds.amazonaws.com:5432/postgres"
    val username = "root"
    val password = "TiMLRHdCNLftYOLskOkF"
    
      val conf = new SparkConf().setAppName("FirstDump")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      val srcTablename = args(0)
      val tablename = args(1)
 
      val data = sqlContext.sql("select * from antuit_pricing."+srcTablename) // Load latest data from impala
      
      
      val prop = new java.util.Properties
      prop.setProperty("user",username)
      prop.setProperty("password",password)

      data.write.jdbc(url, tablename, prop)
      
      
    // data.write.mode(SaveMode.Append).jdbc(url, "test2", prop)

  }


}
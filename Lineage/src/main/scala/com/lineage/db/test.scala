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
import java.util.Properties

object test {
  
  
 
  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://metricsone.cpslmao02wkq.us-west-2.rds.amazonaws.com:5432/postgres"
    val username = "root"
    val password = "TiMLRHdCNLftYOLskOkF"
    val tablename = args(0)
     
      val conf = new SparkConf().setAppName("SQL_LOAD")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
 
      //val archData = sqlContext.sql("select * from archimport."+args(2)) // Load archive data
      val LatestData = sqlContext.sql("select * from antuit_pricing."+tablename) // Load latest data from impala
     //res.show()
     
    

    // there's probably a better way to do this
    var connection:Connection = null
   
    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

       val prop = new Properties()
      prop.setProperty("user", username)
      prop.setProperty("password", password)
      //prop.setProperty("driver",driver)
      LatestData.write.jdbc(url, tablename, prop)

    } catch {
      case e => e.printStackTrace
    }
    connection.close()
  }


}
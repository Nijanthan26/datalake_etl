package com.lineage

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import com.microsoft.sqlserver.jdbc.SQLServerDriver

object mrs {
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("MRS Import")
    val sc = new SparkContext(conf)
    val tablename = args (0)
    import org.apache.spark.SparkContext._
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    val tableData = sqlContext.read.format("jdbc")
    .option("url", "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0009SQLDBFacilityData09_001").
    option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
    option("dbtable", tablename).
    option("user", "readonly").
    option("password", "R3@60n1Y$").load()
    
    
  }
  
}
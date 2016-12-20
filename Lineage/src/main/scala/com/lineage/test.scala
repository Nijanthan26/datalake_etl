package com.lineage


import java.security.MessageDigest
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object test {
    def main(args: Array[String]): Unit = {
    
      val conf = new SparkConf().setAppName("FirstDump")
      val sc = new SparkContext(conf)
      val sparkSession = SparkSession.builder().appName("test").getOrCreate()
      import sparkSession.implicits._
      val data = sparkSession.createDataset(Seq((1,2, "date"), (3,4, "date2"),(3,4, "date3"),(3,4, "date5"))).toDF("col1", "col2", "archive_date")
      data.write.format("com.databricks.spark.csv").option("delimiter", "\u0001").save("/antuit/databases/testwrite1")
        
      val data2 = sparkSession.createDataset(Seq((1,2, "date"), (3,4, "date2"),(3,4, "date3"),(3,4, "date5"))).toDF("col1", "col2", "archive_date")
      data2.write.format("com.databricks.spark.csv").option("delimiter", "\u0001").save("hdfs://nameservice1/antuit/databases/testwrite2")
  }
}
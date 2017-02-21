package com.lineage

import com.lineage.RowHash
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object hqlExecuter {
  
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("sqlrun")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      val archData = sqlContext.sql(args(0)) // Load archive data
       
      }
  
}
package com.lineage

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

object FirstDump {
/*
 * To add md5 and sequence numbers to archive data and the latest data present in impala and dump the combined data in data lake.
 * This is one time run. 
 * */
	def addDeltaFirstTimeWithArc(initialDf: Dataset[Row], deltaDf: Dataset[Row]): Dataset[Row] = {
			    val sparkSession = deltaDf.sparkSession
            
             
          // val sortedCols = "archive_date" +: deltaDf.columns.filter(x => !x.equals("archive_date"))
			    val sortedinitialDf = initialDf.select("archive_date" , deltaDf.columns.filter(x => !x.equals("archive_date")):_*)
			    val sortedDelta =     deltaDf.select("archive_date" , deltaDf.columns.filter(x => !x.equals("archive_date")):_*)
          val initialDfSha = RowHash.addHash(sortedinitialDf)//.drop("archive_date"))
					val deltaDfSha = RowHash.addHash(sortedDelta)//.drop("archive_date"))
					val deduped = initialDfSha.union(deltaDfSha).rdd.map { row => (row.getString(row.length-1), row) }.reduceByKey((r1, r2) => r1).	map { case(sha2, row) => row }
					val dedupedDf = sparkSession.createDataFrame(deduped, deltaDfSha.schema) 
					dedupedDf.createOrReplaceTempView("deduped")
					import org.apache.spark.sql.functions._ 
					dedupedDf.withColumn("sequence", monotonically_increasing_id) 
    
			    
	}
	
		def addDeltaFirstTimeNoArc(deltaDf: Dataset[Row]): Dataset[Row] = {
			    val sparkSession = deltaDf.sparkSession
            
             
          // val sortedCols = "archive_date" +: deltaDf.columns.filter(x => !x.equals("archive_date"))
			    val sortedDelta =     deltaDf.select("archive_date" , deltaDf.columns.filter(x => !x.equals("archive_date")):_*)
         // val initialDfSha = RowHash.addHash(sortedDelta)//.drop("archive_date"))
					val deltaDfSha = RowHash.addHash(sortedDelta)//.drop("archive_date"))
					val deduped = deltaDfSha.union(deltaDfSha).rdd.map { row => (row.getString(row.length-1), row) }.reduceByKey((r1, r2) => r1).	map { case(sha2, row) => row }
					val dedupedDf = sparkSession.createDataFrame(deduped, deltaDfSha.schema) 
					dedupedDf.createOrReplaceTempView("deduped")
					import org.apache.spark.sql.functions._ 
					dedupedDf.withColumn("sequence", monotonically_increasing_id) 
    
			    
	}
		
  
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("FirstDump")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      if(args(2).equals("_"))
      {
      //val archData = sqlContext.sql("select * from archimport."+args(2)) // Load archive data
      val LatestData = sqlContext.sql("select * from sqoopdailydelta."+args(1)) // Load latest data from impala
      val res = addDeltaFirstTimeNoArc(LatestData)
      //res.show()
      res.registerTempTable("mytempTable")
      sqlContext.sql("drop table if exists antuit_stage."+args(0))
      sqlContext.sql("create table antuit_stage."+args(0)+" as select * from mytempTable");
          }
      else
      {
      val archData = sqlContext.sql("select * from archimport."+args(2)) // Load archive data
      val LatestData = sqlContext.sql("select * from sqoopdailydelta."+args(1)) // Load latest data from impala
      val res = addDeltaFirstTimeWithArc(archData, LatestData)
      //res.show()
      res.registerTempTable("mytempTable")
      sqlContext.sql("drop table if exists antuit_stage."+args(0))
      sqlContext.sql("create table antuit_stage."+args(0)+" as select * from mytempTable");
      }

  
  
      }
  
}
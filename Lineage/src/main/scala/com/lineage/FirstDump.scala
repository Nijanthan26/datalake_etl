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
	def addDeltaFirstTime(initialDf: Dataset[Row], deltaDf: Dataset[Row]): Dataset[Row] = {
			    val sparkSession = deltaDf.sparkSession
       if(!(deltaDf.columns.contains("archive_date")))
         {
         
         
        
        val delta =  deltaDf.withColumn("archive_date",lit(0))
        // delta.show()
			   // val sortedCols = "archive_date" +: deltaDf.columns.filter(x => !x.equals("archive_date"))
			    val sortedinitialDf = initialDf.select("archive_date" , delta.columns.filter(x => !x.equals("archive_date")):_*)
			    val sortedDelta =     delta.select("archive_date" , delta.columns.filter(x => !x.equals("archive_date")):_*)
			    
          val initialDfSha = RowHash.addHash(sortedinitialDf)//.drop("archive_date"))
					val deltaDfSha = RowHash.addHash(sortedDelta)//.drop("archive_date"))
					////initialDfSha.show()
					//deltaDfSha.show()
					val deduped = initialDfSha.union(deltaDfSha).rdd.map { row => (row.getString(row.length-1), row) }.reduceByKey((r1, r2) => r1).	map { case(sha2, row) => row }
					
					val dedupedDf = sparkSession.createDataFrame(deduped, deltaDfSha.schema) 
					dedupedDf.show()
					dedupedDf.createOrReplaceTempView("deduped")
					import org.apache.spark.sql.functions._ 
					dedupedDf.withColumn("sequence", monotonically_increasing_id) 
         }
       else
       {
         val delta =  deltaDf
         delta.show()
			   // val sortedCols = "archive_date" +: deltaDf.columns.filter(x => !x.equals("archive_date"))
			    val sortedinitialDf = initialDf.select("archive_date" , delta.columns.filter(x => !x.equals("archive_date")):_*)
			    val sortedDelta =     delta.select("archive_date" , delta.columns.filter(x => !x.equals("archive_date")):_*)
          val initialDfSha = RowHash.addHash(sortedinitialDf)//.drop("archive_date"))
					val deltaDfSha = RowHash.addHash(sortedDelta)//.drop("archive_date"))
					val deduped = initialDfSha.union(deltaDfSha).rdd.map { row => (row.getString(row.length-1), row) }.reduceByKey((r1, r2) => r1).	map { case(sha2, row) => row }
					val dedupedDf = sparkSession.createDataFrame(deduped, deltaDfSha.schema) 
					dedupedDf.createOrReplaceTempView("deduped")
					import org.apache.spark.sql.functions._ 
					dedupedDf.withColumn("sequence", monotonically_increasing_id) 
       }
			    
	}
		
  
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("FirstDump")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      val archData = sqlContext.sql("select * from staging."+args(0)) // Load archive data
      val LatestData = sqlContext.sql("select * from sqoopdailydelta."+args(1)) // Load latest data from impala
      val res = addDeltaFirstTime(archData, LatestData)
      //res.show()
      res.registerTempTable("mytempTable")
      sqlContext.sql("drop table if exists antuit_stage."+args(2))
      sqlContext.sql("create table antuit_stage."+args(2)+" as select * from mytempTable");
  
  
      }
  
}
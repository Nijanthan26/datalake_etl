package com.lineage

//import com.lineage.RowHash
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.Row
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext

object DeltaAddPrqt {

	def addDeltaIncremental(initialDfShaWithDate: DataFrame, deltaDf: DataFrame,sqlContext:SQLContext): DataFrame = {
	  
			val initialDfSha = initialDfShaWithDate//.drop("archive_date")
					//val sparkSession = deltaDf.sparkSession
					//val  delta = deltaDf
					val commonColList = deltaDf.columns.filter(x => !x.equals("archive_date")) 
					val sortedDelta = deltaDf.select("archive_date" , commonColList:_*)
					val deltaDfSha = RowHash.addHash(sortedDelta,sqlContext)
					initialDfShaWithDate.registerTempTable("initialDfSha")
					val currentRowNum = sqlContext.sql("select max(sequence) from initialDfSha").collect()(0).getLong(0)
					deltaDfSha.registerTempTable("deltaDfSha")
					import org.apache.spark.sql.functions._ 
					val deltaDfShaSeq = deltaDfSha.withColumn("sequence", monotonically_increasing_id + currentRowNum)
					val deduped = initialDfSha.unionAll(deltaDfShaSeq).rdd.map { row => (row.getString(row.length-2), row) }.reduceByKey((r1, r2) => r1).map { case(sha2, row) => row }
					sqlContext.createDataFrame(deduped, deltaDfShaSeq.schema)

	}


	def main(args: Array[String]): Unit = {
			

					val antuitStageTablename = args(0)
					val deltaTable = args(1)				
					val table = deltaTable.substring(deltaTable.indexOf(".")+1)
					val db = deltaTable.substring(0,deltaTable.indexOf("."))
					
					val conf = new SparkConf().setAppName("DeltaAdd"+table)
					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc)
				 // val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc) 
			
				  import sqlContext.implicits._
					sqlContext.sql("ALTER TABLE "+antuitStageTablename+" RENAME TO "+antuitStageTablename+"_temp")
					
					val dfProc = hiveContext.sql("select * from "+antuitStageTablename+"_temp") //load the Previously Processes table  from Data Lake
					val dfDelta = hiveContext.sql("select * from "+deltaTable) // Load the delta data from Impala
//				/  val dfProc = sqlContext.sql("select * from hj_t_item_master_temp")
					// dfProc.write.format("parquet").saveAsTable("hj_t_item_master")
					
					if(dfDelta.count >0)
					{
						val res = addDeltaIncremental(dfProc, dfDelta,sqlContext )
						res.write.format("parquet").saveAsTable(antuitStageTablename)
						hiveContext.sql("drop table "+antuitStageTablename+"_temp")

					}
					else{
						System.exit(0)
					}
			//sc.close()
	}
}
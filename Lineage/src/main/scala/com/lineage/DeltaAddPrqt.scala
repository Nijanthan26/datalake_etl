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
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
object DeltaAddPrqt {

	def addDeltaIncremental(initialDfShaWithDate: Dataset[Row], deltaDf: Dataset[Row]): Dataset[Row] = {
			val initialDfSha = initialDfShaWithDate//.drop("archive_date")
					val sparkSession = deltaDf.sparkSession
					val  delta = deltaDf
					val commonColList = delta.columns.filter(x => !x.equals("archive_date")) 
					val sortedDelta = delta.select("archive_date" , commonColList:_*)
					val deltaDfSha = RowHash.addHash(sortedDelta)
					initialDfShaWithDate.createOrReplaceTempView("initialDfSha")
					val currentRowNum = sparkSession.sql("select max(sequence) from initialDfSha").collect()(0).getLong(0)
					deltaDfSha.createOrReplaceTempView("deltaDfSha")
					import org.apache.spark.sql.functions._ 
					val deltaDfShaSeq = deltaDfSha.withColumn("sequence", monotonically_increasing_id + currentRowNum)
					val deduped = initialDfSha.union(deltaDfShaSeq).rdd.map { row => (row.getString(row.length-2), row) }.reduceByKey((r1, r2) => r1).map { case(sha2, row) => row }
					sparkSession.createDataFrame(deduped, deltaDfShaSeq.schema)

	}


	def main(args: Array[String]): Unit = {
			

					val antuitStageTablename = args(0)
					val deltaTable = args(1)				
					val table = deltaTable.substring(deltaTable.indexOf(".")+1)
					val db = deltaTable.substring(0,deltaTable.indexOf("."))
					
					val conf = new SparkConf().setAppName("DeltaAdd"+table)
					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc)
			
				  import sqlContext.implicits._
					sqlContext.sql("ALTER TABLE "+antuitStageTablename+" RENAME TO "+antuitStageTablename+"_temp")
					
					val dfProc = sqlContext.sql("select * from "+antuitStageTablename+"_temp") //load the Previously Processes table  from Data Lake
					val dfDelta = sqlContext.sql("select * from "+deltaTable) // Load the delta data from Impala
					if(dfDelta.count >0)
					{
						val res = addDeltaIncremental(dfProc, dfDelta )
						res.write.format("parquet").saveAsTable(antuitStageTablename)


					}
					else{
						System.exit(0)
					}
			//sc.close()
	}
}
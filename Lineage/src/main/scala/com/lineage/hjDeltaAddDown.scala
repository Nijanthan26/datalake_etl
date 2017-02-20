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
object hjDeltaAddDown {

	def addDeltaIncremental(initialDfShaWithDate:Dataset[Row], deltaDf: Dataset[Row]):Dataset[Row] = {
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
			val conf = new SparkConf().setAppName("DeltaAdd")
					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc)
					import sqlContext.implicits._
				//	sqlContext.sql("insert into antuit_stage.dl_t_sequencetrack select CURRENT_TIMESTAMP,\'"+ args(0) +"\',max(sequence) from "+ args(0)) 

					val dfProc = sqlContext.sql("select * from "+args(0)) //load the Previously Processes table  from Data Lake
					val dfDelta = sqlContext.sql("select * from "+args(1)) // Load the delta data from Impala
					if(dfDelta.count >0)
					{
						val res = addDeltaIncremental(dfProc, dfDelta )
								val cal = Calendar.getInstance()
								val Date =cal.get(Calendar.DATE )
								val Month1 =cal.get(Calendar.MONTH )
								val Month = Month1+1
								val Hour = cal.get(Calendar.HOUR_OF_DAY)
								val min = cal.get(Calendar.MINUTE)
								val second = cal.get(Calendar.SECOND)

								res.write.format("com.databricks.spark.csv").option("delimiter", "\u0001").save("/antuit/databases/antuit_stage/"+args(0)+"_"+Date+"_"+Month+"_"+Hour+"_"+min+"_"+second)

								sqlContext.sql("create table "+args(0)+"_merge like "+ args(0))
								sqlContext.sql("drop table if exists "+args(0))
								sqlContext.sql("create table "+args(0)+" like "+ args(0)+"_merge")
								sqlContext.sql("ALTER TABLE "+ args(0) +" set location \'/antuit/databases/antuit_stage/"+args(0)+"_"+Date+"_"+Month+"_"+Hour+"_"+min+"_"+second+"\'")
								sqlContext.sql("drop table if exists "+args(0)+"_merge")

					}
					else{
						System.exit(0)
					}
			//sc.close()
	}
}
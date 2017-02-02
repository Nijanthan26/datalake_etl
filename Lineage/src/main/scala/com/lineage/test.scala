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

object test {


	def addDeltaFirstTimeAcl(cciDf: Dataset[Row], txDf: Dataset[Row]): Dataset[Row] = {
			val sparkSession = cciDf.sparkSession

					val cciDfSha = RowHash.addHash(cciDf)
					val txDfSha = RowHash.addHash(txDf)

					val deduped = cciDfSha.union(txDfSha).rdd.map { row => (row.getString(row.length-1), row) }.reduceByKey((r1, r2) => r1).map { case(sha2, row) => row }
					val dedupedDf = sparkSession.createDataFrame(deduped, cciDfSha.schema) 

							dedupedDf.createOrReplaceTempView("deduped")

							import org.apache.spark.sql.functions._ 
							dedupedDf.withColumn("sequence", monotonically_increasing_id) 


	}


	def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("DeltaAdd")
					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc) 
					import sqlContext.implicits._
					val antuitStageTablename = args(0)
					val deltaTable = args(1)
				//	sqlContext.sql("insert into antuit_stage.dl_t_sequencetrack select CURRENT_TIMESTAMP,\'"+ args(0) +"\',max(sequence) from "+ args(0))  

									val table = deltaTable.substring(deltaTable.indexOf(".")-1)
									val db = deltaTable.substring(0,deltaTable.indexOf("."))

				

						if(table.startsWith("acl_")){



									val deltaTableCci = "acl_cci_"+table.substring(5)
									val deltaTableTx = "acl_tx_"+table.substring(5)

									val dfDeltacci = sqlContext.sql("select  tab.*, 'CCI' as source , concat(tab.comp_code,concat('_','CCI'))  as global_compcode from  "+db+"."+deltaTableCci+" tab") //load the Previously Processes table  from Data Lake
									val dfDeltatx = sqlContext.sql("select  tab.*, 'CCI' as source , concat(tab.comp_code,concat('_','CCI'))  as global_compcode from  "+db+"."+deltaTableTx+" tab") // Load the delta data from Impala


									val res = addDeltaFirstTimeAcl(dfDeltacci,dfDeltatx)
									res.registerTempTable("mytempTable")


									sqlContext.sql("drop table if exists antuit_stage."+args(0))
									sqlContext.sql("create table antuit_stage."+args(0)+" as select * from mytempTable");

						}
						else
						{

						  println("exitting.......................... " + table)

						}
			
			//sc.close()
	}
}
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

object FirstDump {

	def addDeltaFirstTime(deltaDf: Dataset[Row]): Dataset[Row] = {
			val sparkSession = deltaDf.sparkSession
					val deltaDfSha = RowHash.addHash(deltaDf)
					val deduped = deltaDfSha.union(deltaDfSha).rdd.map { row => (row.getString(row.length-1), row) }.reduceByKey((r1, r2) => r1).	map { case(sha2, row) => row }
					val dedupedDf = sparkSession.createDataFrame(deduped, deltaDfSha.schema) 
							dedupedDf.createOrReplaceTempView("deduped")
							import org.apache.spark.sql.functions._ 
							dedupedDf.withColumn("sequence", monotonically_increasing_id) 


	}



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
			val conf = new SparkConf().setAppName("FirstDump")
					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc)
					import sqlContext.implicits._

					val antuitStageTablename = args(0)
					val deltaTable = args(1)
					
					  

					val table = deltaTable.substring(deltaTable.indexOf(".")+1)
					val db = deltaTable.substring(0,deltaTable.indexOf("."))



					if(table.startsWith("acl_")){

					  println("........................###################################################...............................")
						val deltaTableCci = "acl_cci_"+table.substring(4)
						val deltaTableTx = "acl_tx_"+table.substring(4)

						val dfDeltacci = sqlContext.sql("select  tab.*, 'CCI' as source , concat(tab.comp_code,concat('_','CCI'))  as global_compcode from  "+db+"."+deltaTableCci+" tab") //load the Previously Processes table  from Data Lake
						val dfDeltatx = sqlContext.sql("select  tab.*, 'TX' as source , concat(tab.comp_code,concat('_','TX'))  as global_compcode from  "+db+"."+deltaTableTx+" tab") // Load the delta data from Impala


						val res = addDeltaFirstTimeAcl(dfDeltacci,dfDeltatx)
						res.registerTempTable("mytempTable")


						sqlContext.sql("drop table if exists "+antuitStageTablename)
						sqlContext.sql("create table "+antuitStageTablename+" as select * from mytempTable");

					}
					else
					{

						//val archData = sqlContext.sql("select * from archimport."+args(2)) // Load archive data
						  val LatestData = sqlContext.sql("select * from  "+deltaTable) // Load latest data from impala
							val res = addDeltaFirstTime(LatestData)

							res.registerTempTable("mytempTable")
							sqlContext.sql("drop table if exists "+antuitStageTablename)
							sqlContext.sql("create table "+antuitStageTablename+" as select * from mytempTable");
						

					}

/usr/bin/spark2-submit --class com.lineage.FirstDump --master yarn  /home/vinuta/DBExportBranch/datalake_etl/Lineage/target/scala-2.11/lineage-datalake-spark-scala_2.11-1.0.jar antuit_stage.acl_m_info_flow_prof_d aclsqoopdailydelta.acl_m_info_flow_prof_dsqlContext.sql("insert into antuit_stage.dl_t_sequencetrack select CURRENT_TIMESTAMP,\'"+ antuitStageTablename +"\',max(sequence) from "+ antuitStageTablename)
			// sqlContext.sql("insert into antuit_stage.dl_t_sequencetrack select CURRENT_TIMESTAMP,\'"+ args(0) +"\',max(sequence) from antuit_stage."+ args(0)) 
	}
}
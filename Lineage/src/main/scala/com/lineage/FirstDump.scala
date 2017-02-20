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

					println(".........................................................................................."+cciDfSha.count)
					println(".........................................................................................."+txDfSha.count)

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


						val deltaTableCci = "acl_cci_"+table.substring(4)
						val deltaTableTx = "acl_tx_"+table.substring(4)

						val dfDeltatxT = sqlContext.sql("select * from  "+db+"."+deltaTableTx +"  limit 1")
						val deltaTableCciT = sqlContext.sql("select * from  "+db+"."+deltaTableCci +"  limit 1") // Load the delta data from Impala

						val dfDeltacciCol = deltaTableCciT.columns
						val dfDeltatxCol = dfDeltatxT.columns

						var cciSelectQuery = ""
						
						if(table.equals("acl_m_zip")){
						 cciSelectQuery = "select  tab.*, 'CCI' as source from  "+db+"."+deltaTableCci+" tab"
						}
						else{
						 cciSelectQuery = "select  tab.*, 'CCI' as source , concat(tab.comp_code,concat('-','CCI'))  as global_comp_code from  "+db+"."+deltaTableCci+" tab"
						}
						
						val dfDeltacci = sqlContext.sql(cciSelectQuery) //load the Previously Processes table  from Data Lake

						if(dfDeltacciCol.sameElements(dfDeltatxCol))
						{
						var txSelectQuery = ""
						
						if(table.equals("acl_m_zip")){
						 txSelectQuery = "select  tab.*, 'TX' as source  from  "+db+"."+deltaTableTx+" tab"
						}
						else{
						 txSelectQuery = "select  tab.*, 'TX' as source , concat(tab.comp_code,concat('-','TX'))  as global_comp_code from  "+db+"."+deltaTableTx+" tab"
						}
						  
							val dfDeltatx = sqlContext.sql(txSelectQuery) // Load the delta data from Impala
							val res = addDeltaFirstTimeAcl(dfDeltacci,dfDeltatx)
							res.registerTempTable("mytempTable")
						}else
						{
							var  selectQuerytx= "select "
									for(i <- 0 until (dfDeltacciCol.length)){
										if(dfDeltatxCol contains dfDeltacciCol(i))
										{
											selectQuerytx = selectQuerytx +" "+dfDeltacciCol(i)+","
										}else{
											selectQuerytx = selectQuerytx +"  null as "+dfDeltacciCol(i)+","
										}

									}
							
							
							if(table.equals("acl_m_zip")){
							    selectQuerytx = selectQuerytx + " \'TX\' as source   from  "+db+"."+deltaTableTx+" tab"
							}
							else{
						       selectQuerytx = selectQuerytx + " \'TX\' as source , concat(tab.comp_code,concat(\'-\',\'TX\'))  as global_comp_code from  "+db+"."+deltaTableTx+" tab"
							}
							    
							    val dfDeltatx = sqlContext.sql(selectQuerytx)
									val res = addDeltaFirstTimeAcl(dfDeltacci,dfDeltatx)
									//dfDeltatx.show()
									res.registerTempTable("mytempTable")
						}

						sqlContext.sql("drop table if exists "+antuitStageTablename)
						sqlContext.sql("create table "+antuitStageTablename+" as select * from mytempTable");

					}
					else
					{

					
					 	val LatestData = sqlContext.sql("select * from  "+deltaTable) // Load latest data from impala
								val res = addDeltaFirstTime(LatestData)

								res.registerTempTable("mytempTable")




								sqlContext.sql("drop table if exists "+antuitStageTablename)
								sqlContext.sql("create table "+antuitStageTablename+" as select * from mytempTable");


					}

			sqlContext.sql("insert into antuit_stage.dl_t_sequencetrack select CURRENT_TIMESTAMP,\'"+ antuitStageTablename +"\',max(sequence) from "+ antuitStageTablename)
			// sqlContext.sql("insert into antuit_stage.dl_t_sequencetrack select CURRENT_TIMESTAMP,\'"+ args(0) +"\',max(sequence) from antuit_stage."+ args(0)) 
	}
}


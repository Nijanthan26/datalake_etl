package com.lineage

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





object DeltaAdd {

	def addDeltaIncremental(initialDfShaWithDate: Dataset[Row], deltaDf: Dataset[Row]): Dataset[Row] = {
			val initialDfSha = initialDfShaWithDate//.drop("archive_date")
					val sparkSession = deltaDf.sparkSession

					val  delta = deltaDf
					val deltaDfSha = RowHash.addHash(delta)
					initialDfShaWithDate.createOrReplaceTempView("initialDfSha")
					val currentRowNum = sparkSession.sql("select max(sequence) from initialDfSha").collect()(0).getLong(0)
					deltaDfSha.createOrReplaceTempView("deltaDfSha")
					import org.apache.spark.sql.functions._ 
					val deltaDfShaSeq = deltaDfSha.withColumn("sequence", monotonically_increasing_id + currentRowNum)
					val deduped = initialDfShaWithDate.union(deltaDfShaSeq).rdd.map { row => (row.getString(row.length-2), row) }.reduceByKey((r1, r2) => r1).map { case(sha2, row) => row }
					sparkSession.createDataFrame(deduped, deltaDfShaSeq.schema)

	}


	def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("DeltaAdd")
					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc)



					val antuitStageTablename = args(0)
					val deltaTable = args(1)
					import sqlContext.implicits._
					sqlContext.sql("insert into antuit_stage.dl_t_sequencetrack select CURRENT_TIMESTAMP,\'"+ antuitStageTablename +"\',max(sequence) from "+ antuitStageTablename)  

					val table = deltaTable.substring(deltaTable.indexOf(".")+1)
					val db = deltaTable.substring(0,deltaTable.indexOf("."))

					val cal = Calendar.getInstance()
					val Date =cal.get(Calendar.DATE )
					val Month1 =cal.get(Calendar.MONTH )
					val Month = Month1+1
					val Hour = cal.get(Calendar.HOUR_OF_DAY)
					val min = cal.get(Calendar.MINUTE)
					val second = cal.get(Calendar.SECOND)

					val dfProc = sqlContext.sql("select * from "+antuitStageTablename)

					if(table.startsWith("acl_")){


						println("..................................................................................................................."+table+db)

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

						
						import org.apache.spark.sql.types._
						val schema = StructType(dfDeltacci.schema.fields)
						var dfDeltatx = sqlContext.createDataFrame(sc.emptyRDD[Row],schema )

						if(dfDeltacciCol.sameElements(dfDeltatxCol))
						{
						  if(table.equals("acl_m_zip")){
						  dfDeltatx = sqlContext.sql("select  tab.*, 'TX' as source from  "+db+"."+deltaTableTx+" tab") // Load the delta data from Impala
						  }else{						  
							dfDeltatx = sqlContext.sql("select  tab.*, 'TX' as source , concat(tab.comp_code,concat('-','TX'))  as global_comp_code from  "+db+"."+deltaTableTx+" tab") // Load the delta data from Impala
						  }
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
						  selectQuerytx = selectQuerytx + " \'TX\' as source  from  "+db+"."+deltaTableTx+" tab"
						}
						else{
							selectQuerytx = selectQuerytx + " \'TX\' as source , concat(tab.comp_code,concat(\'-\',\'TX\'))  as global_comp_code from  "+db+"."+deltaTableTx+" tab"
						}
						dfDeltatx = sqlContext.sql(selectQuerytx)
						}


						val dfDelta = dfDeltacci.unionAll(dfDeltatx)
								if(dfDelta.count >0)
								{
									val res = addDeltaIncremental(dfProc, dfDelta )

											res.write.format("com.databricks.spark.csv").option("delimiter", "\u0001").save("/antuit/databases/antuit_stage/"+table+"_"+Date+"_"+Month+"_"+Hour+"_"+min+"_"+second)
											sqlContext.sql("create table "+antuitStageTablename+"_merge like "+ antuitStageTablename)
											sqlContext.sql("drop table if exists "+antuitStageTablename)
											sqlContext.sql("create table "+antuitStageTablename+" like "+ antuitStageTablename+"_merge")
											sqlContext.sql("ALTER TABLE "+antuitStageTablename +" set location \'/antuit/databases/antuit_stage/"+table+"_"+Date+"_"+Month+"_"+Hour+"_"+min+"_"+second+"\'")
											sqlContext.sql("drop table if exists  "+antuitStageTablename+"_merge")
								}
								else{
									System.exit(0)
								}


					}
					else
					{
						//load the Previously Processes table  from Data Lake
						val dfDelta = sqlContext.sql("select * from  "+deltaTable) // Load the delta data from Impala

								if(dfDelta.count >0)
								{
									val res = addDeltaIncremental(dfProc, dfDelta )

											res.write.format("com.databricks.spark.csv").option("delimiter", "\u0001").save("/antuit/databases/antuit_stage/"+table+"_"+Date+"_"+Month+"_"+Hour+"_"+min+"_"+second)
											sqlContext.sql("create table "+antuitStageTablename+"_merge like "+ antuitStageTablename)
											sqlContext.sql("drop table if exists "+antuitStageTablename)
											sqlContext.sql("create table "+antuitStageTablename+" like "+ antuitStageTablename+"_merge")
											sqlContext.sql("ALTER TABLE "+antuitStageTablename +" set location \'/antuit/databases/antuit_stage/"+table+"_"+Date+"_"+Month+"_"+Hour+"_"+min+"_"+second+"\'")
											sqlContext.sql("drop table if exists  "+antuitStageTablename+"_merge")
								}
								else{
									System.exit(0)
								}
						//sc.close()
					}  
	}
}


package com.lineage


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.apache.spark.storage.StorageLevel._

object mrsInc {
  
  	def addDeltaFirstTime(deltaDf: Dataset[Row]): Dataset[Row] = {
			val sparkSession = deltaDf.sparkSession
					val deltaDfSha = RowHash.addHash(deltaDf)
					val deduped = deltaDfSha.union(deltaDfSha).rdd.map { row => (row.getString(row.length-1), row) }.reduceByKey((r1, r2) => r1).	map { case(sha2, row) => row }
					val dedupedDf = sparkSession.createDataFrame(deduped, deltaDfSha.schema) 
							dedupedDf.createOrReplaceTempView("deduped")
							import org.apache.spark.sql.functions._ 
							dedupedDf.withColumn("sequence", monotonically_increasing_id) 


	}
  
  def main(args: Array[String])
  {
    
		val conf = new SparkConf().setAppName("MRS")
		val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val table = args(0)
   // val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
   // val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    
    
    val mrsSource09 = sqlContext.load("jdbc", 
  Map(
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "url" -> "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0009SQLDBFacilityData09_001",
  "user" -> "readonly",
  "password" -> "R3@60n1Y$",
  "dbtable" -> table))
  
      val mrsSource61 = sqlContext.load("jdbc", 
  Map(
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "url" -> "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0002SQLDBFacilityData61_001",
  "user" -> "readonly",
  "password" -> "R3@60n1Y$",
  "dbtable" -> table))
  
   /*   val mrsSourceMain = sqlContext.load("jdbc", 
  Map(
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "url" -> "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0266SQLDBFacilityDataMain_001",
  "user" -> "readonly",
  "password" -> "R3@60n1Y$",
  "dbtable" -> table))
  */
   val mrsDf1 = mrsSource09.unionAll(mrsSource61)
   
  // val mrsDf2 = mrsDf1.unionAll(mrsSourceMain)
       
   
		val res = addDeltaFirstTime(mrsDf1)
		
		
		res.write.format("orc").saveAsTable("default.mrs_test_source");  //Change  schema and table name
    //sourceTable.write().saveAsTable("default.mrs_test_source");  //First time Import, Change  schema and table name
   /* 
    //sourceTable.show()
    
    val OldDF = hiveContext.sql("SELECT * FROM default.mrs_test")
    
    val upsertsDF = sourceTable.except(OldDF) //Get updated and inserted records from source
    
    upsertsDF.registerTempTable("mrsUpdate")
    
   //upsertsDF.write.saveAsTable("default.upserts_tbl")
    
    val deletesDF = OldDF.except(sourceTable) 
    
    deletesDF.registerTempTable("deletesDF")
    
   // deletesDF.write.saveAsTable("default.deletes_tbl") //Get Deleted records from source
    
    
    
    sqlContext.sql("INSERT INTO TABLE default.mrs_test SELECT * FROM mrsUpdate")
    */
  }
  
  
  
}
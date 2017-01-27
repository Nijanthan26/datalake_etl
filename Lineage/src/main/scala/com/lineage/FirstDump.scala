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
  
  def addDeltaFirstTimeNoArc(deltaDf: Dataset[Row]): Dataset[Row] = {
			    val sparkSession = deltaDf.sparkSession
					val deltaDfSha = RowHash.addHash(deltaDf)
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
 
      //val archData = sqlContext.sql("select * from archimport."+args(2)) // Load archive data
      val LatestData = sqlContext.sql("select * from  "+args(1)) // Load latest data from impala
      val res = addDeltaFirstTimeNoArc(LatestData)
      //res.show()
      res.registerTempTable("mytempTable")
      sqlContext.sql("drop table if exists antuit_stage."+args(0))
      sqlContext.sql("create table antuit_stage."+args(0)+" as select * from mytempTable");
          
      sqlContext.sql("insert into antuit_stage.dl_t_sequencetrack select CURRENT_TIMESTAMP,"+ args(0) +",max(sequence) from antuit_stage."+ args(0)) 
      }
}
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
object DeltaAddTemp {
  
    def addDeltaIncremental(initialDfShaWithDate: Dataset[Row], deltaDfWithDate: Dataset[Row]): Dataset[Row] = {
        val initialDfSha = initialDfShaWithDate//.drop("archive_date") still in discussion
        val deltaDf = deltaDfWithDate//.drop("archive_date");
        val sparkSession = deltaDf.sparkSession
        val deltaDfSha = RowHash.addHash(deltaDf)

        initialDfSha.createOrReplaceTempView("initialDfSha")
        val currentRowNum = sparkSession.sql("select max(sequence) from initialDfSha").collect()(0).getLong(0)
        deltaDfSha.createOrReplaceTempView("deltaDfSha")
        import org.apache.spark.sql.functions._
        val deltaDfShaSeq = deltaDfSha.withColumn("sequence", monotonically_increasing_id + currentRowNum)
        //deltaDfShaSeq.show()
        val deduped = initialDfSha.union(deltaDfShaSeq).rdd.map { row => (row.getString(row.length-2), row) }.reduceByKey((r1, r2) => r1).map { case(sha2, row) => row }
        sparkSession.createDataFrame(deduped, deltaDfShaSeq.schema)
        }


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("DeltaAdd")
        val sc = new SparkContext(conf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._
        val dfProc = sqlContext.sql("select * from antuit_stage."+args(0)) //load the Previously Processes table  from Data Lake
        val dfDelta = sqlContext.sql("select * from antuit_stage."+args(1)) // Load the delta data from Impala
        val res = addDeltaIncremental(dfProc, dfDelta )
        //res.write.format("parquet")
        // res.show()
        res.write.format("com.databricks.spark.csv").option("header", "false").save("/antuit/databases/antuit_stage/testTable/12_13")
    
      }
}
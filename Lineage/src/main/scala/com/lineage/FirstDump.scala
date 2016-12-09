package com.lineage

import com.lineage.md5Hash.addHash
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
import org.apache.spark.SparkContext

object FirstDump {
  
  def addDeltaFirstTime(initialDf: Dataset[Row], deltaDf: Dataset[Row]): Dataset[Row] = {
                            val sparkSession = deltaDf.sparkSession
                                        val initialDfSha = addHash(initialDf.drop("seq"))
                                        val deltaDfSha = addHash(deltaDf)

                                        val deduped = initialDfSha.union(deltaDfSha).rdd.map { row => (row.getString(row.length-1), row) }.
                                        reduceByKey((r1, r2) => r1).
                                        map { case(sha2, row) => row }

                                        val dedupedDf = sparkSession.createDataFrame(deduped, deltaDfSha.schema)
                                        dedupedDf.createOrReplaceTempView("deduped")
                                        import org.apache.spark.sql.functions._
                                        dedupedDf.withColumn("sequence", monotonically_increasing_id)
        }
		
def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("test").setMaster("local")
        val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val df1 = sqlContext.sql("select * from antuit_stage."+args(0))
    val df2 = sqlContext.sql("select * from antuit_stage."+args(1))
    val res = addDeltaFirstTime(df1, df2)
    res.show()
    res.registerTempTable("mytempTable")
    sqlContext.sql("drop table if exists antuit_stage.t_order_p")
    sqlContext.sql("create table antuit_stage.t_order_p as select * from mytempTable");


    }
  
}
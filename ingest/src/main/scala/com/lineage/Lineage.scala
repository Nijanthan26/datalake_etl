package com.lineage

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe


object Lineage {

	def addHash(deltaDf: Dataset[Row]): Dataset[Row] = {
			    val sparkSession = deltaDf.sparkSession
					sparkSession.udf.register("sha2m", (r: Row) => {
						val sha2Hasher = MessageDigest.getInstance("SHA-256")
								val buf = new StringBuilder
								val bytes = r.mkString("\u0001").getBytes
								val hexString: StringBuffer = new StringBuffer
								sha2Hasher.digest(bytes).foreach { b => 
								hexString.append(Integer.toHexString(0xFF & b))
								}
								hexString.toString()
					})

					deltaDf.createOrReplaceTempView("delta_table")

					sparkSession.sql("select *, sha2m(struct(*)) as sha2 from delta_table")
	}

	/**
	 * Merge delta table with the initial table for the first time
	 * 
	 * @param initialDf the initial table without the sha2 and sequence columns
	 * @param deltaDf the delta table without the sha2 and sequence columns
	 */
	def addDeltaFirstTime(initialDf: Dataset[Row], deltaDf: Dataset[Row]): Dataset[Row] = {
			    val sparkSession = deltaDf.sparkSession
					val initialDfSha = addHash(initialDf.drop("archive_date"))
					val deltaDfSha = addHash(deltaDf)

					val deduped = initialDfSha.union(deltaDfSha).rdd.map { row => (row.getString(row.length-1), row) }.
					reduceByKey((r1, r2) => r1).
					map { case(sha2, row) => row }

					val dedupedDf = sparkSession.createDataFrame(deduped, deltaDfSha.schema) 
					dedupedDf.createOrReplaceTempView("deduped")
					import org.apache.spark.sql.functions._ 
					dedupedDf.withColumn("sequence", monotonically_increasing_id)
	}

	def addDeltaIncremental(initialDfShaWithDate: Dataset[Row], deltaDf: Dataset[Row]): Dataset[Row] = {
			        val initialDfSha = initialDfShaWithDate.drop("archive_date")
					val sparkSession = deltaDf.sparkSession
					val deltaDfSha = addHash(deltaDf)

					initialDfSha.createOrReplaceTempView("initialDfSha")
					val currentRowNum = sparkSession.sql("select max(sequence) from initialDfSha").collect()(0).getLong(0)
					deltaDfSha.createOrReplaceTempView("deltaDfSha")
					import org.apache.spark.sql.functions._ 
					val deltaDfShaSeq = deltaDfSha.withColumn("sequence", monotonically_increasing_id + currentRowNum)

					val deduped = initialDfSha.union(deltaDfShaSeq).rdd.map { row => (row.getString(row.length-2), row) }.
					reduceByKey((r1, r2) => r1)
					map { case(sha2, row) => row }

					sparkSession.createDataFrame(deduped, deltaDfShaSeq.schema)
	}

	def main(args: Array[String]): Unit = {
			    val conf = new SparkConf().setAppName("test").setMaster("local")
					val sparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
				  import sparkSession.implicits._
					val c = sparkSession.sparkContext.textFile("").map { _.split("") }.map { x => Row(x) }
					  
					val df1 = sparkSession.createDataset(Seq((1,2, "date"), (3,4, "date2"))).toDF("col1", "col2", "archive_date")
					val df2 = sparkSession.createDataset(Seq((1,2), (4,5))).toDF("col1", "col2")
					addDeltaFirstTime(df1, df2).show()

					val df3 = sparkSession.createDataset(Seq((1,2, "e938687d93115efb90247142521e32fa93cd11ef136820c65f586dbdb3c", 1L), 
							(3,4, "e4da721a58b4a26606931ec2a375a1f565f6884479487f668bb7bceb5bc25f", 2L))).toDF("col1", "col2", "sha2", "sequence")
					val df4 = sparkSession.createDataset(Seq((1,2), (4,5))).toDF("col1", "col2")
					addDeltaIncremental(df3, df4).show()
	} 

}
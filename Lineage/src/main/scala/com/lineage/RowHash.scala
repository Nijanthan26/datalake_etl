package com.lineage
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
object RowHash {
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
}
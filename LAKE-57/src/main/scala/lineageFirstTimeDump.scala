import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
import org.apache.spark.SparkContext
object lineageFirstTimeDump {

	def addHash(deltaDf: Dataset[Row]): Dataset[Row] = {
			    val sparkSession = deltaDf.sparkSession
					sparkSession.udf.register("sha2m", (r: Row) => {
						val sha2Hasher = MessageDigest.getInstance("SHA-256")
								val buf = new StringBuilder
								val bytes = r.mkString(",").getBytes
								val hexString: StringBuffer = new StringBuffer
								sha2Hasher.digest(bytes).foreach { b => hexString.append(Integer.toHexString(0xFF & b))
								}
								hexString.toString()
					})

					deltaDf.createOrReplaceTempView("delta_table")

					sparkSession.sql("select *, sha2m(struct(*)) as sha2 from delta_table")
	}

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
    val df1 = sqlContext.sql("select * from antuit_stage.data1")
    val df2 = sqlContext.sql("select * from antuit_stage.data2")
    val res = addDeltaFirstTime(df1, df2)
    res.show()
    res.registerTempTable("mytempTable")
	sqlContext.sql("drop table if exists antuit_stage.data3;create table antuit_stage.data3 as select * from mytempTable");


    }

}
   

}
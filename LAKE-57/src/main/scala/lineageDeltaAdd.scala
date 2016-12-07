import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
import org.apache.spark.SparkContext
object lineageDeltaAdd {

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
        def addDeltaIncremental(initialDfShaWithDate: Dataset[Row], deltaDf: Dataset[Row]): Dataset[Row] = {
                                val initialDfSha = initialDfShaWithDate.drop("seq")
                                        val sparkSession = deltaDf.sparkSession
                                        val deltaDfSha = addHash(deltaDf)

                                        initialDfSha.createOrReplaceTempView("initialDfSha")
                                        val currentRowNum = sparkSession.sql("select max(sequence) from initialDfSha").collect()(0).getLong(0)
                                        deltaDfSha.createOrReplaceTempView("deltaDfSha")
                                        import org.apache.spark.sql.functions._
                                        val deltaDfShaSeq = deltaDfSha.withColumn("sequence", monotonically_increasing_id + currentRowNum)

                                        val deduped = initialDfSha.union(deltaDfShaSeq).rdd.map { row => (row.getString(row.length-2), row) }.reduceByKey((r1, r2) => r1).map { case(sha2, row) => row }

                                        sparkSession.createDataFrame(deduped, deltaDfShaSeq.schema)
        }


def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
        val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val dfProc = sqlContext.sql("select * from antuit_stage.data3")
    val dfFresh = sqlContext.sql("select * from antuit_stage.data4")
    val res = addDeltaIncremental(dfProc, dfFresh)
    res.show()
    res.registerTempTable("mytempTable")
        sqlContext.sql("drop table if exists antuit_stage.data3")
        sqlContext.sql(" create table antuit_stage.data3 as select * from mytempTable")


    }

}

package com.lineage
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import com.microsoft.sqlserver.jdbc.SQLServerDriver



object importtable {
  
  def main(args: Array[String]) {
   //val  tablename= args(0)
   val conf = new SparkConf().setAppName("Load Data from DB")
   val sc = new SparkContext(conf)
   val sqlcontext = new org.apache.spark.sql.SQLContext(sc)

val dataframe_db = sqlcontext.read.format("jdbc").
option("url", "jdbc:sqlserver://192.168.100.223:1433;databaseName=AAD").
option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
option("dbtable", "t_bmm_customer").
option("user", "readonly").
option("password", "HJ#ric1!").load()


dataframe_db.rdd.map { x => x.mkString("\u0001")}.saveAsTextFile("/antuit_stage/"+args(0))


}
}

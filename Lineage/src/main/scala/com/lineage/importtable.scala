package com.lineage
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import com.microsoft.sqlserver.jdbc.SQLServerDriver



object importtable {
  
  def main(args: Array[String]) {
   val  tablename= args(0)
   val conf = new SparkConf().setAppName("Import")
   val sc = new SparkContext(conf)
   val sqlcontext = new org.apache.spark.sql.SQLContext(sc)

val dataframe_db = sqlcontext.read.format("jdbc").
option("url", "jdbc:mysql://lineage-mule-cluster.cluster-cpslmao02wkq.us-west-2.rds.amazonaws.com:3306/mrs_uat").
option("driver", "com.mysql.jdbc.Driver").
option("dbtable", tablename).
option("user", "readonly").
option("password", "5u9pvWECL9DPtHUB").load()

dataframe_db.show()
//dataframe_db.write.format("parquet").saveAsTable("antuit_stage.mule_"+tablename)
dataframe_db.rdd.map { x => x.mkString("\u0001")}.saveAsTextFile("/antuit/sqoopdest/hj_"+tablename)


}
}

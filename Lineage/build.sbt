name := "lineage-datalake-spark-scala"
version := "1.0"
scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.0",
  "org.apache.spark" % "spark-sql_2.11" % "1.6.0",
  "org.apache.spark" % "spark-hive_2.11" % "1.6.0" ,
  "com.microsoft.sqlserver" % "mssql-jdbc" % "6.1.0.jre7",
    "org.postgresql" % "postgresql" % "9.4-1200-jdbc41"
)
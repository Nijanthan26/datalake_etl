package com.lineage.db
 import java.sql.DriverManager
import java.sql.Connection

object test {
  
  
 
  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://metricsone.cpslmao02wkq.us-west-2.rds.amazonaws.com:5432/postgres"
    val username = "root"
    val password = "TiMLRHdCNLftYOLskOkF"

    // there's probably a better way to do this
    var connection:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("show tables")
      while ( resultSet.next() ) {
        val host = resultSet.getString(1)
        println(host)
      }
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
  }


}
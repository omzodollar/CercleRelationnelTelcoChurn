package com.crtchurn.core.graph
import org.neo4j.driver.v1._
import com.crtchurn.CRTApp
import org.apache.spark.sql.DataFrame
import org.neo4j.spark.Neo4jDataFrame
import org.apache.spark.sql.types._
import com.crtchurn.CRTApp._spark

object GraphToNeo4j {
  case class User(name: String, last_name: String, age: Int, city: String)
  def insertRecord(user: User): Int = {
    val driver = GraphDatabase.driver("bolt://localhost:11001", AuthTokens.basic("neo4j", "passer"))
    val session = driver.session
    val script = s"CREATE (user:Users {name:'${user.name}',last_name:'${user.last_name}',age:${user.age},city:'${user.city}'})"
    val result: StatementResult = session.run(script)
    session.close()
    driver.close()
    result.consume().counters().nodesCreated()
  }
     def createGraph() : Unit = {
       val driver = GraphDatabase.driver("bolt://localhost:11001", AuthTokens.basic("neo4j", "passer"))
       val session = driver.session

       val query =
        s"""
           |LOAD CSV WITH HEADERS FROM
           |"file:///cdr.csv" as line
           |CREATE (subscriber:Subscriber {id : toInteger(line.`Subscriber ID`)})
           |RETURN subscriber.id
     """.stripMargin
       val result: StatementResult = session.run(query)
       session.close()
       driver.close()
       result.consume().counters().nodesCreated()

       //Neo4jDataFrame.withDataType(_spark.sqlContext, query, Seq.empty, "Name" -> LongType)
      /* val query2 =
         s"""
            |MATCH(n:Complaint) RETURN complaint.id
          """.stripMarginx
      // Neo4jDataFrame.withDataType(_spark.sqlContext, query2, Seq.empty, "Name" -> IntegerType)*/
    }
    //val neo = Neo4j(_spark.sparkContext)
    //val testConnection = neo.cypher("MATCH (n) RETURN n;")

}

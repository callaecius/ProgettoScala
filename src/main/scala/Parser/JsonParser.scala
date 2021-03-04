package Parser

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, explode, second}
import org.joda.time.DateTime
import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import scala.tools.scalap.scalax.rules.scalasig.ScalaSigEntryParsers.entryType



object JsonParser {

  val conf = new SparkConf().setMaster("local[2]").setAppName("MyTable")
  val sContext = new SparkContext(conf)
  val sqlContext = new SQLContext(sContext)
  val hiveContext = new HiveContext(sContext)
  val input = "/Users/daltoncalder/BigData/stream/input/Firs500Rows.json"

  import hiveContext.implicits._
  def main(args: Array[String]){

    val dfEvent = sqlContext.read.json(input)
    val new_dfEvent = dfEvent.withColumnRenamed("public", "publicField")

    val dsEvent = new_dfEvent.as[Event]
    val rddEvent = dsEvent.rdd

    /**********Trovare i Singoli Autori**********/
    val dfActor = new_dfEvent.select("actor").distinct()
    dfActor.show()

    val rddActor = rddEvent.map(x => x.actor).distinct()
    rddActor.take(10).foreach(println)

    /**********Trovare i Singoli Autori all’interno dei Commit**********/
    val dfPayload = dfEvent.select("payload.*")
    val dfCommits = dfPayload.select(explode(col("commits"))).select("col.*")
    val dfAuthor = dfCommits.select("author").distinct()
    dfAuthor.show()

    val rddCommit = dfCommits.as[Commit].rdd
    val rddAuthor = rddCommit.map(x => x.author).distinct()
    rddAuthor.take(10).foreach(println)

    /**********Trovare i Singoli Repo**********/
    val dfRepo = new_dfEvent.select("repo").distinct()
    dfRepo.show()

    val rddRepo = rddEvent.map(x => x.repo).distinct()
    rddRepo.take(10).foreach(println)

    /**********Trovare i vari tipi di evento Type**********/
    val dfType = new_dfEvent.select("`type`").distinct()
    dfType.show()

    val rddType = rddEvent.map(x => x.`type`).distinct()
    rddType.take(10).foreach(println)

    /**********Contare il numero di Autori**********/
    val dfAttore = new_dfEvent.select("actor").distinct().count()
    println(dfAttore)

    val rddAttore = rddEvent.map(x => x.actor).distinct().count()
    println(rddAttore)

    /**********Contare il numero di Repo**********/
    val dfRepos = new_dfEvent.select("repo").distinct().count()
    println(dfRepos)

    val rddRepos = rddEvent.map(x => x.repo).distinct().count()
    println(rddRepos)

  }
}

package Parser

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, explode, second, agg , max, min}
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

    /********** 1/13 Trovare i Singoli Autori **********/
    val dfActor = new_dfEvent.select("actor").distinct()
    dfActor.show()

    val rddActor = rddEvent.map(x => x.actor).distinct()
    rddActor.take(10).foreach(println)

    /********** 2/13 Trovare i Singoli Autori all’interno dei Commit **********/
    val dfPayload = dfEvent.select("payload.*")
    val dfCommits = dfPayload.select(explode(col("commits"))).select("col.*")
    val dfAuthor = dfCommits.select("author").distinct()
    dfAuthor.show()

    val rddCommit = dfCommits.as[Commit].rdd
    val rddAuthor = rddCommit.map(x => x.author).distinct()
    rddAuthor.take(10).foreach(println)

    /********** 3/13 Trovare i Singoli Repo **********/
    val dfRepo = new_dfEvent.select("repo").distinct()
    dfRepo.show()

    val rddRepo = rddEvent.map(x => x.repo).distinct()
    rddRepo.take(10).foreach(println)

    /********** 4/13 Trovare i vari tipi di evento Type **********/
    val dfType = new_dfEvent.select("`type`").distinct()
    dfType.show()

    val rddType = rddEvent.map(x => x.`type`).distinct()
    rddType.take(10).foreach(println)

    /********** 5/13 Contare il numero di Autori **********/
    val dfAttore = new_dfEvent.select("actor").distinct().count()
    println(dfAttore)

    val rddAttore = rddEvent.map(x => x.actor).distinct().count()
    println(rddAttore)

    /********** 6/13 Contare il numero di Repo **********/
    val dfRepos = new_dfEvent.select("repo").distinct().count()
    println(dfRepos)

    val rddRepos = rddEvent.map(x => x.repo).distinct().count()
    println(rddRepos)

    /********** 1/14 Contare il numero di Event per ogni Attore **********/
    val dfNumbEvent = new_dfEvent.groupBy( "actor").count().show()
    println(dfNumbEvent)

    val rddAct = rddEvent.map(x => (x.actor, 1L)).reduceByKey((countFirst, contSecond) => countFirst + contSecond)
      rddAct.foreach(println)

    /********** 2/14 Contare il numero di Event divisi per Type e Attore **********/
    val dfEvento = new_dfEvent.select(($"type"), ($"actor"), count($"*").over(Window.partitionBy("type", "actor")) as "nEvent")
    dfEvento.show()

    val rddEvento = rddEvent.map(x => ((x.`type`, x.actor), 1L)).reduceByKey((e1,e2) => e1+e2)
    rddEvento.take(10).foreach(println)

    /********** 3/14 Contare il numero di Event divisi per Type, Attore e Repo **********/
    val dfEventos = new_dfEvent.select($"type", $"actor", $"repo", count($"*").over(Window.partitionBy($"type", $"actor", $"repo")) as "nEvent")
    dfEventos.show()

    val rddEventos = rddEvent.map(x => ((x.`type`, x.actor, x.repo), 1L)).reduceByKey((e1,e2) => e1+e2)
    rddEventos.take(10).foreach(println)

    /********** 4/14 Contare il numero di Event divisi per Type, Attore, Repo e Secondo **********///in dubbio
    val dfData = new_dfEvent.withColumn("second", second($"created_at"))
    val dfEventi = dfData.select($"type", $"actor", $"repo", $"second", count($"*").over(Window.partitionBy($"type", $"actor", $"repo", $"second")) as "nEvent")
    dfEventi.show()

    val rddEventi = rddEvent.map(x=> ((x.`type`, x.actor, x.repo, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contFrist, contSecond) => contFrist + contSecond)
    rddEventi.take(10).foreach(println)

    /********** 5/14 Trovare il massimo diviso minimo numero di Event per Secondo **********/
    val dfDataMassima = new_dfEvent.withColumn("second", second($"create_at"))
    val dfEventMassima = dfDataMassima.select($"second", count($"*").over(Window.partitionBy($"second")) as "conteggio")
    val dfMassimoEvent = dfEventMassima.agg(max("conteggio"))
    dfMassimoEvent.show()
    val df_RddDataMassima = rddEvent.map(x =>(x.actor.id, x)).aggregateByKey(0)((cont, actor) => cont + 1, (contFrist, contSecond) => contFrist + contSecond)
    df_RddDataMassima.take(10).foreach(println)

    /********** 6/14 Trovare il massimo diviso minimo numero di Event per Attore **********/
    //Massimo||
    val dfMaxAttore = new_dfEvent.select($"actor", count($"*").over(Window.partitionBy($"actor")) as "conteggio")
    val dfMasActor = dfMaxAttore.agg(max("conteggio"))
    dfMasActor.show()

    val rddMaxAttore = new_dfEvent.map(x => (x.actor.id, x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMaxActor = rddEvent.map(x => x._2).max()
    println(rddMaxActor)

    //Minimo||
    val dfMinAttore = new_dfEvent.select($"actor", count($"*").over(Window.partitionBy($"actor")) as "conteggio")
    val dfMinActor = dfMinAttore.agg(min("conteggio"))
    dfMinActor.show()

    val dfMinimoAttore = new_dfEvent.map(x => (x.actor.id, x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMinActor = rddEvent.map(x => x._2).min()
    println(rddMinActor)

    /********** 7/14 Trovare il massimo diviso minimo numero di Event per Repo **********/
    //Massimo||
    val dfMax_Repo = new_dfEvent.select($"repo", count($"*").over(Window.partitionBy($"repo")) as "conteggio")
    val dfMassimoRepo = dfMax_Repo.agg(max("conteggio"))
    dfMassimoRepo.show()

    val rddMax_Repo = rddEvent.map(x => (x.repo, x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMassimoRepo = rddMax_Repo.map(x => x._2).max()
    println(rddMassimoRepo)

    //Minimo||
    val dfMin_Repo = new_dfEvent.select($"repo", count($"*").over(Window.partitionBy($"repo")) as "conteggio")
    val dfMinimoRepo = dfMin_Repo.agg(min("conteggio"))
    dfMinimoRepo.show()

    val rddMin_Repo = rddEvent.map(x => (x.actor.id, x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMinimoRepo = rddMin_Repo.map(x => x._2).min()
    println(rddMinimoRepo)

    /********** 8/14 Trovare il massimo diviso minimo numero di Event per Secondo per Attore **********/
    //Massimo||
    val dfMaxSecAtt = new_dfEvent.withColumn( "second", second($"created_at"))
    val dfMa_AttSec = dfMaxSecAtt.select( $"second", $"actor", count($"*").over(Window.partitionBy( $"second", $"actor")) as "conteggio")
    val dfMassAS = dfMa_AttSec.agg(max("conteggio"))
    dfMassAS.show()

    val rddMaxAS = rddEvent.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.actor.id), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMassimoAS = rddMaxAS.map(x => x._2).max()
    println(rddMassimoAS)

    //Minimo||
    val dfMinSecAtt = new_dfEvent.withColumn( "second", second($"created_at"))
    val dfMin_AttSec = dfMinSecAtt.select( $"second", $"actor", count($"*").over(Window.partitionBy( $"second", $"actor")) as "conteggio")
    val dfMinAS = dfMin_AttSec.agg(min("conteggio"))
    dfMinAS.show()

    val rddMinAS = rddEvent.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.actor.id), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMinimoAS = rddMinAS.map(x => x._2).min()
    println(rddMinimoAS)

    /********** 9/14 Trovare il massimo diviso minimo numero di Event per Secondo per Repo **********/
    //Massimo||
    val dfMaxESR = new_dfEvent.withColumn( "second", second($"created_at"))
    val dfMax_ESR = dfMaxESR.select( $"second", $"repo", count($"*").over(Window.partitionBy( $"second", $"repo")) as "conteggio")
    val dfMassimoESR = dfMax_ESR.agg(max("conteggio"))
    dfMassimoESR.show()

    val rddMaxESR = rddEvent.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMassimoESR = rddMaxESR.map(x => x._2).max()
    println(rddMassimoESR)

    //Minimo||
    val dfMinESR = new_dfEvent.withColumn( "second", second($"created_at"))
    val dfMix_ESR = dfMinESR.select( $"second", $"repo", count($"*").over(Window.partitionBy( $"second", $"repo")) as "conteggio")
    val dfMinimoESR = dfMix_ESR.agg(min("conteggio"))
    dfMinimoESR.show()

    val rddMinESR = rddEvent.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMinimoESR = rddMinESR.map(x => x._2).min()
    println(rddMinimoESR)

    /********** 10/14 Trovare il massimo diviso minimo numero di Event per Secondo per Repo per Attore **********/
    //Massimo||
    val dfMaxESRA = new_dfEvent.withColumn( "second", second($"created_at"))
    val dfMax_ESRA = dfMaxESRA.select( $"second", $"repo", $"actor", count($"*").over(Window.partitionBy( $"second", $"repo", $"actor")) as "conteggio")
    val dfMassimoESRA = dfMax_ESRA.agg(max("conteggio"))
    dfMassimoESRA.show()

    val rddMaxESRA = rddEvent.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo, x.actor.id), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMassimoESRA = rddMaxESRA.map(x => x._2).max()
    println(rddMassimoESRA)

    //Minimo||
    val dfMinESRA = new_dfEvent.withColumn( "second", second($"created_at"))
    val dfMix_ESRA = dfMinESRA.select( $"second", $"repo", $"actor", count($"*").over(Window.partitionBy( $"second", $"repo", $"actor")) as "conteggio")
    val dfMinimoESRA = dfMix_ESRA.agg(min("conteggio"))
    dfMinimoESRA.show()

    val rddMinESRA = rddEvent.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo, x.actor.id), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMinimoESRA = rddMinESRA.map(x => x._2).min()
    println(rddMinimoESRA)

    /********** 1/15 Contare il numero di Commit **********/
    val dfNumbComit = dfCommits.distinct().count()
    println(dfNumbComit)

    val rddNumbComit  = rddCommit.distinct().count()
    println(rddNumbComit)

    /********** 2/15 Contare il numero di Commit per Attore **********/
    val dfNumbComitAct = dfPayload.select(explode(col("commits"))).select("col.*").crossJoin(new_dfEvent)
    val dfComAct = dfNumbComitAct.select(col("actor"), count($"*").over(Window.partitionBy("actor")) as "conteggio")
    dfComAct.show()

    val rddNumComAct = rddCommit.distinct().count()
    println(rddNumComAct)

    /********** 3/15 Contare il numero di Commit Diviso per Type e Attore **********/
    val dfDivTA = jsonDF.select("*").withColumn("commitSize", size(col("payload.commits"))).groupBy("type", "actor")
      .agg(sum("commitSize").as("totSizeCommit")).show()
    println(dfDivTA)

    /********** 4/15 Contare il numero di Commit Diviso per Type, Attore e Event **********/

    /********** 5/15 Contare il numero di Commit Diviso per Type, Attore e Secondo **********/

    /********** 6/15 Trovare il massimo diviso minimo numero di Commit per Secondi **********/

    /********** 7/15 Trovare il massimo diviso minimo numero di Commit per Attori **********/

    /********** 8/15 Trovare il massimo diviso minimo numero di Commit per Repo **********/

    /********** 9/15 Trovare il massimo diviso minimo numero di Commit per Secondi per Attore **********/

    /********** 10/15 Trovare il massimo diviso minimo numero di Commit per Secondi per Repo **********/

    /********** 11/15 Trovare il massimo diviso minimo numero di Commit per Secondi per Repo e Attore **********/

    /********** 1/16 Contare il numero di Attore attivi per ogni Secondo **********/

    /********** 2/16 Contare il numero di Attore divisi per ogni Type e Secondo **********/
    val dfContAdviTS = jsonDF.withColumn("seconds", second(col("created_at")))
      .groupBy("type", "seconds", "actor").count()
    dfContAdviTS.show()

    /********** 3/16 Contare il numero di Attore divisi per ogni Repo, Type e Secondo **********/

    /********** 4/16 Il massimo diviso minimo numero di Attore attivi per Secondo **********/

    /********** 5/16 Il massimo diviso minimo numero di Attore attivi per Secondo e Type **********/

    /********** 6/16 Il massimo diviso minimo numero di Attore attivi per Secondo, Type e Repo **********/

  }
}

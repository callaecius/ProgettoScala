package Steaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.sql.Timestamp
import java.text.SimpleDateFormat

object Streaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyFirstSparkApp").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val stream_context = new StreamingContext(sc, Seconds(5))


    val filestream = stream_context.textFileStream("/Users/daltoncalder/BigData/stream/input")
    stream_context.checkpoint("/Users/daltoncalder/BigData/checkpoint")


    val dateformat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")


    val logs = filestream.flatMap(line => {
      val lineSplitted = line.split(",")
      List(Log(new Timestamp(dateformat.parse(lineSplitted(0)).getTime), lineSplitted(1), lineSplitted(2).toInt))
    })

    val logsPair = logs.map(l => (l.tag, 1L))
    val result = logsPair.reduceByKey((l1, l2) => l1 + l2)


    val myFunction = (seqVal: Seq[Long], stateOpt: Option[Long]) => {
      stateOpt match {
        case Some(state) => Option(seqVal.sum + state)
        case None => Option(seqVal.sum)
      }
    }

    val logsPairTotal = logsPair.updateStateByKey(myFunction)
    val joinedLogsPair = result.join(logsPairTotal)

    joinedLogsPair.repartition(1).saveAsTextFiles("/Users/daltoncalder/BigData/stream/output", "txt")

    stream_context.start()
    stream_context.awaitTermination()

  }

}

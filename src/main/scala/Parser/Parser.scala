package Parser

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Parser {
  val conf = new SparkConf().setMaster("local[2]").setAppName("CountingSheep")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    val df = sqlContext.read.json("/Users/daltoncalder/BigData/CartellaJson/2018-03-01-0.json")
    df.registerTempTable("myTable")

    //risolve problema con il public
    val new_df = df.withColumnRenamed("public", "publicField")
    new_df.show()

    //1RDD
    val data = sqlContext.sql("select * from myTable")
    data.show()
    //val dataRDD = data.as[Event].rdd
    //dataRDD.foreach(println)

    //autorRdd
    val dataActor = sqlContext.sql("select actor from myTable")
    dataActor.show()
    sc.stop
  }
}

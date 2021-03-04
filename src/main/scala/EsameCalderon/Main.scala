package EsameCalderon

import org.apache.spark.sql.{Row, SQLContext}
import Parser.JsonParser.hiveContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Window
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{col, count, explode, max, min, second}


object Main {
  val conf = new SparkConf().setMaster("local[2]")
    .setAppName("CountingSheep")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new HiveContext(sc)

  import hiveContext.implicits._
  def main(args: Array[String]): Unit = {



  val  autore1 = new Autore(1,"Stephen", "King","1947","Portland")
  val  autore2 = new Autore(2,"George", "Martin","1948","Bayonne")
  val  autore3 = new Autore(3,"Licia", "Troisi","1980","Roma")
  val  autore4 = new Autore(4,"John", "Tolkien","1892","Bloemfontein")
  val listaAutori = Seq(autore1,autore2,autore3,autore4)

  val casaEditrice1 = new CasaEditrice(1, "Smerciamo libri", "Milano")
  val casaEditrice2 = new CasaEditrice(2, "Libri libri", "Roma")
  val casaEditrice3 = new CasaEditrice(3, "LibriAlQuadrato", "Torino")
  val casaEditrice4 = new CasaEditrice(4, "Book House", "Miami")
  val listaCaseEditrici = Seq(casaEditrice1,casaEditrice2,casaEditrice3,casaEditrice4)

  val libro1 = new Libro(1, "IT",1, 1079, "1986",4)
  val libro2 = new Libro(2, "Carrie",1, 631,"1974",4)
  val libro3 = new Libro(3, "Shining",1, 781,"1977",1)
  val libro4 = new Libro(4, "Pet semetary",1, 543,"1977",1)
  val libro5 = new Libro(5, "Il trono di spade",2, 499,"1999",3)
  val libro6 = new Libro(6, "Uno scontro tra re",2, 578,"1999",2)
  val libro7 = new Libro(7, "Tempesta di spade",2, 601,"1977",2)
  val libro8 = new Libro(8, "Il banchetto dei corvi",2, 790,"2000",1)
  val libro9 = new Libro(9, "I guerrieri del ghiaccio",2, 791,"2000",1)
  val libro10 = new Libro(10, "Nihal della terra del vento",3, 350,"2004",1)
  val libro11 = new Libro(11, "La missione di Sennar",3, 400,"2004",3)
  val libro12 = new Libro(12, "Il talismano del potere",3, 333,"2004",3)
  val libro13 = new Libro(13, "Le storie perdute",3, 499,"2005",2)
  val libro14 = new Libro(14, "Lo hobbit",4, 323,"1974",4)
  val libro15 = new Libro(15, "Il signore degli anelli",4, 987,"1978",4)
  val listaLibri = Seq(libro1 ,
    libro2 ,
    libro3 ,
    libro4 ,
    libro5 ,
    libro6 ,
    libro7 ,
    libro8 ,
    libro9 ,
    libro10,
    libro11,
    libro12,
    libro13,
    libro14,
    libro15)

  val film1 = new Film(1,"Carrie","Brian de palma",98,2,"1976")
  val film2 = new Film(2, "Carrie","Kimberly Peirce",99,2,"2013")
  val film3 = new Film(3, "IT Capitolo 1","Andy Muschietti",135,1,"2017")
  val film4 = new Film(4, "IT Capitolo 2","Andy Muschietti",169,1,"2019")
  val film5 = new Film(5, "IT","Tommy Lee Wallace",192,1,"1990")
  val film6 = new Film(6, "Lo hobbit"," Peter Jackson",182,14,"2012")
  val film7 = new Film(7, "Lo hobbit 2"," Peter Jackson",180,14,"2012")
  val listaFilm = Seq(film1,
    film2,
    film3,
    film4,
    film5,
    film6,
    film7)

    /**********1'Esercizio**********/
  val listaAutoriRdd = sc.parallelize(listaAutori)
  val listacasaEditriceRdd  = sc.parallelize(listaAutori)
  val listaLibroRdd  = sc.parallelize(listaAutori)
  val listaFilmRdd  = sc.parallelize(listaAutori)

  val dfListaAutori = listaAutoriRdd.toDF()
  val dfListacasaEditrice = listacasaEditriceRdd.toDF()
  val dfListaLibro = listaLibroRdd.toDF()
  val dfListaFilm = listaFilmRdd.toDF()

    /**********2'Esercizio**********/
    val casaEditriceDF = dfListacasaEditrice.select("id","sede")
    casaEditriceDF.show()

    /**********3'Esercizio**********/
    val listLibri1981 = dfListaLibri.filter($"annoDiUscita" >= 1981)
    listLibri1981.show()

    /**********4'Esercizio**********/
    val infoDF = dfListaFilm.join(dfListaLibro, dfListaFilm("idautore") === dfListaLibro("id"),"inner")
    infoDF.show()

    /**********5'Esercizio**********/
    val maxPagine = dfListacasaEditrice.select(col("idEditrici"),max(casaEditriceDF("numeroPagine")).over(Window.partitionBy("idLibri")) as "max")
    maxPagine.show()

    /**********6'Esercizio**********/
    val dfxAutore = dfListaLibro.join(dfListaLibro, dfListacasaEditrice("id") === dfListaLibro("idAutore"),"inner")
    val dfxAutore = dfListaAutori.select(col("nome"),first(listaLibriDF("annoDiPubblicazione")).over(Window.partitionBy("nome")) as "primoAnnoDiPubblicazione")

    /**********7'Esercizio**********/

    /**********8'Esercizio**********/
    val autori = listaAutoriRdd.filter(x => (x.annoDiUscita.contains("1970") ))
    autori.foreach(println)

    /**********9'Esercizio**********/

    /**********10'Esercizio**********/
    val c_LibriRdd = listaLibroRdd.map(x=>(x.idEditrici,x))
    val grouped = c_LibriRdd.groupByKey()
    grouped.foreach(println)

    /**********11'Esercizio**********/
    val libroPair = listaLibroRdd.map(x => (x.annoDiPubblicazione, x.idCasaEditrice))
    val pairBook = libroPair.reduceByKey((firstYear,secondYear) =>{
      if( firstYear > secondYear){
        firstYear
      }else{
        secondYear
      }
    })
    pairBook.foreach(println)

    /**********12'Esercizio**********/

    /**********13'Esercizio**********/
    def tempoD = (libro1: Libro) => {
      if (libro1.annoDiPubblicazione < "1990") {
        "Vecchio"
      } else if(libro1.annoDiPubblicazione > "2000") {
        "Nuovo"
      }else
      {
        "Recente"
      }
    }
    val pairIdLibri = listaLibroRdd.map(x=>(x.id, tempoD))
    val pairIdLibriGrouped = pairIdLibri.groupByKey()
    pairIdLibriGrouped.foreach(println)

  }
}

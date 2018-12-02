package com.test.spark.wiki.extracts

import java.io.{FileInputStream, FileReader, InputStreamReader}
import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.slf4j.{Logger, LoggerFactory}
import java.io.File

import scala.collection.JavaConversions._
import scala.tools.scalap.scalax.util.StringUtil

case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)

    // TODO Q1 Transformer cette seq en dataset
    val yamlRdd = session.sparkContext.parallelize(getLeagues)
      .flatMap {
        input =>
          (fromDate.getYear until toDate.getYear).map {
            year =>
              year + 1 -> (input.name, input.url.format(year, year + 1))
          }
      }
    /*
    yamlRdd.foreach(println)
    val yamlRdd2 = yamlRdd.flatMap {
      case (season, (league, url)) =>
        List(LeagueStanding(league,season,1,"",1,1,1,1,1,1,1,1))
    }
    yamlRdd2.foreach(println)
    */


    val leagueRdd = yamlRdd.flatMap {
        case (season, (league, url)) =>
          try {
            // TODO Q2 Implémenter le parsing Jsoup. Il faut retourner une Seq[LeagueStanding]
            // ATTENTION:
            //  - Quelques pages ne respectent pas le format commun et pourraient planter - pas grave
            //  - Il faut veillez à recuperer uniquement le classement général
            //  - Il faut normaliser les colonnes "positions", "teams" et "points" en cas de problèmes de formatage
            //parse the url using Jsoup to get the Seq[LeagueStanding]
            println("parse url : "+url)
            //create random list for testing
            List(LeagueStanding(league,season,(math.random * (30-20) + 20).toInt
              ,"Team"+(math.random * (30-20) + 20).toInt
              ,(math.random * (30-20) + 20).toInt
              ,(math.random * (30-20) + 20).toInt
              ,(math.random * (30-20) + 20).toInt
              ,(math.random * (30-20) + 20).toInt
              ,(math.random * (30-20) + 20).toInt
              ,(math.random * (30-20) + 20).toInt
              ,(math.random * (30-20) + 20).toInt
              ,(math.random * (30-20) + 20).toInt
            ))
          } catch {
            case _: Throwable =>
              // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
              logger.warn(s"Can't parse season $season from $url")
              Seq.empty
          }
     }
      // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket
    import session.implicits._
    val leagueDF = leagueRdd.toDF()
    leagueDF.printSchema()
    leagueDF.coalesce(2).write.mode(SaveMode.Overwrite).format("parquet").save(bucket)
    //.mode(SaveMode.Overwrite)
    //.parquet(bucket)

    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?

    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?
  }

  /*
  private def test: Unit = {
    println("read me $$$$$$$$$$")
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    val fr = new FileReader(new File("src/main/resources/leagues.yaml"))
    var s = fr.read()
    println(s)
  }
  */

  private def getLeagues: Seq[LeagueInput] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    mapper.readValue(new File("src/main/resources/leagues.yaml"), classOf[Array[LeagueInput]]).toSeq
  }
}

// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
case class LeagueInput(@JsonProperty("name") name: String,
                       @JsonProperty("url") url: String)

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Int)

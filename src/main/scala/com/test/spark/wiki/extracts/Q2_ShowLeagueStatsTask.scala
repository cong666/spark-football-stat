package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()

  import session.implicits._

  override def run(): Unit = {
    val standings = session.read.parquet(bucket).as[LeagueStanding].cache()

    /*
    *+------+------+--------+------+------+------+---+-----+----+--------+------------+---------------+
|league|season|position|  team|points|played|won|drawn|lost|goalsFor|goalsAgainst|goalsDifference|
+------+------+--------+------+------+------+---+-----+----+--------+------------+---------------+
|  Liga|  1978|      26|Team25|    29|    28| 24|   26|  22|      21|          25|             22|
|  Liga|  1979|      28|Team26|    22|    26| 26|   27|  22|      23|          23|             29|
|  Liga|  1980|      29|Team29|    27|    23| 28|   28|  20|      22|          21|             21|
    */
    // TODO Répondre aux questions suivantes en utilisant le dataset $standings
    standings
      // ...code...
      .show(standings.count().toInt)

    // TODO Q1
    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et " +
      "par championnat")
    standings.createOrReplaceTempView("standings_info")
    val dfQ1 = session.sql("select season,league, avg(points) from standings_info group by season,league")
    dfQ1.show(dfQ1.count().toInt)
    //val dfQ1_t = standings.groupBy("season","league").agg(avg("points"))
    //dfQ1_t.show(dfQ1_t.count().toInt)

    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")
    val dfQ2 = standings.filter($"league" === "Ligue 1").groupBy("team").agg(max("points").as("max_points")).orderBy($"max_points")
    dfQ2.show(dfQ2.count().toInt)

    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")
    val dfQ3 = standings.groupBy("league").agg(avg("won"))
    dfQ3.show(dfQ3.count().toInt)

    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?

    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnats " +
      "par décennie")
    var dfQ4 = standings.orderBy(standings("league"),standings("points").desc,standings("season"))
    dfQ4.show(dfQ4.count().toInt)
  }
}

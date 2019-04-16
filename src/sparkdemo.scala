

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkdemo {

  case class ChooseZiduan
  (
    mmsi: Long,
    acqTime: Long,
    lon: Long,
    lat: Long,
    speed: Long
  )

  def main(args: Array[String]):Unit = {
    val appName = "test"
    val master = "local"
    val input1 = "/Sparkheng/container_201701.csv"
    val input2 = "/Sparkheng/shipinfo.csv"
    val output = "/Sparkheng/ship"

    Logger.getLogger("org").setLevel(Level.ERROR )

    val conf = new SparkConf()
    conf.setMaster(master)
    conf.setAppName(appName)


    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc = SparkContext
    import ss.implicits._


    val containerDf = ss.read
      .option("header","true")
      .csv(input1)
      .filter(row => !(row.getString(0).contains("uniqueId") || row.getString(0).replaceAll("[,\\s]", "").equals("")))
      .map(line => {

        ChooseZiduan(
          mmsi = line.getString(0).toLong,
          acqTime = line.getString(1).toLong,
          lon = line.getString(6).toLong,
          lat = line.getString(7).toLong,
          speed = line.getString(9).toLong
        )


      })
    val shipDf = ss.read
      .option("header","true")
      .csv(input2)

    val chooseDf = containerDf
      .join(
        shipDf, containerDf("mmsi") === shipDf("ais_mmsi")
      )
    chooseDf.createTempView("joined_tb")

    chooseDf
      .repartition(1)
      .write
      .option("header","true")
      .csv(output)

  }
}

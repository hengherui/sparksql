
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Test {

  case class ChooseZiduan
  (
    mmsi: Long,
    acqTime: Long,
    lon: Long,
    lat: Long

  )

  def main(args: Array[String]): Unit = {
    val appName = "test"
    val master = "local"
    val input1 = "/Sparkheng/20190110_FilterThrDynamicCompressByMmsi_201808.csv"
    // val input2 = "/Sparkheng/shipinfo.csv"
    val output = "/Sparkheng/shipmonth8"

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster(master)
    conf.setAppName(appName)


    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc = SparkContext
    import ss.implicits._


    val containerDf = ss.read
      .option("header", "true")
      .csv(input1)
      .filter(row => !(row.getString(0).contains("uniqueId") || row.getString(0).replaceAll("[,\\s]", "").equals("")))
      .map(line => {

        ChooseZiduan(
          mmsi = line.getString(0).toLong,
          acqTime = line.getString(1).toLong,
          lon = line.getString(6).toLong,
          lat = line.getString(7).toLong

        )


      })

    containerDf.createTempView("df")

    val df = ss.sql("SELECT mmsi,acqTime,lon,lat FROM (SELECT *, row_number() OVER (PARTITION BY mmsi ORDER BY acqTime ) rank FROM df ) tmp WHERE rank <= 500" )

     df.show(10)
    //println("countShipLineTypeByMajorLineDf")
    //df.show(10,false)

     df.repartition(1)
       .write
       .option("header","true")
       .csv(output)


  }
}

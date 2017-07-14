package com.noce.spark.vistorflow

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.concurrent.duration._
import java.util.Properties
import org.apache.hadoop.fs.{FileSystem, Path}
import Utils.{schema, schemaPlus, emptyStr2Int}

object VistorFlowDur {
// nohup spark-submit \
  // --master yarn-client \
  // --num-executors 30 \
  // --executor-memory 6G \
  // --executor-cores 3 \
  // --driver-memory 4G \
  // --conf spark.default.parallelism=200 \
  // --conf spark.storage.memoryFraction=0.3 \
  // --conf spark.shuffle.memoryFraction=0.2 \
  // --jars mysql-connector-java-5.1.42.jar \
  // --class com.hadoop.spark.team3.VistorFlowDur2 vistorflowdur.jar 3 &> gdgyy.log &

  val dfDate = new SimpleDateFormat("yyyyMMdd")
  val dfHour = new SimpleDateFormat("yyyyMMddHH")
  val dfMinute = new SimpleDateFormat("yyyyMMddHHmm")

  // 使用模式匹配的方法加上udf来分档
  val classify = udf((duration: Int) => {
    val durationPlus = duration + 30 * 60
    durationPlus match {
      case i if i == 30 * 60 => 0
      case i if i > 30 * 60 && i <= 1 * 60 * 60 => 1
      case i if i > 1 * 60 * 60 && i <= 2 * 60 * 60 => 2
      case i if i > 2 * 60 * 60 && i <= 4 * 60 * 60 => 3
      case i if i > 4 * 60 * 60 && i <= 8 * 60 * 60 => 4
      case i if i > 8 * 60 * 60 && i <= 12 * 60 * 60 => 5
      case i if i > 12 * 60 * 60 => 6
    }
  })

  def getLastNTimestamp(cal:Calendar, n:Int) = {
    val lb = new collection.mutable.ListBuffer[String]
    val currentMinute = cal.getTime().getMinutes
    val offsetMinute = -(currentMinute % 5)
    cal.add(Calendar.MINUTE, offsetMinute)
    var last = dfMinute.format(cal.getTime())
    for (i <- 0 to n-1) {
      lb += last
      cal.add(Calendar.MINUTE, -5)
      last = dfMinute.format(cal.getTime())
    }
    lb
  }

  def getUntilNowTimestamp(cal:Calendar) = {
    val lb = new collection.mutable.ListBuffer[String]
    val currentMinute = cal.getTime().getMinutes
    val offsetMinute = -(currentMinute % 5)
    cal.add(Calendar.MINUTE, offsetMinute)
    var last = dfMinute.format(cal.getTime())
    while (last.substring(8, 12) != "0000") {
      lb += last
      cal.add(Calendar.MINUTE, -5)
      last = dfMinute.format(cal.getTime())
    }
    lb
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)

    var lastN = 3
    var currentTimestamp = ""
    var dirpath = "/NOCE"

    if (args.length == 1) {
      lastN = args(0).toInt
    } else if (args.length == 3) {
      lastN = args(0).toInt
      currentTimestamp = args(1)
      dirpath = args(2)
    } else {
      System.err.println("usage: VistorFlowDur2 <n> [currentTimestamp] [dirpath]")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("VistorFlowDur")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext._

    val prop = new Properties()
    prop.put("user", "noce")
    prop.put("password", "123456")
    prop.put("driver", "com.mysql.jdbc.Driver")

    val url="jdbc:mysql://132.122.70.7:3306/noce"

    // 读取景区表
    var dfDimDistrictA = hiveContext.sql("select city_id, base_statn_id, sector_id, district_id, district_name from noce.dim_district_a")
    dfDimDistrictA.cache()

    // 读取居民表
    var dfResidence = hiveContext.sql("select district_id, imsi from noce.RESIDENCE")
    // 此处测试数据
    // var dfResidence = hiveContext.sql("select 1001 as district_id, 'sdfasdfsadfsadfasdf' as imsi")
    dfResidence.cache()
    dfResidence = broadcast(dfResidence)

    def getDFFromTimestamp(timestamp: String) = {
      // 读取文件
      val rdd = sc.textFile(s"$dirpath/AGG/AGG_3G_BSSAP/*/*$timestamp*.txt").map(_.split("\\|")).map(attributes => Row(attributes(0), emptyStr2Int(attributes(1)), attributes(2), attributes(13), attributes(3), attributes(4), emptyStr2Int(attributes(9)), emptyStr2Int(attributes(10)), attributes(5), emptyStr2Int(attributes(6)), emptyStr2Int(attributes(7)), emptyStr2Int(attributes(28))))
      var dfAgg3gBssap = hiveContext.createDataFrame(rdd, schema)
      dfAgg3gBssap
    }

    def filterDF(dfAgg3gBssap: org.apache.spark.sql.DataFrame) = {
      // 过滤DataFrame
      // 根据景区表过滤
      val dfFilteredDistrict = dfAgg3gBssap.join(dfDimDistrictA, dfAgg3gBssap("latn_id") === dfDimDistrictA("city_id") && dfAgg3gBssap("base_statn_id") === dfDimDistrictA("base_statn_id") && dfAgg3gBssap("sector_id") === dfDimDistrictA("sector_id")).drop(dfAgg3gBssap("latn_id")).drop(dfAgg3gBssap("base_statn_id")).drop(dfAgg3gBssap("sector_id"))
      // 根据居民表过滤
      val dfResidenceRenamed = dfResidence.withColumnRenamed("district_id", "district_id_r").withColumnRenamed("imsi", "imsi_r")
      var dfFilteredResidence = dfFilteredDistrict.join(dfResidenceRenamed, dfFilteredDistrict("district_id") === dfResidenceRenamed("district_id_r") && dfFilteredDistrict("imsi") === dfResidenceRenamed("imsi_r"), "left_outer").filter("district_id_r is null").drop("district_id_r").drop("imsi_r")
      dfFilteredResidence
    }

    def getFilteredDF(lastNTimestamp: collection.mutable.ListBuffer[String]) = {
      var newDF = hiveContext.createDataFrame(sc.emptyRDD[Row], schema)
      for (timeStamp5M <- lastNTimestamp) {
        var dfPlus = hiveContext.createDataFrame(sc.emptyRDD[Row], schema)
        try {
          dfPlus = getDFFromTimestamp(timeStamp5M)
          // 不查看是否为空的话，因为是Dataframe，没到action不会触发异常
          dfPlus.rdd.isEmpty()
        } catch {
          case e: Throwable => {
            dfPlus = hiveContext.createDataFrame(sc.emptyRDD[Row], schema)
          }
        } finally {
          newDF = newDF.unionAll(dfPlus)
        }
      }
      var returnDF = hiveContext.createDataFrame(sc.emptyRDD[Row], schemaPlus)
      if (!newDF.rdd.isEmpty()) {
        returnDF = filterDF(newDF)
      }
      returnDF
    }
    // val lastNTimestamp = collection.mutable.ListBuffer("201506070950", "201606070945", "201706070940")
    // getFilteredDF(lastNTimestamp).count()

    def analyseDF(df: org.apache.spark.sql.DataFrame, timestamp: String) = {
      val dfResult = df.groupBy("imsi", "district_id").agg(first("user_number").as("user_number"), first("city_id").as("city_id"), first("district_name").as("district_name"), (unix_timestamp(max("end_time"), "yyyyMMddHHmmss")-unix_timestamp(min("end_time"), "yyyyMMddHHmmss")).as("duration")).withColumn("section_id", classify(col("duration"))).groupBy("district_id", "section_id").agg(first("city_id").as("city_id"), count(col("imsi")).as("vistor_number")).withColumn("timestamp", unix_timestamp(lit(timestamp), "yyyyMMddHHmm").cast(TimestampType)).select("TIMESTAMP", "CITY_ID", "DISTRICT_ID", "SECTION_ID", "VISTOR_NUMBER").withColumn("update_time", current_timestamp())
      // dfResult.show()
      val dfWriter = dfResult.write.mode("append")
      dfWriter.jdbc(url, "VISTOR_FLOW_DUR", prop)
    }

    def analyseFlowDF(df: org.apache.spark.sql.DataFrame, timestamp: String): Unit ={
      val dfResult = df.groupBy("imsi", "district_id").agg(first("user_number").as("user_number"), first("city_id").as("city_id"), first("district_name").as("district_name"), first("home_province").as("home_province"), first("home_latn_id").as("home_city"), (unix_timestamp(lit(timestamp), "yyyyMMddHHmm") - unix_timestamp(max("end_time"), "yyyyMMddHHmmss")).as("duration")).withColumn("VISTOR_NUMBER1",  when(col("duration") <= 30 * 60, 1).otherwise(0)).withColumn("VISTOR_NUMBER2",  when(col("duration") <= 150 * 60, 1).otherwise(0)).withColumn("VISTOR_NUMBER3", when(col("duration") <= 1470 * 60, 1).otherwise(0)).groupBy("district_id","home_province","home_city").agg(first("city_id").as("city_id"), sum(col("VISTOR_NUMBER1")).as("vistor_number1"), sum(col("VISTOR_NUMBER2")).as("vistor_number2"), sum(col("VISTOR_NUMBER3")).as("vistor_number3")).withColumn("timestamp", unix_timestamp(lit(timestamp), "yyyyMMddHHmm").cast(TimestampType)).select("TIMESTAMP", "CITY_ID", "DISTRICT_ID", "HOME_PROVINCE", "HOME_CITY",  "VISTOR_NUMBER1", "VISTOR_NUMBER2", "VISTOR_NUMBER3" ).withColumn("update_time", current_timestamp())
      // dfResult.show()
      val dfWriter = dfResult.write.mode("append")
      dfWriter.jdbc(url, "VISTOR_FLOW_HOME", prop)
    }

    // 测试数据
    // lastN = 3
    // currentTimestamp = "201707040017"

    val cal:Calendar = Calendar.getInstance()
    if (currentTimestamp != "") cal.setTime(dfMinute.parse(currentTimestamp))
    val currentTime = cal.getTime()
    val currentMinutes = currentTime.getHours() * 60 + currentTime.getMinutes()
    var untilNowDF = hiveContext.createDataFrame(sc.emptyRDD[Row], schemaPlus)
    // 如果lastN == 0的话，读取迄今为止的所有数据，则不用读缓存表，并刷新缓存表
    // 如果当前时间距0点的分钟数落在，[lastN * 5 * 1, lastN * 5 * 2）这个区间的话，说明需要刷新缓存表
    if (lastN != 0 && !(currentMinutes >= lastN * 5 * 1 && currentMinutes < lastN * 5 *2)) {
      try {
        untilNowDF = hiveContext.read.format("ORC").load(s"$dirpath/TMP/AGG_3G_BSSAP_15M_DISTRICT/cache_file.orc")
      } catch {
        case e: Throwable => untilNowDF = hiveContext.createDataFrame(sc.emptyRDD[Row], schemaPlus)
      }
    }
    val lastNTimestamp = if (lastN != 0) getLastNTimestamp(cal, lastN) else getUntilNowTimestamp(cal)
    val df = untilNowDF.unionAll(getFilteredDF(lastNTimestamp))
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    analyseDF(df, lastNTimestamp(0))
    analyseFlowDF(df, lastNTimestamp(0))

    val dfWriter = df.write.mode("overwrite").format("ORC")
    dfWriter.save(s"$dirpath/TMP/AGG_3G_BSSAP_15M_DISTRICT/cache_file_new.orc")

    val fs = FileSystem.get(sc.hadoopConfiguration)
    // 删除上一次的缓存文件，并且将这一次的缓存文件命名为cache_file.orc
    try {
      fs.delete(new Path(s"$dirpath/TMP/AGG_3G_BSSAP_15M_DISTRICT/cache_file.orc"))
    } catch {
      case e: Throwable => {
        println(s"原缓存文件 $dirpath/TMP/AGG_3G_BSSAP_15M_DISTRICT/cache_file.orc 删除异常")
      }
    } finally {
      fs.rename(new Path(s"$dirpath/TMP/AGG_3G_BSSAP_15M_DISTRICT/cache_file_new.orc"), new Path(s"$dirpath/TMP/AGG_3G_BSSAP_15M_DISTRICT/cache_file.orc"))
    }

    df.unpersist()
    sc.stop()
  }
}

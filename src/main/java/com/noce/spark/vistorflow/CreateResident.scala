package com.noce.spark.vistorflow

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.log4j.Level
import org.apache.log4j.Logger
import collection.mutable.ListBuffer
import Utils.schema
import Utils.emptyStr2Int

object CreateResident {
  // spark-submit --class com.hadoop.spark.team3.CreateResident createresident.jar 5 2017062523

  val dfDate = new SimpleDateFormat("yyyyMMdd")
  val dfHour = new SimpleDateFormat("yyyyMMddHH")
  val dfMinute = new SimpleDateFormat("yyyyMMddHHmm")

  def getDefaultStartTime():String={
    val cal:Calendar = Calendar.getInstance()
    cal.setFirstDayOfWeek(Calendar.MONDAY)//将每周第一天设为星期一，默认是星期天
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    cal.add(Calendar.DATE, -1*8)
    val startTime = dfDate.format(cal.getTime())+"23"
    startTime
  }

  // def getTimeRangeStr():String={
    // val cal:Calendar = Calendar.getInstance()
    // cal.setFirstDayOfWeek(Calendar.MONDAY)//将每周第一天设为星期一，默认是星期天
    // cal.add(Calendar.DATE, -1*7)
    // cal.set(Calendar.DAY_OF_WEEK, Calendar.FRIDAY)
    // val friday = dfDate.format(cal.getTime())+"23"
    // cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    // cal.add(Calendar.DATE, -1)
    // val monday = dfDate.format(cal.getTime())+"23"
    // s"hour >= $monday AND hour < $friday"
  // }

  def getTimeRange(startTime:String, stayDates:Int):ListBuffer[String]={
    val lb = new ListBuffer[String]
    lb += startTime
    val cal:Calendar = Calendar.getInstance()
    cal.setTime(dfHour.parse(startTime))
    for (i <- 1 to stayDates) {
      cal.add(Calendar.DATE, 1)
      lb += dfDate.format(cal.getTime())
    }
    lb
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    var stayDates = 5
    var startTime = ""
    var dirpath = "/NOCE"

    if (args.length == 1) {
      stayDates = args(0).toInt
    } else if (args.length == 3) {
      stayDates = args(0).toInt
      startTime = args(1)
      dirpath = args(2)
    } else {
      System.err.println("usage: CreateResident <stayDates> [start_time] [dirpath]")
      System.exit(1)
    }

    if (startTime == "") startTime = getDefaultStartTime()

    val conf = new SparkConf().setAppName("CreateResident")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext._

    def getDFFromTimestamp(timestamp: String) = {
      // 读取文件
      val rdd = sc.textFile(s"$dirpath/AGG/AGG_3G_BSSAP/*/*$timestamp*.txt").map(_.split("\\|")).map(attributes => Row(attributes(0), emptyStr2Int(attributes(1)), attributes(2), attributes(13), attributes(3), attributes(4), emptyStr2Int(attributes(9)), emptyStr2Int(attributes(10)), attributes(5), emptyStr2Int(attributes(6)), emptyStr2Int(attributes(7)), emptyStr2Int(attributes(28))))
      var dfAgg3gBssap = hiveContext.createDataFrame(rdd, schema)
      dfAgg3gBssap
    }

    def getDFFromTimeRange(lastNTimestamp: collection.mutable.ListBuffer[String]) = {
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
      newDF
    }

    val dfDimDistrictA = hiveContext.sql("select * from noce.dim_district_a").select("city_id", "base_statn_id", "sector_id", "district_id", "district_name")
    dfDimDistrictA.cache()

    val timeRange = getTimeRange(startTime, stayDates)
    val dfRaw = getDFFromTimeRange(timeRange)
    var dfAgg3gBssap = dfRaw.select("imsi", "user_number", "latn_id", "base_statn_id", "sector_id", "end_time", "hour").filter("latn_id = 200").withColumn("date", substring(col("hour"), 0, 8)).withColumn("hour", substring(col("hour"), 9, 2).cast(IntegerType)).withColumn("date", unix_timestamp(col("date"), "yyyyMMdd"))

    // var filterStr = ""
    // startTime = "2017062523"

    val dfPeople = dfAgg3gBssap.join(dfDimDistrictA, dfAgg3gBssap("latn_id")  === dfDimDistrictA("city_id") && dfAgg3gBssap("base_statn_id") === dfDimDistrictA("base_statn_id") && dfAgg3gBssap("sector_id")  === dfDimDistrictA("sector_id")).drop(dfAgg3gBssap("latn_id")).drop(dfAgg3gBssap("base_statn_id")).drop(dfAgg3gBssap("sector_id"))

    // 使用when otherwise的方法来分档


    // 区分居民
    // val dfResidents = dfPeople.filter("hour >= 23 OR hour <= 6").groupBy("imsi", "date", "district_id").agg(first("user_number").as("user_number"), first("city_id").as("city_id"), first("district_name").as("district_name"), (unix_timestamp(max("end_time"), "yyyyMMddHHmmss")-unix_timestamp(min("end_time"), "yyyyMMddHHmmss")).as("duration")).withColumn("is_stay", when(col("duration") >= 4, 1).otherwise(0)).filter("is_stay = 1").groupBy("imsi", "district_id").agg(first("user_number").as("user_number"), first("city_id").as("city_id"), first("district_name").as("district_name"), count("is_stay").as("stay_days")).withColumn("user_attr", when(col("stay_days").equalTo(stayDates), 1).otherwise(0)).filter("user_attr = 1").withColumn("update_time", current_timestamp()).select("UPDATE_TIME","IMSI","User_Number","CITY_ID","DISTRICT_ID","DISTRICT_NAME","USER_ATTR")
    val dfResidents = dfPeople.filter("hour >= 23 OR hour <= 6").withColumn("date", when(col("hour").equalTo(23), col("date") + 24*60*60).otherwise(col("date"))).groupBy("imsi", "date", "district_id").agg(first("user_number").as("user_number"), first("city_id").as("city_id"), first("district_name").as("district_name"), (unix_timestamp(max("end_time"), "yyyyMMddHHmmss")-unix_timestamp(min("end_time"), "yyyyMMddHHmmss")).as("duration")).withColumn("is_stay", when(col("duration") >= 4, 1).otherwise(0)).filter("is_stay = 1").groupBy("imsi", "district_id").agg(first("user_number").as("user_number"), first("city_id").as("city_id"), first("district_name").as("district_name"), count("is_stay").as("stay_days")).withColumn("user_attr", when(col("stay_days").equalTo(stayDates), 1).otherwise(0)).filter("user_attr = 1").withColumn("update_time", current_timestamp()).select("UPDATE_TIME","IMSI","User_Number","CITY_ID","DISTRICT_ID","DISTRICT_NAME","USER_ATTR")
    dfResidents.write.mode("overwrite").format("ORC").saveAsTable("noce.RESIDENCE")
    // 区分工作人员
    // val dfStaff = dfPeople.filter("hour >= 8 AND hour <= 17").groupBy("imsi", "date", "district_id").agg(first("user_number").as("user_number"), first("city_id").as("city_id"), first("district_name").as("district_name"), (unix_timestamp(max("end_time"), "yyyyMMddHHmmss")-unix_timestamp(min("end_time"), "yyyyMMddHHmmss")).as("duration")).withColumn("is_stay", when(col("duration") >= 4, 1).otherwise(0)).filter("is_stay = 1").groupBy("imsi", "district_id").agg(first("user_number").as("user_number"), first("city_id").as("city_id"), first("district_name").as("district_name"), count("is_stay").as("stay_days")).withColumn("user_attr", when(col("stay_days").equalTo(stayDates), 2).otherwise(0)).filter("user_attr = 2").withColumn("update_time", current_timestamp()).select("UPDATE_TIME","IMSI","User_Number","CITY_ID","DISTRICT_ID","DISTRICT_NAME","USER_ATTR")
    val dfStaff = dfPeople.filter("hour >= 8 AND hour <= 17").withColumn("date", when(col("hour").equalTo(23), col("date") + 24*60*60).otherwise(col("date"))).groupBy("imsi", "date", "district_id").agg(first("user_number").as("user_number"), first("city_id").as("city_id"), first("district_name").as("district_name"), (unix_timestamp(max("end_time"), "yyyyMMddHHmmss")-unix_timestamp(min("end_time"), "yyyyMMddHHmmss")).as("duration")).withColumn("is_stay", when(col("duration") >= 4, 1).otherwise(0)).filter("is_stay = 1").groupBy("imsi", "district_id").agg(first("user_number").as("user_number"), first("city_id").as("city_id"), first("district_name").as("district_name"), count("is_stay").as("stay_days")).withColumn("user_attr", when(col("stay_days").equalTo(stayDates), 2).otherwise(0)).filter("user_attr = 2").withColumn("update_time", current_timestamp()).select("UPDATE_TIME","IMSI","User_Number","CITY_ID","DISTRICT_ID","DISTRICT_NAME","USER_ATTR")
    dfStaff.write.mode("append").format("ORC").saveAsTable("noce.RESIDENCE")

    // 使用模式匹配的方法加上udf来分档
    // import org.apache.spark.sql.functions.udf
    // val isStay = udf((duration: Int) => {
      // duration match {
        // case i if i >= 4 * 60 * 60 => 1
        // case i if i < 4 * 60 * 60 => 0
      // }
    // })

    // val dfResidents = dfPeople.filter("hour >= 23 OR hour <= 6").groupBy("imsi", "date", "district_id").agg(first("user_number").as("user_number"), first("city_id").as("city_id"), first("district_name").as("district_name"), (unix_timestamp(max("end_time"), "yyyyMMddHHmmss")-unix_timestamp(min("end_time"), "yyyyMMddHHmmss")).as("duration")).withColumn("is_stay", isStay(col("duration"))).filter("is_stay = 1").groupBy("imsi", "district_id").agg(first("user_number").as("user_number"), first("city_id").as("city_id"), first("district_name").as("district_name"), count("is_stay").as("stay_days")).withColumn("user_attr", when(col("stay_days").equalTo(1), 1))


    // val dfDimSector = hiveContext.sql("select city_id, area_id, base_statn_id, sector_id, first(city_name), first(area_name), first(base_statn_name), first(sector_name) from noce.dim_sector where city_id=200 group by city_id, area_id, base_statn_id, sector_id order by city_id, sector_id, area_id, base_statn_id")
    // val dfDimSector1 = hiveContext.sql("select city_id, base_statn_id, sector_id, count(*) from noce.dim_sector where city_id=200 group by city_id, base_statn_id, sector_id order by city_id, base_statn_id, sector_id ")

    // var foo = dfResidents.groupBy("imsi", "date", "district_id").agg(first("hour"), first("user_number"), countDistinct("hour").as("cd_hour"), concat_ws(";", collect_set("hour")))
    // var foo = dfResidents.groupBy("imsi", "date", "district_id").agg(first("hour"), first("user_number"), countDistinct("hour").as("cd_hour"), concat_ws(";", collect_set(dfResidents("hour").cast(StringType))))
    // var foo = hiveContext.sql("select latn_id, base_statn_id, sector_id, count(*) from noce.agg_3g_bssap where latn_id=200 and hour=2017053018 group by latn_id, base_statn_id, sector_id order by latn_id, base_statn_id, sector_id")
    sc.stop()

  }
}

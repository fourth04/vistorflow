package com.noce.spark.vistorflow

import org.apache.spark.sql.types._

object Utils {
  val schema = StructType(
    List(
      StructField("message_type", StringType, nullable=true),
      StructField("type", IntegerType, nullable=true),
      StructField("imsi", StringType, nullable=true),
      StructField("user_number", StringType, nullable=true),
      StructField("start_time", StringType, nullable=true),
      StructField("end_time", StringType, nullable=true),
      StructField("base_statn_id", IntegerType, nullable=true),
      StructField("sector_id", IntegerType, nullable=true),
      StructField("home_province", StringType, nullable=true),
      StructField("home_latn_id", IntegerType, nullable=true),
      StructField("latn_id", IntegerType, nullable=true),
      StructField("hour", IntegerType, nullable=true)
    )
  )

  val schemaPlus = StructType(
    List(
      StructField("message_type", StringType, nullable=true),
      StructField("type", IntegerType, nullable=true),
      StructField("imsi", StringType, nullable=true),
      StructField("user_number", StringType, nullable=true),
      StructField("start_time", StringType, nullable=true),
      StructField("end_time", StringType, nullable=true),
      StructField("home_province", StringType, nullable=true),
      StructField("home_latn_id", IntegerType, nullable=true),
      StructField("hour", IntegerType, nullable=true),
      StructField("city_id", IntegerType, nullable=true),
      StructField("base_statn_id", IntegerType, nullable=true),
      StructField("sector_id", IntegerType, nullable=true),
      StructField("district_id", IntegerType, nullable=true),
      StructField("district_name", StringType, nullable=true)
    )
  )

  def emptyStr2Int(myString:String) = {
    val trimString = myString.trim
    if (trimString.isEmpty() || trimString == "null" || trimString == "\\N") {
      0
    } else {
      trimString.toInt
    }
  }
}

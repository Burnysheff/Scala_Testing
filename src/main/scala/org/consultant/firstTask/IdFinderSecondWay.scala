package org.consultant.firstTask

import org.apache.spark.sql.SparkSession

object IdFinderSecondWay {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Java Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate();

    val df = spark.sparkContext.textFile("Sessions/*").collect()

    val sec = df.filter(df => df.contains("$0 ACC_45616"))

    reflect.io.File("Results/ResultFirst/resultFirst_2").writeAll(sec.length.toString)

    spark.close()
  }
}

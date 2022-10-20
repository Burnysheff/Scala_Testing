package org.consultant.firstTask

import org.apache.spark.sql.SparkSession

object IdFinderFirstWay {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Java Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate();

    val df = spark.sparkContext.wholeTextFiles("Sessions/*")

    val sec = df.map(df => df._2).collect()

    val res = sec.filter(sec => sec.contains("$0 ACC_45616"))

    reflect.io.File("Results/ResultFirst/resultFirst_1").writeAll(res.length.toString)

    spark.close()
  }
}

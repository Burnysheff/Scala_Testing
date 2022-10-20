package org.consultant.secondTask

import org.apache.spark.sql.SparkSession

object OpenFilesDays {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Java Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate();

    val df = spark.sparkContext.textFile("Sessions/*").collect()

    val pro = df.filter(df => df.contains("DOC_OPEN"))

    val pi = pro.map(pro => pro.split(" "))

    val dates = pi.map(pi => (pi.apply(1), pi.apply(3)))

    val fixed = dates.map(dates => (dates._1.split("_"), dates._2))

    val fix = fixed.map(fixed => (fixed._1.apply(0), fixed._2))

    val groups = fix.groupBy(fix => fix._1)

    val group = groups.map(groups => (groups._1, groups._2.map(gr => gr._2)))

    group.foreach(f =>
      reflect.io.File("Results/ResultSecond/resultSecond_Full").appendAll(f._1, " : ",  f._2.mkString("Array(", ", ", ")"), "\n")
    )

    val res = group.map(gr => (gr._1, gr._2.length))

    res.foreach(f =>
      reflect.io.File("Results/ResultSecond/resultSecond_Result").appendAll(f._1, " : ", f._2.toString, "\n")
    )

    val grg = group.map(groups => (groups._1, groups._2.groupBy(gr => gr)))

    val resa = grg.map(g => (g._1, g._2.map(q => (q._1, q._2.length))))

    resa.foreach(f =>
      reflect.io.File("Results/ResultSecond/resultSecondGrouped").appendAll(f._1, " : ", f._2.toString(), "\n")
    )

    spark.close()
  }
}

package com.alexpyc

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.benchmark._
import org.apache.spark.sql.SparkSession

object InsecureQ3 {
  def dataDir: String = {
    if (System.getenv("SPARKSGX_DATA_DIR") == null) {
      throw new Exception("Set SPARKSGX_DATA_DIR")
    }
    System.getenv("SPARKSGX_DATA_DIR")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("InsecureQ3")
      .getOrCreate()
    Utils.initSQLContext(spark.sqlContext)

    val numPartitions = args(0)
    val sizeLevel = args(1).toInt

    val size = math.pow(10, i).toInt.toString
    BigDataBenchmark.q3(spark, Insecure, size, numPartitions)

    spark.stop()
  }
}

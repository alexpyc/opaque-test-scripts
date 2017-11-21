package com.alexpyc

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.benchmark._
import org.apache.spark.sql.SparkSession

object ObliviousPR {
  def dataDir: String = {
    if (System.getenv("SPARKSGX_DATA_DIR") == null) {
      throw new Exception("Set SPARKSGX_DATA_DIR")
    }
    System.getenv("SPARKSGX_DATA_DIR")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("ObliviousPR")
      .getOrCreate()
    Utils.initSQLContext(spark.sqlContext)

    val numPartitions = args(0).toInt
    val sizeLevel = args(1).toInt

    PageRank.run(spark, Oblivious, math.pow(2, sizeLevel).toInt.toString, numPartitions)

    spark.stop()
  }
}

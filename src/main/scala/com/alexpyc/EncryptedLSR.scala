package com.alexpyc

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.benchmark._
import org.apache.spark.sql.SparkSession

object EncryptedLSR {
  def dataDir: String = {
    if (System.getenv("SPARKSGX_DATA_DIR") == null) {
      throw new Exception("Set SPARKSGX_DATA_DIR")
    }
    System.getenv("SPARKSGX_DATA_DIR")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("EncryptedLSR")
      .getOrCreate()
    Utils.initSQLContext(spark.sqlContext)

    val numPartitions = args(0)
    val sizeLevel = args(1).toInt

    val size = math.pow(10, i).toInt.toString
    LeastSquaresBenchmark.query(spark, Encrypted, size, numPartitions)

    spark.stop()
  }
}
package com.alexpyc

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.benchmark._
import org.apache.spark.sql.SparkSession

object JoinGene {
  def dataDir: String = {
    if (System.getenv("SPARKSGX_DATA_DIR") == null) {
      throw new Exception("Set SPARKSGX_DATA_DIR")
    }
    System.getenv("SPARKSGX_DATA_DIR")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("JoinGene")
      .getOrCreate()
    Utils.initSQLContext(spark.sqlContext)

    val numPartitions = args(0).toInt
    val sizeLevel = args(1).toInt

    JoinReordering.geneQuery(spark, (math.pow(2, sizeLevel) * 125).toInt.toString, numPartitions)

    spark.stop()
  }
}

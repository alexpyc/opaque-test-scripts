package com.alexpyc

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.benchmark._
import org.apache.spark.sql.SparkSession

object Tests {
  def dataDir: String = {
    if (System.getenv("SPARKSGX_DATA_DIR") == null) {
      throw new Exception("Set SPARKSGX_DATA_DIR")
    }
    System.getenv("SPARKSGX_DATA_DIR")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Tests")
      .getOrCreate()
    Utils.initSQLContext(spark.sqlContext)

    val numPartitions = arg(0).toInt

    // BDB
    for (i <- 3 to 6) {
        val size = math.pow(10, i).toInt.toString

        BigDataBenchmark.q1(spark, Insecure, size, numPartitions)
        BigDataBenchmark.q1(spark, Encrypted, size, numPartitions)
        BigDataBenchmark.q1(spark, Oblivious, size, numPartitions)

        BigDataBenchmark.q2(spark, Insecure, size, numPartitions)
        BigDataBenchmark.q2(spark, Encrypted, size, numPartitions)
        BigDataBenchmark.q2(spark, Oblivious, size, numPartitions)

        BigDataBenchmark.q3(spark, Insecure, size, numPartitions)
        BigDataBenchmark.q3(spark, Encrypted, size, numPartitions)
        BigDataBenchmark.q3(spark, Oblivious, size, numPartitions)
    }

    // LSR
    for (i <- 3 to 6) {
        val size = math.pow(10, i).toInt.toString

        LeastSquaresBenchmark.query(spark, Insecure, size, numPartitions)
        LeastSquaresBenchmark.query(spark, Encrypted, size, numPartitions)
        LeastSquaresBenchmark.query(spark, Oblivious, size, numPartitions)
    }


    // PR
    for (i <- 8 to 20) {
        PageRank.run(spark, Insecure, math.pow(2, i).toInt.toString, numPartitions)
        PageRank.run(spark, Encrypted, math.pow(2, i).toInt.toString, numPartitions)
        PageRank.run(spark, Oblivious, math.pow(2, i).toInt.toString, numPartitions)
    }

    // Join
    for (i <- 0 to 13) {
        JoinReordering.treatmentQuery(spark, (math.pow(2, i) * 125).toInt.toString, numPartitions)
        JoinReordering.geneQuery(spark, (math.pow(2, i) * 125).toInt.toString, numPartitions)
    }

    for (i <- 0 to 13) {
        JoinCost.run(spark, Insecure, (math.pow(2, i) * 125).toInt.toString, numPartitions)
        JoinCost.run(spark, Encrypted, (math.pow(2, i) * 125).toInt.toString, numPartitions)
        JoinCost.run(spark, Oblivious, (math.pow(2, i) * 125).toInt.toString, numPartitions)
    }

    Thread.sleep(100000000)

    spark.stop()
  }
}

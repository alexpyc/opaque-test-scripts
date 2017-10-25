package com.alexpyc.tests

import org.apache.spark.sql.SparkSession
import scala.math.random

object Pi {
    def main (args: Array[String]) {
        if (args.length < 1){
          System.exit(1)
        }

        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("Pi").getOrCreate()
        val sc = spark.sparkContext
        import spark.implicits._

        val n = args(0).toInt
        val df = sc.parallelize(1 to n).toDF

        val count = df.map( n => (random, random) ).toDF.map( d => d.getDouble(0)*d.getDouble(0)+d.getDouble(1)*d.getDouble(1) ).filter($"value" < 1).count()
        val pi = 4.0 * count / n
        println(s"Pi is roughly ${pi}")

        spark.stop()
    }
}

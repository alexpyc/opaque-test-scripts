package com.alexpyc.tests.vanilla

import org.apache.spark.sql.SparkSession
import scala.math.random

object Pi {
    def power (x: Double) : Double = {
        return x * x
    }

    def main (args: Array[String]) {
        if (args.length < 1){
          System.exit(1)
        }

        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("Pi").getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext

        val n = args(0).toInt
        val data = sc.parallelize( (1 to n).map(x => power(random)+power(random)) )
        val df = data.toDF
        df.cache()

        val count = df.filter($"value" < 1).count()
        val pi = 4.0 * count / n
        println(s"Pi is roughly ${pi}")

        spark.stop()
    }
}

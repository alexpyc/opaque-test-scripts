package com.alexpyc.tests.encryption

import org.apache.spark.sql.SparkSession
import edu.berkeley.cs.rise.opaque.implicits._
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
        edu.berkeley.cs.rise.opaque.Utils.initSQLContext(spark.sqlContext)
        val sc = spark.sparkContext

        val n = args(0).toInt
        val data = sc.parallelize( (1 to n).map(x => power(random)+power(random)) )
        val df = data.toDF.encrypted
        df.cache()

        val count = df.filter($"value" < 1).count()
        val pi = 4.0 * count / n
        println(s"Pi is roughly ${pi}")

        spark.stop()
    }
}

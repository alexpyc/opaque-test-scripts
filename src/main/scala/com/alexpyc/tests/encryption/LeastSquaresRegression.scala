package com.alexpyc.tests.encryption

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import edu.berkeley.cs.rise.opaque.implicits._
import org.apache.spark.sql.functions._

object LeastSquaresRegression {

    def main (args: Array[String]) {
        if (args.length < 1){
          System.exit(1)
        }

        val address = args(0)
        val input_path = address + "/Input"
        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("LeastSquaresRegression").getOrCreate()
        edu.berkeley.cs.rise.opaque.Utils.initSQLContext(spark.sqlContext)
        val sc = spark.sparkContext
        import spark.implicits._

        val dataRDD: RDD[org.apache.spark.mllib.regression.LabeledPoint] = sc.objectFile(input_path)
        val dataDS = dataRDD.map(x => (x.label, x.features(0)))
        val data = dataDS.toDS
        val newNames = Seq("x", "y")
        val df = data.toDF(newNames: _*).encrypted
        df.cache()

        val xbar = df.select("x").groupBy().avg("x").first.getDouble(0)
        val ybar = df.select("y").groupBy().avg("y").first.getDouble(0)
        val slope = df.select(($"x" - xbar).as("n1"), ($"y" - ybar).as("n2"))
            .select(($"n1" * $"n2").as("n"), ($"n1" * $"n1").as("d"))
            .groupBy()
            .agg(sum($"n").alias("n"), sum($"d").alias("d")).select($"n" / $"d").first.getDouble(0)
        val intercept = ybar - xbar * slope

        println(s"Slope: ${slope}; Intercept: ${intercept}")

        spark.stop()
    }
}

package com.alexpyc.tests

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

object LeastSquaresRegression {

    def main (args: Array[String]) {
        if (args.length < 1){
          System.exit(1)
        }

        val address = args(0)
        val input_path = address + "/Input"
        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("LeastSquaresRegression").getOrCreate()
        val sc = spark.sparkContext
        import spark.implicits._

        val dataRDD: RDD[org.apache.spark.mllib.regression.LabeledPoint] = sc.objectFile(input_path)
        val dataDS = dataRDD.map(x => (x.label, x.features(0)))
        val data = dataDS.toDS
        val newNames = Seq("x", "y")
        val df = data.toDF(newNames: _*)
        df.createOrReplaceTempView("data")

        spark.sql("""
            SELECT slope, ybar - xbar * slope as intercept
            FROM (
                SELECT sum((x - xbar) * (y - ybar)) / sum((x - xbar) * (x - xbar)) as slope, max(ybar) as ybar, max(xbar) as xbar
                FROM (
                    SELECT avg(y) over(rows between unbounded preceding and unbounded following) as ybar, y, avg(x) over(rows between unbounded preceding and unbounded following) as xbar, x
                    FROM data
                )
            )
        """).write.option("header", "true").csv("hdfs://localhost:9000/HiBench/Linear/Output")

        spark.stop()
    }
}

package com.alexpyc.tests

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.sql.functions.{explode, sum, split}

object WordCount {

    def main (args: Array[String]) {
        if (args.length < 1){
          System.exit(1)
        }

        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("WordCount").getOrCreate()
        val sc = spark.sparkContext
        import spark.implicits._

        val address = args(0)
        val input_path = address + "/Input"
        val df = sc.sequenceFile[NullWritable, Text](input_path).map(_._2.toString).toDF("line")
        val result = df.withColumn("line", split($"line","\\s+")).withColumn("word", explode($"line")).drop("line").map( word => (word.getString(0), 1)).toDF("word", "count").groupBy($"word").agg(sum($"count"))

        result.write.option("header", "true").csv(s"${address}/Output")

        spark.stop()
    }
}

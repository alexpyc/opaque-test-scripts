package com.alexpyc.tests.oblivious

import org.apache.spark.sql.SparkSession
import edu.berkeley.cs.rise.opaque.implicits._
import org.apache.spark.sql.functions._

object WordCount {

    def main (args: Array[String]) {
        if (args.length < 1){
          System.exit(1)
        }

        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("WordCount").getOrCreate()
        import spark.implicits._
        edu.berkeley.cs.rise.opaque.Utils.initSQLContext(spark.sqlContext)
        val sc = spark.sparkContext

        val address = args(0)
        val input_path = address + "/Input"
        val data = sc.textFile(input_path)
        val df = data.toDF.withColumn("value", split($"value", "\\s+")).withColumn("word", explode($"value")).drop($"value").withColumn("count", lit(1)).oblivious
        df.cache()

        val result = df.groupBy($"word").agg(sum($"count"))
        result.show()
        result.write.option("header", "true").csv(s"${address}/Output")
        spark.stop()
    }
}

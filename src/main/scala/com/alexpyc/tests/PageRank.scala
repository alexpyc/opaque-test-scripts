package com.alexpyc.tests

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, col, size, sum, split}

object PageRank {

    def main (args: Array[String]) {
        if (args.length < 1){
          System.exit(1)
        }

        val address = args(0)
        val input_path = address + "/Input/edges"
        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("PageRank").getOrCreate()
        import spark.implicits._

        val lines = spark.read.text(input_path).as[String]
        val links = lines.map{ s =>
            val elements = s.split("\\s+")
            val parts = elements.slice(elements.length - 2, elements.length)
            (parts(0), parts(1))
        }.distinct().rdd.groupByKey().map( x => (x._1, x._2.toSeq.distinct)).toDF("url", "urls")
        var ranks = links.map(v => (v.getString(0), 1.0)).toDF("url", "rank")

        for (i <- 1 to 10) {
            ranks = links.join(ranks, Seq("url")).withColumn("rank", (col("rank") / size($"urls"))).withColumn("url", explode($"urls")).drop($"urls").groupBy($"url").agg(sum($"rank").alias("rank"))
        }

        ranks.write.option("header", "true").csv("hdfs://localhost:9000/HiBench/Pagerank/Output")

        spark.stop()
    }
}

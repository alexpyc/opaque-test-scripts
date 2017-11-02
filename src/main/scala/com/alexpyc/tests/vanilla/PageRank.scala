package com.alexpyc.tests.vanilla

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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
        val data = lines.map{ s =>
            val elements = s.split("\\s+")
            val parts = elements.slice(elements.length - 2, elements.length)
            (parts(0), parts(1))
        }.distinct().rdd
        val edges = data.toDF("src", "dst").withColumn("weight", lit(1.0f))
        edges.cache()
        val vertices = data.toDF("src", "dst").select($"src", lit(1.0f).as("rank")).distinct
        vertices.cache()

        val result = vertices.join(edges, Seq("src"))
            .select($"dst", ($"rank" * $"weight").as("weightedRank"))
            .groupBy("dst").agg(sum("weightedRank").as("totalIncomingRank"))
            .select($"dst", (lit(0.15) + lit(0.85) * $"totalIncomingRank").as("rank"))
        result.show()
        result.write.option("header", "true").csv(s"${address}/Output")

        spark.stop()
    }
}

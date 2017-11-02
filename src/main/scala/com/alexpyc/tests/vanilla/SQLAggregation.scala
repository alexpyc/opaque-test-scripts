package com.alexpyc.tests.vanilla

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SQLAggregation {
    def main (args: Array[String]) {
        if (args.length < 1){
          System.exit(1)
        }

        val address = args(0)
        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("SQLAggregation").getOrCreate()
        import spark.implicits._

        spark.sql("DROP TABLE IF EXISTS uservisits")
        spark.sql(s"""
            CREATE EXTERNAL TABLE uservisits (
                sourceIP STRING,
                destURL STRING,
                visitDate STRING,
                adRevenue DOUBLE,
                userAgent STRING,
                countryCode STRING,
                languageCode STRING,
                searchWord STRING,
                duration INT
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            STORED AS SEQUENCEFILE LOCATION '${address}/Input/uservisits'
        """)
        val df = spark.sql("SELECT * FROM uservisits")
        df.cache()

        val result = df.select(substring($"sourceIP", 1, 7).as("sourceIP"), $"adRevenue")
            .groupBy($"sourceIP")
            .agg(sum($"adRevenue").alias("sumAdRevenue"))
        result.show()
        result.write.option("header", "true").csv(s"${address}/Output/uservisits_aggregation")

        spark.stop()
    }
}

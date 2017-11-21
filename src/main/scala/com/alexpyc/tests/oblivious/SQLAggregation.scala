package com.alexpyc.tests.oblivious

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import edu.berkeley.cs.rise.opaque.implicits._

object SQLAggregation {
    def main (args: Array[String]) {
        if (args.length < 1){
          System.exit(1)
        }

        val address = args(0)
        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("SQLAggregation").getOrCreate()
        edu.berkeley.cs.rise.opaque.Utils.initSQLContext(spark.sqlContext)
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
        val df = spark.sql("SELECT * FROM uservisits").oblivious
        df.cache()

        val result = df.select(substring($"sourceIP", 0, 7).as("sourceIP"), $"adRevenue")
            .groupBy($"sourceIP")
            .agg(sum($"adRevenue").alias("sumAdRevenue"))
        result.show()
        result.write.option("header", "true").csv(s"${address}/Output/uservisits_aggregation")

        spark.stop()
    }
}

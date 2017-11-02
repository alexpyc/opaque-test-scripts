package com.alexpyc.tests.encryption

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import edu.berkeley.cs.rise.opaque.implicits._

object SQLJoin {
    def main (args: Array[String]) {
        if (args.length < 1){
          System.exit(1)
        }

        val address = args(0)
        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("SQLJoin").getOrCreate()
        edu.berkeley.cs.rise.opaque.Utils.initSQLContext(spark.sqlContext)
        import spark.implicits._

        spark.sql("DROP TABLE IF EXISTS rankings")
        spark.sql(s"""
            CREATE EXTERNAL TABLE rankings (
                pageURL STRING,
                pageRank INT,
                avgDuration INT
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            STORED AS SEQUENCEFILE LOCATION '${address}/Input/rankings'
        """)
        val dfR = spark.sql("SELECT * FROM rankings").encrypted
        dfR.cache()
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
        val dfUV = spark.sql("SELECT * FROM uservisits").encrypted
        dfUV.cache()

        val result = dfR.join(dfUV, dfR("pageURL") === dfUV("destURL"))
            .filter($"visitDate" >= "2000-01-01")
            .filter($"visitDate" <= "2000-12-31")
            .groupBy($"sourceIP")
            .agg(avg($"pageRank").as("avgPageRank"), sum($"adRevenue").as("totalRevenue"))
            .select($"sourceIP", $"avgPageRank", $"totalRevenue")
            .sort($"totalRevenue".desc)
            .select($"sourceIP", $"avgPageRank", $"totalRevenue")
        result.show()
        result.write.option("header", "true").csv(s"${address}/Output/rankings_uservisits_join")

        spark.stop()
    }
}

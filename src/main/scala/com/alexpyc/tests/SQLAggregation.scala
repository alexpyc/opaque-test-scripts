package com.alexpyc.tests

import org.apache.spark.sql.SparkSession
//import edu.berkeley.cs.rise.opaque.implicits._

object SQLAggregation {
    def main (args: Array[String]) {
        if (args.length < 1){
          System.exit(1)
        }

        val address = args(0)
        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("SQLAggregation").getOrCreate()

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
        spark.sql("DROP TABLE IF EXISTS uservisits_aggregation")
        spark.sql(s"""
            CREATE EXTERNAL TABLE uservisits_aggregation (
                sourceIP STRING,
                sumAdRevenue DOUBLE
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            STORED AS SEQUENCEFILE LOCATION '${address}/Output/uservisits_aggregation'
        """)
        spark.sql("""
            INSERT OVERWRITE TABLE uservisits_aggregation
            SELECT SUBSTR(sourceIP, 1, 7), SUM(adRevenue)
            FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 7)
        """)

        spark.stop()
    }
}

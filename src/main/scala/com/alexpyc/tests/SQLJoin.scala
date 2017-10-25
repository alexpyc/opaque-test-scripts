package com.alexpyc.tests

import org.apache.spark.sql.SparkSession
//import edu.berkeley.cs.rise.opaque.implicits._

object SQLJoin {
    def main (args: Array[String]) {
        if (args.length < 1){
          System.exit(1)
        }

        val address = args(0)
        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("SQLJoin").getOrCreate()

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
        spark.sql("DROP TABLE IF EXISTS rankings_uservisits_join")
        spark.sql(s"""
            CREATE EXTERNAL TABLE
            rankings_uservisits_join (
                sourceIP STRING,
                avgPageRank DOUBLE,
                totalRevenue DOUBLE
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            STORED AS SEQUENCEFILE LOCATION '${address}/Output/rankings_uservisits_join'
        """)
        spark.sql("""
            INSERT OVERWRITE TABLE rankings_uservisits_join
            SELECT sourceIP, avg(pageRank) as avgPageRank, sum(adRevenue) as totalRevenue
            FROM rankings as R, uservisits as UV
            WHERE R.pageURL = UV.destURL
            AND UV.visitDate BETWEEN Date('2000-01-15') AND Date('2000-01-22')
            GROUP BY UV.sourceIP
            ORDER BY totalRevenue DESC LIMIT 1
        """)

        spark.stop()
    }
}

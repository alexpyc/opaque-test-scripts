package com.alexpyc.tests

import org.apache.spark.sql.SparkSession
//import edu.berkeley.cs.rise.opaque.implicits._

object SQLSelection {
    def main (args: Array[String]) {
        if (args.length < 1){
        /*
          System.err.println(
            s"Usage: $SQLSelection <SQL tasks hdfs address>"
          )
        */
          System.exit(1)
        }

        val address = args(0)
        val spark = SparkSession.builder.master("local").enableHiveSupport().appName("SQLSelection").getOrCreate()

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
        spark.sql("DROP TABLE IF EXISTS rankings_selection")
        spark.sql(s"""
            CREATE EXTERNAL TABLE rankings_selection (
                pageURL STRING,
                pageRank INT
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            STORED AS SEQUENCEFILE LOCATION '${address}/Output/rankings_selection'
        """)
        spark.sql("""
            INSERT OVERWRITE TABLE rankings_selection
            SELECT pageURL, pageRank
            FROM rankings WHERE pageRank > 10
        """)

        spark.stop()
    }
}

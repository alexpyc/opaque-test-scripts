package com.alexpyc.tests.encryption

import org.apache.spark.sql.SparkSession
import edu.berkeley.cs.rise.opaque.implicits._

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
        val df = spark.sql("SELECT * FROM rankings").encrypted
        df.cache()

        val result = df.select($"pageURL", $"pageRank").filter($"pageRank" > 10)
        result.show()
        result.write.option("header", "true").csv(s"${address}/Output/rankings_selection")

        spark.stop()
    }
}

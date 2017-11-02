#!/bin/bash

cd ~/opaque-test-scripts

base_dir="hdfs://localhost:9000/HiBench"
sql_dir="${base_dir}/Join"
wc_dir="${base_dir}/Wordcount"
linear_dir="${base_dir}/Linear"
pr_dir="${base_dir}/Pagerank"

echo "vanilla" "SQLJoin"
hdfs dfs -rm -r "${sql_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.SQLJoin ./target/tests-1.jar ${sql_dir}
echo "vanilla" "SQLSelection"
hdfs dfs -rm -r "${sql_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.SQLSelection ./target/tests-1.jar ${sql_dir}
echo "vanilla" "SQLAggregation"
hdfs dfs -rm -r "${sql_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.SQLAggregation ./target/tests-1.jar ${sql_dir}
echo "vanilla" "WordCount"
hdfs dfs -rm -r "${wc_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.WordCount ./target/tests-1.jar ${wc_dir}
echo "vanilla" "Linear"
hdfs dfs -rm -r "${linear_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.LeastSquaresRegression ./target/tests-1.jar ${linear_dir}
echo "vanilla" "PageRank"
hdfs dfs -rm -r "${pr_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.PageRank ./target/tests-1.jar ${pr_dir}
echo "vanilla" "Pi"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.Pi ./target/tests-1.jar 10000

echo "encryption" "SQLJoin"
hdfs dfs -rm -r "${sql_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.SQLJoin ./target/tests-1.jar ${sql_dir}
echo "encryption" "SQLSelection"
hdfs dfs -rm -r "${sql_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.SQLSelection ./target/tests-1.jar ${sql_dir}
echo "encryption" "SQLAggregation"
hdfs dfs -rm -r "${sql_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.SQLAggregation ./target/tests-1.jar ${sql_dir}
echo "encryption" "WordCount"
hdfs dfs -rm -r "${wc_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.WordCount ./target/tests-1.jar ${wc_dir}
echo "encryption" "Linear"
hdfs dfs -rm -r "${linear_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.LeastSquaresRegression ./target/tests-1.jar ${linear_dir}
echo "encryption" "PageRank"
hdfs dfs -rm -r "${pr_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.PageRank ./target/tests-1.jar ${pr_dir}
echo "encryption" "Pi"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.Pi ./target/tests-1.jar 10000

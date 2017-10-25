#!/bin/bash

cd ~/opaque-test-scripts

base_dir="hdfs://localhost:9000/HiBench"
sql_dir="${base_dir}/Join"
wc_dir="${base_dir}/Wordcount"
linear_dir="${base_dir}/Linear"
pr_dir="${base_dir}/Pagerank"

if [ -d "./report" ]
then
    if [ -f "./report/console.log" ]
    then
        rm ./report/console.log
    fi
else
    mkdir ./report
fi
touch ./report/console.log

echo "SQLJoin"
hdfs dfs -rm -r "${sql_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.SQLJoin ./target/tests-1.jar ${sql_dir} >> ./report/console.log
echo "SQLSelection"
hdfs dfs -rm -r "${sql_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.SQLSelection ./target/tests-1.jar ${sql_dir} >> ./report/console.log
echo "SQLAggregation"
hdfs dfs -rm -r "${sql_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.SQLAggregation ./target/tests-1.jar ${sql_dir} >> ./report/console.log
echo "WordCount"
hdfs dfs -rm -r "${wc_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.WordCount ./target/tests-1.jar ${wc_dir} >> ./report/console.log
echo "Linear"
hdfs dfs -rm -r "${linear_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.LeastSquaresRegression ./target/tests-1.jar ${linear_dir} >> ./report/console.log
echo "PageRank"
hdfs dfs -rm -r "${pr_dir}/Output"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.PageRank ./target/tests-1.jar ${pr_dir} >> ./report/console.log
echo "Pi"
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.Pi ./target/tests-1.jar 10000000 >> ./report/console.log

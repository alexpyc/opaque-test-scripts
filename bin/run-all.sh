#!/bin/bash

cd ~/opaque-test-scripts

base_dir="hdfs://localhost:9000/HiBench"
sql_dir="${base_dir}/Join"
wc_dir="${base_dir}/Wordcount"
linear_dir="${base_dir}/Linear"
pr_dir="${base_dir}/Pagerank"

if [ ! -d "./report" ]
then
    mkdir "./report"
fi
if [ ! -f "./report/results.log" ]
then
    touch "./report/results.log"
fi
echo "" > "./report/results.log"

#Vanilla Spark tests
mode="Vanilla"

task_name="SQLJoin"
echo $mode $task_name
hdfs dfs -rm -r "${sql_dir}/Output"
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.SQLJoin ./target/tests-1.jar ${sql_dir}
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

task_name="SQLSelection"
echo $mode $task_name
hdfs dfs -rm -r "${sql_dir}/Output"
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.SQLSelection ./target/tests-1.jar ${sql_dir}
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

task_name="SQLAggregation"
echo $mode $task_name
hdfs dfs -rm -r "${sql_dir}/Output"
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.SQLAggregation ./target/tests-1.jar ${sql_dir}
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

task_name="WordCount"
echo $mode $task_name
hdfs dfs -rm -r "${wc_dir}/Output"
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.WordCount ./target/tests-1.jar ${wc_dir}
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

task_name="Linear"
echo $mode $task_name
hdfs dfs -rm -r "${linear_dir}/Output"
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.LeastSquaresRegression ./target/tests-1.jar ${linear_dir}
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

task_name="PageRank"
echo $mode $task_name
hdfs dfs -rm -r "${pr_dir}/Output"
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.PageRank ./target/tests-1.jar ${pr_dir}
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

task_name="Pi"
echo $mode $task_name
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.vanilla.Pi ./target/tests-1.jar 10000
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

#Opaque encryption tests
mode="Encryption"

task_name="SQLJoin"
echo $mode $task_name
hdfs dfs -rm -r "${sql_dir}/Output"
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.SQLJoin ./target/tests-1.jar ${sql_dir}
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

task_name="SQLSelection"
echo $mode $task_name
hdfs dfs -rm -r "${sql_dir}/Output"
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.SQLSelection ./target/tests-1.jar ${sql_dir}
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

task_name="SQLAggregation"
echo $mode $task_name
hdfs dfs -rm -r "${sql_dir}/Output"
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.SQLAggregation ./target/tests-1.jar ${sql_dir}
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

task_name="WordCount"
echo $mode $task_name
hdfs dfs -rm -r "${wc_dir}/Output"
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.WordCount ./target/tests-1.jar ${wc_dir}
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

task_name="Linear"
echo $mode $task_name
hdfs dfs -rm -r "${linear_dir}/Output"
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.LeastSquaresRegression ./target/tests-1.jar ${linear_dir}
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

task_name="PageRank"
echo $mode $task_name
hdfs dfs -rm -r "${pr_dir}/Output"
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.PageRank ./target/tests-1.jar ${pr_dir}
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

task_name="Pi"
echo $mode $task_name
start=`date +%s.%N`
${SPARK_HOME}/bin/spark-submit --class com.alexpyc.tests.encryption.Pi ./target/tests-1.jar 10000
end=`date +%s.%N`
runtime=`echo "$end-$start" | bc`
echo -e "$mode\t\t\t$task_name\t\t\t$runtime" >> "./report/results.log"

#Opaque oblivious tests

#!/bin/bash

num_partition=$1

for i in `seq 3 6`
do
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.InsecureQ1 ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.InsecureQ2 ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.InsecureQ3 ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.EncryptedQ1 ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.EncryptedQ2 ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.EncryptedQ3 ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.ObliviousQ1 ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.ObliviousQ2 ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.ObliviousQ3 ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.InsecureLSR ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.EncryptedLSR ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.ObliviousLSR ./target/tests-1.jar $num_partition $i >> ./console.log
done

for i in `seq 8 20`
do
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.InsecurePR ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.EncryptedPR ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.ObliviousPR ./target/tests-1.jar $num_partition $i >> ./console.log
done

for i in `seq 0 13`
do
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.EncryptedJoin ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.ObliviousJoin ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.JoinGene ./target/tests-1.jar $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.JoinTreatment ./target/tests-1.jar $num_partition $i >> ./console.log
done

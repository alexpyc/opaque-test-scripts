#!/bin/bash

num_partition=$1

for i in `seq 3 6`
do
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.InsecureQ1 $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.InsecureQ2 $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.InsecureQ3 $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.EncryptedQ1 $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.EncryptedQ2 $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.EncryptedQ3 $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.ObliviousQ1 $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.ObliviousQ2 $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.ObliviousQ3 $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.InsecureLSR $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.EncryptedLSR $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.ObliviousLSR $num_partition $i >> ./console.log
done

for i in `seq 8 20`
do
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.InsecurePR $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.EncryptedPR $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.ObliviousPR $num_partition $i >> ./console.log
done

for i in `seq 0 13`
do
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.InsecureJoin $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.EncryptedJoin $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.ObliviousJoin $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.JoinGene $num_partition $i >> ./console.log
    $SPARK_HOME/bin/spark-submit --class com.alexpyc.JoinTreatment $num_partition $i >> ./console.log
done

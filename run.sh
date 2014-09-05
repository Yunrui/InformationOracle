#!/bin/sh

articleDirectory=$1
metaDirectory=$2/*
label=$3

echo 'J$p1ter' | sudo -u hdfs -S hadoop fs -rmr /tmp/newsletter
echo 'J$p1ter' | sudo -u hdfs hadoop fs -rmr /io/result/*

echo "alter 'content', {NAME=>'d', METHOD=>'delete'}; alter 'content', {NAME=>'d'}" | hbase shell
echo 'J$p1ter' | sudo -u hdfs -S hadoop fs -cat $metaDirectory | python dumpContentMeta.py | hbase shell
echo 'J$p1ter' | sudo -u hdfs -S mahout seqdirectory -c UTF-8 -i $articleDirectory -o /io/result/seq
echo 'J$p1ter' | sudo -u hdfs -S mahout seq2sparse -i /io/result/seq/par* -o /io/result/tfidf -s 50 -md 10  -ng 2 -ml 50 -x 20 -seq

javac -cp "/opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/hadoop/client/*:/opt/cloudera/parcels/CDH/lib/mahout/*:." SequenceFileOperator.java
java -cp "/opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/hadoop/client/*:/opt/cloudera/parcels/CDH/lib/mahout/*:." SequenceFileOperator hdfs://v3namenode:8020/io/result/tfidf/dictionary.file-0 hdfs://v3namenode:8020/io/result/tfidf/tfidf-vectors/part-r-00000 | hbase shell

cp tag_match.pig /tmp/
echo 'J$p1ter' | sudo -u hdfs -S pig -f /tmp/tag_match.pig
echo 'J$p1ter' | sudo -u hdfs -S hadoop fs -cat /tmp/newsletter/par* | python insert.py "$label"

echo $articleDirectory
echo $metaDirectory
echo $label

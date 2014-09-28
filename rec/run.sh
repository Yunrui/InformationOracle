#!/bin/sh

zookeeper=$1
method=$2
articleDirectory=$3
metaDirectory=$4/*
label=$5

#Choose training  method
if [ $method = 'lda' ]
then
    script='lda'
    weight='TF'
else
    if [ $method = 'tag' ]
    then
        script='tag_match'
        weight='TFIDF'
    else 
        echo "\033[41;36mUnknown training method, currently only lda and tag are supported. \033[0m"
        exit;
    fi
fi

script=$script.pig

#Print Information
echo "\033[40;32mTraining Configuration: \033[0m"
echo "  Training Method: $method"
echo "  Training Script: $script"
echo "  Training Weight: $weight"

echo "\033[40;33mTokenize Articles.......... \033[0m"
echo 'J$p1ter' | sudo -u hdfs hadoop fs -rmr /io/result/*
echo 'J$p1ter' | sudo -u hdfs -S mahout seqdirectory -c UTF-8 -i $articleDirectory -o /io/result/seq
echo 'J$p1ter' | sudo -u hdfs -S mahout seq2sparse -i /io/result/seq/par* -o /io/result/weight -s 50 -md 10  -ng 2 -ml 50 -x 20 -seq -wt $weight

echo "\033[40;33mPreparing Execution Context.......... \033[0m"
java -cp "lib/hadoop/*:lib/hadoop/client/*:lib/mahout/*:lib/*:lib/hbase/*:*:." io.rec.RecRun $zookeeper


if [ $method = 'tag' ]
then
    echo "\033[40;33mExecute Tag Match.......... \033[0m"
    java -cp "/opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/hadoop/client/*:/opt/cloudera/parcels/CDH/lib/mahout/*:." SequenceFileOperator hdfs://v3namenode:8020/io/result/weight/dictionary.file-0 hdfs://v3namenode:8020/io/result/weight/tfidf-vectors/part-r-00000 | hbase shell
fi

if [ $method = 'lda' ]
then
    echo "\033[40;33mExecute LDA.......... \033[0m"
    echo 'J$p1ter' | sudo -u hdfs -S mahout rowid -i /io/result/weight/tf-vectors -o /io/result/matrix
    echo 'J$p1ter' | sudo -u hdfs -S mahout cvb -i /io/result/matrix/matrix -o /io/result/lda -k 100 -ow -x 20 -dict /io/result/weight/dictionary.file-0 -dt /io/result/lda2/topic -mt /io/result/lda2/model
    java -cp "/opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/hadoop/client/*:/opt/cloudera/parcels/CDH/lib/mahout/*:." LDADumper hdfs://v3namenode:8020/io/result/matrix/docIndex  hdfs://v3namenode:8020/io/result/lda2/topic/part-m-00000 | hbase shell
fi

echo "\033[40;33mCalculating Recommendation.......... \033[0m"
cp $script /tmp/
echo 'J$p1ter' | sudo -u hdfs -S pig -f /tmp/$script
echo 'J$p1ter' | sudo -u hdfs -S hadoop fs -rmr /tmp/newsletter
echo 'J$p1ter' | sudo -u hdfs -S hadoop fs -cat /tmp/newsletter/par* | python insert.py "$label"

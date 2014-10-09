#!/bin/sh

mkdir target

echo "\033[40;33mCompiling.......... \033[0m"
#javac -cp "lib/hadoop/*:lib/mahout/*:." src/io/rec/SequenceFileOperator.java -d target
#javac -cp "lib/hadoop/*:lib/mahout/*:." src/io/rec/LDADumper.java -d target
#javac -cp "lib/hadoop/*:lib/mahout/*:lib/hbase/*:lib/*:."  src/io/rec/RefreshContentData.java -d target
javac -cp "lib/hadoop/*:lib/mapreduce/*:lib/mahout/*:lib/hbase/*:lib/*:."  src/main/java/io/rec/*.java -d target

echo "\033[40;33mPackaging.......... \033[0m"
cd target
jar -cvf rec.jar io
mv rec.jar ../
cd ..

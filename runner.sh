#!/bin/bash

if [[ $# -lt 2 ]]; then
        echo "Usage  : $0 <package_dir> <class_name>"
        echo "Example: $0 part1/c WordCount"
        exit 1
fi;

DIR=$1/
FILE=$2
CP="./bin:/usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/usr/lib/hadoop/client-0.20/*"

rm -f bin/*.class  bin/${FILE}.jar

javac -cp $CP ${DIR}/*.java -d bin
cd bin;jar cfv ${FILE}.jar *.class;cd -

if [ "$3" == "local" ]; then
        java -cp $CP ${DIR}${FILE} input/${DIR} output
        cat output/*
        exit 0
fi;

HDIR=/user/cloudera

hadoop fs -rm ${HDIR}/input/*
hadoop fs -rmdir ${HDIR}/input
hadoop fs -mkdir ${HDIR}/input
hadoop fs -copyFromLocal ${DIR}/input/* ${HDIR}/input
hadoop jar bin/${FILE}.jar ${FILE} ${HDIR}/input ${HDIR}/output $3
echo "=================================================="
echo "hadoop fs -cat ${HDIR}/output/*"
echo "=================================================="
hadoop fs -cat ${HDIR}/output/*
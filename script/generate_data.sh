#! /bin/bash

HADOOP_HOME=/home/hadoop/hadoop-2.7.3
TASK_NUM=10
TASK_RECORD=400000

for i in `seq 1 ${TASK_NUM}`; do
 exec python datacreation_wide.py -r ${TASK_RECORD}  > /tmp/data_wide_$i.txt && ${HADOOP_HOME}/bin/hadoop fs -put /tmp/data_wide_$i.txt /home/hadoop/input/ &
done
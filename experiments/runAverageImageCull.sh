#!/bin/bash
ant -f ../build.xml avgimgcull
hadoop dfs -rm $HDFS_HOME/averageimagecull.jar
hadoop dfs -put averageimagecull.jar $HDFS_HOME/
hadoop jar averageimagecull.jar $1 $2 $3

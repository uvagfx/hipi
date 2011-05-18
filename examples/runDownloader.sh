#!/bin/bash
ant -f ../build.xml downloader
hadoop dfs -rm $HDFS_HOME/downloader.jar
hadoop dfs -put downloader.jar $HDFS_HOME/
hadoop jar downloader.jar $1 $2 $3

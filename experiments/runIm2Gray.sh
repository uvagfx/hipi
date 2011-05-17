#!/bin/bash
ant -f ../build.xml im2gray
hadoop dfs -rm /virginia/uvagfx/im2gray.jar
hadoop dfs -put im2gray.jar /virginia/uvagfx/
hadoop jar im2gray.jar $1 $2 $3

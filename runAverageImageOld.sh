#!/bin/bash
ant -buildfile build.xml.averageimageold
hadoop dfs -rm /virginia/uvagfx/averageimageold.jar
hadoop dfs -put averageimageold.jar /virginia/uvagfx/
hadoop jar averageimageold.jar $1 $2 $3

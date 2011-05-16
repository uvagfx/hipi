#!/bin/bash
ant avgimgcull
hadoop dfs -rm /virginia/uvagfx/averageimagecull.jar
hadoop dfs -put averageimagecull.jar /virginia/uvagfx/
hadoop jar averageimagecull.jar $1 $2 $3

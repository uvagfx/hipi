#!/bin/bash
ant -f ../build.xml downloader
hadoop dfs -rm /virginia/uvagfx/downloader.jar
hadoop dfs -put downloader.jar /virginia/uvagfx/
hadoop jar downloader.jar $1 $2 $3

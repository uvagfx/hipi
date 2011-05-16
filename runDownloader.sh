#!/bin/bash
ant downloader
hadoop dfs -rm /virginia/uvagfx/downloader.jar
hadoop dfs -put downloader.jar /virginia/uvagfx/
hadoop jar downloader.jar $1 $2 $3

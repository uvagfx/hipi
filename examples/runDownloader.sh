#!/bin/bash
ant -f ../build.xml downloader
hadoop jar downloader.jar $1 $2 $3

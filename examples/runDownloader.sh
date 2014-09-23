#!/bin/bash
ant -f ../build.xml downloader
hadoop jar downloader.jar -libjars ../3rdparty/json-simple-1.1.1.jar $1 $2 $3

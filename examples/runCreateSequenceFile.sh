#!/bin/bash
ant -f ../build.xml createsequencefile
hadoop jar createsequencefile.jar $1 $2

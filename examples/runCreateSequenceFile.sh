#!/bin/bash
ant -f ../build.xml seqfile
hadoop jar createsequencefile.jar $1 $2

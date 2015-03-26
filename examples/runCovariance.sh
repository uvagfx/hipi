#!/bin/bash
ant -f ../build.xml covariance
hadoop jar covariance.jar $1 $2

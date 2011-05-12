#!/bin/bash
ant
hadoop dfs -rm /virginia/uvagfx/cms2vp/HIBMapReduce.jar
hadoop dfs -rmr /virginia/uvagfx/cms2vp/out
hadoop dfs -put HIBMapReduce.jar /virginia/uvagfx/cms2vp/HIBMapReduce.jar
hadoop jar HIBMapReduce.jar /virginia/uvagfx/cms2vp/bundle.hib /virginia/uvagfx/cms2vp/out/

package org.hipi.examples.covar;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.opencv.OpenCVMatWritable;

public class ComputeMean {

  public static int run(String[] args) throws Exception {

    System.out.println("Running compute mean.");

    Job job = Job.getInstance();
    Covariance.validateArgs(args, job.getConfiguration());

    String inputPath = args[0];
    String outputDir = args[1];
    String outputSubDir = outputDir + "/mean-output/";

    job.setJarByClass(Covariance.class);

    job.setInputFormatClass(HibInputFormat.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(OpenCVMatWritable.class);

    job.setMapperClass(MeanMapper.class);
    job.setReducerClass(MeanReducer.class);
    job.setNumReduceTasks(1);

    job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
    job.setSpeculativeExecution(true);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    Covariance.mkdir(outputDir, job.getConfiguration());
    Covariance.rmdir(outputSubDir, job.getConfiguration());
    FileOutputFormat.setOutputPath(job, new Path(outputSubDir));
    job.getConfiguration().setStrings("mean.outpath", outputSubDir);


    return job.waitForCompletion(true) ? 0 : 1;
  }

}

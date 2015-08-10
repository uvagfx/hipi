package org.hipi.examples.covar;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.hipi.image.FloatImage;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.imagebundle.mapreduce.output.BinaryOutputFormat;
import org.hipi.opencv.OpenCVMatWritable;

public class ComputeMean {

  public static int run(String[] args) throws Exception {

    System.out.println("Starting to run mean job...");

    

    Job job = Job.getInstance();
    Covariance.validateArgs(args, job.getConfiguration());

    String inputPath = args[0];
    String outputPath = args[1];

    job.setJarByClass(Covariance.class);

    job.setInputFormatClass(HibInputFormat.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(OpenCVMatWritable.class);

    job.setMapperClass(MeanMapper.class);
    job.setReducerClass(MeanReducer.class);
    job.setNumReduceTasks(1);

    job.setOutputFormatClass(BinaryOutputFormat.class);
    job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
    job.setSpeculativeExecution(true);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    Covariance.mkdir(args[1], job.getConfiguration());
    Covariance.rmdir(args[1] + "/mean-output/", job.getConfiguration());
    FileOutputFormat.setOutputPath(job, new Path(args[1] + "/mean-output/"));
    job.getConfiguration().setStrings("mean.outpath", args[1] + "/mean-output/");


    return job.waitForCompletion(true) ? 0 : 1;
  }

}

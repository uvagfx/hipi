package org.hipi.tools.covar;

import org.hipi.mapreduce.BinaryOutputFormat;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.opencv.OpenCVMatWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ComputeCovariance {
  
  public static int run(String[] args, String inputHibPath, String outputDir, String inputMeanPath) 
      throws ClassNotFoundException, IllegalStateException, InterruptedException, IOException {
    
    System.out.println("Running compute covariance.");
   
    Job job = Job.getInstance();
    
    job.setJarByClass(Covariance.class);

    job.setInputFormatClass(HibInputFormat.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(OpenCVMatWritable.class);
    
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(OpenCVMatWritable.class);

    job.setMapperClass(CovarianceMapper.class);
    job.setReducerClass(CovarianceReducer.class);
    job.setNumReduceTasks(1);
    
    job.setOutputFormatClass(BinaryOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(inputHibPath));
    FileOutputFormat.setOutputPath(job, new Path(outputDir));
    
    job.getConfiguration().setStrings("hipi.covar.mean.path", inputMeanPath);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}

package org.hipi.examples.covar;

import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.opencv.OpenCVMatWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ComputeCovariance {
  
  public static int run(String[] args, String inputHibPath, String outputDir, String meanCachePath) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
    
    System.out.println("Running compute covariance.");
   
    Job job = Job.getInstance();
    
    job.setJarByClass(Covariance.class);

    job.setInputFormatClass(HibInputFormat.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(OpenCVMatWritable.class);

    job.setMapperClass(CovarianceMapper.class);
    job.setReducerClass(CovarianceReducer.class);
    job.setNumReduceTasks(1);
    
    job.setOutputFormatClass(BinaryOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(inputHibPath));
    FileOutputFormat.setOutputPath(job, new Path(outputDir));
    

    job.addCacheFile(new URI(job.getConfiguration().get("fs.defaultFS") + meanCachePath));

    return job.waitForCompletion(true) ? 0 : 1;
  }
}

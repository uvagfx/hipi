package org.hipi.examples.covar;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.opencv.OpenCVMatWritable;

public class ComputeCovariance {
  
  public static int run(String[] args, String inputHibPath, String outputDir, String meanCachePath) throws Exception {
    
    System.out.println("Running compute covariance.");
   
    Job job = Job.getInstance();
    
    job.addCacheFile(new URI(job.getConfiguration().get("fs.default.name") + meanCachePath));
    
    job.setJarByClass(Covariance.class);

    job.setInputFormatClass(HibInputFormat.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(OpenCVMatWritable.class);

    job.setMapperClass(CovarianceMapper.class);
    job.setReducerClass(CovarianceReducer.class);
    job.setNumReduceTasks(1);

    job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
    
    job.setOutputFormatClass(BinaryOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(inputHibPath));
    FileOutputFormat.setOutputPath(job, new Path(outputDir));

    return job.waitForCompletion(true) ? 0 : 1;
  }
}

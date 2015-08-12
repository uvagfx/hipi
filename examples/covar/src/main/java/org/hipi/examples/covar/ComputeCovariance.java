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
import org.hipi.imagebundle.mapreduce.output.BinaryOutputFormat;
import org.hipi.opencv.OpenCVMatWritable;

public class ComputeCovariance {
  
  public static int run(String[] args) throws Exception {
    
    System.out.println("Running compute covariance.");
   
    Job job = Job.getInstance();
    Covariance.validateArgs(args, job.getConfiguration());
    
    String inputPath = args[0];
    String outputDir = args[1];
    String outputSubDir = outputDir + "/covariance-output/";
    
    String cachePath = outputDir + "/mean-output/part-r-00000";
    validateMeanCachePath(cachePath, job.getConfiguration());
    
    if(cachePath.startsWith("/")) {
      job.addCacheFile(new URI(job.getConfiguration().get("fs.default.name") + cachePath));
    } else {
      job.addCacheFile(new URI(job.getConfiguration().get("fs.default.name") + "/" + cachePath));
    }
    
    job.setJarByClass(Covariance.class);

    job.setInputFormatClass(HibInputFormat.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(OpenCVMatWritable.class);

    job.setMapperClass(CovarianceMapper.class);
    job.setReducerClass(CovarianceReducer.class);
    job.setNumReduceTasks(1);

    job.setOutputFormatClass(BinaryOutputFormat.class);
    job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
    job.setSpeculativeExecution(true);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    Covariance.mkdir(outputDir, job.getConfiguration());
    Covariance.rmdir(outputSubDir, job.getConfiguration());
    FileOutputFormat.setOutputPath(job, new Path(outputSubDir));
    
    job.getConfiguration().setStrings("covariance.outpath" , outputSubDir);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  private static void validateMeanCachePath(String cachePathString, Configuration conf) throws IOException {
    Path cachePath = new Path(cachePathString);
    FileSystem fileSystem = FileSystem.get(conf);
    if (!fileSystem.exists(cachePath)) {
      System.out.println("Path to mean does not exist: " + cachePath);
      System.exit(0);
    }
  }
}

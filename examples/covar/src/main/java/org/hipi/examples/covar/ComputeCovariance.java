package org.hipi.examples.covar;

import java.net.URI;




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

public class ComputeCovariance {
  
  public static int run(String[] args) throws Exception {
    
    System.out.println("Starting to run covariance job...");
   
    
    Job job = Job.getInstance();

    job.setJarByClass(Covariance.class);

    job.addCacheFile(new URI("hdfs://" + args[1] + "/mean-output/part-r-00000"));

    job.setInputFormatClass(HibInputFormat.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(OpenCVMatWritable.class);

    job.setMapperClass(CovarianceMapper.class);
    job.setReducerClass(CovarianceReducer.class);
    job.setNumReduceTasks(1);

    job.setOutputFormatClass(BinaryOutputFormat.class);
    job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
    job.setSpeculativeExecution(true);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    Covariance.mkdir(args[1], job.getConfiguration());
    Covariance.rmdir(args[1] + "/covariance-output/", job.getConfiguration());
    FileOutputFormat.setOutputPath(job, new Path(args[1] + "/covariance-output/"));
    
    job.getConfiguration().setStrings("covariance.outpath" , args[1] + "/covariance-output/");

    return job.waitForCompletion(true) ? 0 : 1;
  }
}

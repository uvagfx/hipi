package org.hipi.examples.covar;


import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hipi.image.FloatImage;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.imagebundle.mapreduce.output.BinaryOutputFormat;
import org.hipi.opencv.OpenCVMatWritable;

public class Covariance extends Configured implements Tool {

  public static final int N = 48; // Patch size is NxN
  public static final float sigma = 10; // Standard deviation of Gaussian weighting function
  
  public static void rmdir(String path, Configuration conf) throws IOException {
    Path outputPath = new Path(path);
    FileSystem fileSystem = FileSystem.get(conf);
    if (fileSystem.exists(outputPath)) {
      fileSystem.delete(outputPath, true);
    }
  }

  public static void mkdir(String path, Configuration conf) throws IOException {
    Path outputPath = new Path(path);
    FileSystem fileSystem = FileSystem.get(conf);
    if (!fileSystem.exists(outputPath)) {
      fileSystem.mkdirs(outputPath);
    }
  }

  public static void validateArgs(String[] args, Configuration conf) throws IllegalArgumentException, IOException {
    
    if (args.length != 2) {
      System.out.println("Usage: covariance <input HIB> <output directory>");
      System.exit(0);
    }

    Path inputPath = new Path(args[0]);
    FileSystem fileSystem = FileSystem.get(conf);
    if (!fileSystem.exists(inputPath)) {
        throw new IllegalArgumentException("Invalid path (does not exist): " + inputPath);
    }
  }

  public int run(String[] args) throws Exception {


    if (ComputeMean.run(args) == 1) {
      return 1;

    }
    if (ComputeCovariance.run(args) == 1) {
      return 1;
    }

    // Indicate success
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Covariance(), args);
    System.exit(res);
  }

}

package org.hipi.tools.covar;

import static org.bytedeco.javacpp.opencv_imgproc.CV_RGB2GRAY;

import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader.HipiColorSpace;
import org.hipi.opencv.OpenCVUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bytedeco.javacpp.opencv_imgproc;
import org.bytedeco.javacpp.opencv_core.Mat;

import java.io.IOException;

public class Covariance extends Configured implements Tool {
  
  public static final int patchSize = 64; // Patch dimensions: patchSize x patchSize
  public static final float sigma = 10; // Standard deviation of Gaussian weighting function
  
  
  // Used to convert input FloatImages into grayscale OpenCV Mats in MeanMapper and CovarianceMapper
  public static boolean convertFloatImageToGrayscaleMat(FloatImage image, Mat cvImage) {
    
    // Convert FloatImage to Mat, and convert Mat to grayscale (if necessary)
    HipiColorSpace colorSpace = image.getColorSpace();
    switch(colorSpace) {
      
      //if RGB, convert to grayscale
      case RGB:
        Mat cvImageRGB = OpenCVUtils.convertRasterImageToMat(image);
        opencv_imgproc.cvtColor(cvImageRGB, cvImage, CV_RGB2GRAY);
        return true;
        
      //if LUM, already grayscale
      case LUM:
        cvImage = OpenCVUtils.convertRasterImageToMat(image);
        return true;
        
      //otherwise, color space is not supported for this example. Skip input image.
      default:
        System.out.println("HipiColorSpace [" + colorSpace + "] not supported in covar example. ");
        return false;
    }
  }
  
  private static void rmdir(String path, Configuration conf) throws IOException {
    Path outputPath = new Path(path);
    FileSystem fileSystem = FileSystem.get(conf);
    if (fileSystem.exists(outputPath)) {
      fileSystem.delete(outputPath, true);
    }
  }

  private static void mkdir(String path, Configuration conf) throws IOException {
    Path outputPath = new Path(path);
    FileSystem fileSystem = FileSystem.get(conf);
    if (!fileSystem.exists(outputPath)) {
      fileSystem.mkdirs(outputPath);
    }
  }

  private static void validateArgs(String[] args, Configuration conf) throws IOException {
    
    if (args.length != 2) {
      System.out.println("Usage: covar.jar <input HIB> <output directory>");
      System.exit(1);
    }

    Path inputPath = new Path(args[0]);
    FileSystem fileSystem = FileSystem.get(conf);
    if (!fileSystem.exists(inputPath)) {
      System.out.println("Input HIB does not exist at location: " + inputPath);
      System.exit(1);
    }
    
  }
  
  private static void validateMeanPath(String inputMeanPathString, Configuration conf) 
      throws IOException {
    Path meanPath = new Path(inputMeanPathString);
    FileSystem fileSystem = FileSystem.get(conf);
    if (!fileSystem.exists(meanPath)) {
      System.out.println("Mean patch does not exist at location: " + meanPath);
      System.exit(1);
    }
  }

  
  public int run(String[] args) throws Exception {
    
    // Used for initial argument validation and hdfs configuration before jobs are run
    Configuration conf = Job.getInstance().getConfiguration();
    
    // Validate arguments before any work is done
    validateArgs(args, conf);
    
    // Build I/O path strings
    String inputHibPath = args[0];
    String outputBaseDir = args[1];
    String outputMeanDir = outputBaseDir + "/mean-output/";
    String outputCovarianceDir = outputBaseDir + "/covariance-output/";
    String inputMeanPath = outputMeanDir + "part-r-00000"; //used to access ComputeMean result
    
    // Set up directory structure
    mkdir(outputBaseDir, conf);
    rmdir(outputMeanDir, conf);
    rmdir(outputCovarianceDir, conf);

    // Run compute mean
    if (ComputeMean.run(args, inputHibPath, outputMeanDir) == 1) {
      System.out.println("Compute mean job failed to complete.");
      return 1;
    }
    
    validateMeanPath(inputMeanPath, conf);
    
    // Run compute covariance
    if (ComputeCovariance.run(args, inputHibPath, outputCovarianceDir, inputMeanPath) == 1) {
      System.out.println("Compute covariance job failed to complete.");
      return 1;
    }

    // Indicate success
    return 0;
  }

  // Main driver for full covariance computation
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Covariance(), args);
    System.exit(res);
  }

}

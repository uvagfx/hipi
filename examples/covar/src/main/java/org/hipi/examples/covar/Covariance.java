package org.hipi.examples.covar;


import static org.bytedeco.javacpp.opencv_imgproc.CV_BGR2GRAY;
import static org.bytedeco.javacpp.opencv_imgproc.cvtColor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.hipi.image.FloatImage;
import org.hipi.opencv.MatUtils;

public class Covariance extends Configured implements Tool {

  //Constants and methods which are used by both ComputeMean and ComputeCovariance
  
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
  
  public static Mat covertFloatImageToGrayScaleMat(FloatImage value) throws IOException {
    //Convert input float image to opencv mat
    Mat srcImage = MatUtils.convertFloatImageToMat(value);
    
    //Convert opencv mat to grayscale, if necessary.
    Mat grayScaleSrcImage = new Mat(srcImage.rows(), srcImage.cols(), opencv_core.CV_32FC1, new Scalar(0.0));
    if (srcImage.type() != opencv_core.CV_32FC1) {
      cvtColor(srcImage, grayScaleSrcImage, CV_BGR2GRAY);
    } else {
      grayScaleSrcImage = srcImage;
    }
    return grayScaleSrcImage;
  }

  public static void validateArgs(String[] args, Configuration conf) throws IOException {
    
    if (args.length != 2) {
      System.out.println("Usage: covariance <input HIB> <output directory>");
      System.exit(0);
    }

    Path inputPath = new Path(args[0]);
    FileSystem fileSystem = FileSystem.get(conf);
    if (!fileSystem.exists(inputPath)) {
      System.out.println("Path to <input HIB> does not exist: " + inputPath);
      System.exit(0);
    }
  }

  public int run(String[] args) throws Exception {

    // Run compute mean
    if (ComputeMean.run(args) == 1) {
      return 1;

    }
    
    // Run compute covariance
    if (ComputeCovariance.run(args) == 1) {
      return 1;
    }

    // Indicate success
    return 0;
  }

  //main driver for full covariance computation
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Covariance(), args);
    System.exit(res);
  }

}

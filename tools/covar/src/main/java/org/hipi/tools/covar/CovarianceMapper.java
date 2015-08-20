package org.hipi.tools.covar;

import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.opencv.OpenCVMatWritable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Rect;
import org.bytedeco.javacpp.opencv_core.Scalar;

import java.io.IOException;
import java.net.URI;
import java.nio.FloatBuffer;

public class CovarianceMapper extends
    Mapper<HipiImageHeader, FloatImage, IntWritable, OpenCVMatWritable> {

  Mat mean; // Stores result of computeMean job which has been stored in the job's cache.
  Mat gaussian; // Stores gaussian matrix computed in setup

  @Override
  public void setup(Context job) {
    
    int N = Covariance.patchSize;
    float sigma = Covariance.sigma;
    
    /////
    // Create mean mat using data from mean computation
    /////
    
    try {
      
      // Access mean data on HDFS
      String meanPathString = job.getConfiguration().get("mean.path");
      if(meanPathString == null) {
        System.err.println("Mean path not set in configuration - cannot continue. Exiting.");
        System.exit(1);
      }
      Path meanPath = new Path(meanPathString);
      FSDataInputStream dis = FileSystem.get(job.getConfiguration()).open(meanPath);
      
      // Populate mat with mean data
      OpenCVMatWritable meanWritable = new OpenCVMatWritable();
      meanWritable.readFields(dis);
      mean = meanWritable.getMat();
      
    } catch (IOException ioe) {
      ioe.printStackTrace();
      System.exit(1);
    }
    
    /////
    // Create a normalized gaussian array for patch masking
    /////
    
    gaussian = new Mat(N, N, opencv_core.CV_32FC1, new Scalar(0.0));
    FloatBuffer gaussianBuffer = gaussian.createBuffer();
    float gaussianSum = 0; // Used for normalization
    
    // 'center' and 'denominator' precomputed for gaussian generation
    int center = N / 2;
    double denominator =  2 * Math.pow(sigma, 2);
    
    for (int i = 0; i < N; i++) {
      for (int j = 0; j < N; j++) {      
        float gaussianValue = generate2DGaussianValue(i, j, center, denominator);
        gaussianBuffer.put(i * N + j, gaussianValue);
        gaussianSum += gaussianValue;
      }
    }

    // Normalize gaussian array
    float averageGaussianValue = (float) gaussianSum / (float) (N * N); //sum divided by area of gaussian array (N x N)
    gaussian = opencv_core.divide(gaussian, averageGaussianValue).asMat();
  }
  
  // 2D Gaussian: f(i, j) = A * exp(-( (i - i0)^2 / (2 * iSigma^2) + (j - j0)^2 / (2 * jSigma^2) ))
  // i0 == j0 == "center"
  // iSigma == jSigma (sigma)
  // A == 1
  // i0 == j0 == "center"
  // (2 * sigma^2) == "denominator"
  private float generate2DGaussianValue(int i, int j, int center, double denominator) {    
    
    double termOne = Math.pow(i - center, 2) / denominator;
    double termTwo = Math.pow(j - center, 2) / denominator;
    
    return ((float)Math.exp(-(termOne + termTwo)));
  }

  @Override
  public void map(HipiImageHeader header, FloatImage image, Context context) throws IOException,
      InterruptedException {
    
    /////
    // Perform conversion to OpenCV
    /////
    
    Mat cvImage = new Mat(image.getHeight(), image.getWidth(), opencv_core.CV_32FC1);
    
    // if unable to convert input FloatImage to grayscale Mat, skip image and move on
    if(!Covariance.convertFloatImageToGrayscaleMat(image, cvImage)) {
      return;
    }
     
    /////
    // Create patches for covariance computation
    /////
    
    // Specify number of patches to use in covariance computation (iMax * jMax patches)
    int iMax = 10;
    int jMax = 10;
    
    // Stores FloatBuffers for each patch
    // FloatBuffers refer to Mat data, so Mats don't need to be stored
    FloatBuffer[] patchBuffers = new FloatBuffer[iMax * jMax];
    
    //patch dimensions (N x N)
    int N = Covariance.patchSize;

    // Create mean-subtracted and gaussian-masked patches
    for (int i = 0; i < iMax; i++) {
      int x = (cvImage.cols() - N) * i / iMax;
      for (int j = 0; j < jMax; j++) {
        int y = (cvImage.rows() - N) * j / jMax;
        
        Mat patch = cvImage.apply(new Rect(x, y, N, N)).clone();
        
        opencv_core.subtract(patch, mean, patch);
        opencv_core.multiply(patch, gaussian, patch);
        
        patchBuffers[(iMax * i) + j] = patch.createBuffer();
      }
    }
    
    /////
    // Run covariance computation
    /////

    // Stores the (N^2 x N^2) covariance matrix AAt
    Mat covarianceMat = new Mat(N * N, N * N, opencv_core.CV_32FC1, new Scalar(0.0));
    FloatBuffer covarianceBuffer = covarianceMat.createBuffer();
    for (int i = 0; i < N * N; i++) {
      for (int j = 0; j < N * N; j++) {
        float accumulatedValue = 0.0f;
        for (int k = 0; k < patchBuffers.length; k++) {
          accumulatedValue += patchBuffers[k].get(i) * patchBuffers[k].get(j); 
        }
        covarianceBuffer.put(i * N * N + j, accumulatedValue);
      }
    }
    context.write(new IntWritable(0), new OpenCVMatWritable(covarianceMat));
  }
}

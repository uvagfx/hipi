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

  Mat mean; // Stores pre-computed mean
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
      String meanPathString = job.getConfiguration().get("hipi.covar.mean.path");
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
    
    // 'center' and 'denominator' precomputed for gaussian generation
    int center = N / 2;
    double denominator =  2 * sigma * sigma;
    
    for (int i = 0; i < N; i++) {
      for (int j = 0; j < N; j++) {      
        gaussianBuffer.put(i * N + j, generate2DGaussianValue(i, j, center, denominator));
      }
    }
    
    // compute euclidean distance of gaussian vector
    double sumOfSquares = 0.0;
    for(int i = 0; i < N * N; i++) {
      sumOfSquares += gaussianBuffer.get(i) * gaussianBuffer.get(i);
    }
    
    double euclideanDistance = Math.sqrt(sumOfSquares);
    
    if(euclideanDistance == 0) {
      System.out.println("Invalid euclidean distance of gaussian vector [0]. Cannot continue.");
      System.exit(1);
    }
    
    // normalize gaussian weighting matrix
    gaussian = opencv_core.divide(gaussian, euclideanDistance).asMat();

  }
  
  // 2D Gaussian: f(i, j) = A * exp(-( (i - i0)^2 / (2 * iSigma^2) + (j - j0)^2 / (2 * jSigma^2) ))
  // i0 == j0 == "center"
  // iSigma == jSigma (sigma)
  // A == 1
  // i0 == j0 == "center"
  // (2 * sigma^2) == "denominator"
  private float generate2DGaussianValue(int i, int j, int center, double denominator) {    
    
    double termOne = ((double)((i - center) * (i - center))) / denominator;
    double termTwo = ((double)((j - center) * (j - center))) / denominator;
    
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
      System.out.println("CovarianceMapper is skipping image with invalid color space.");
      return;
    }
     
    /////
    // Create patches for covariance computation
    /////
    
    // Specify number of patches to use in covariance computation (iMax * jMax patches)
    int iMax = 10;
    int jMax = 10;
    
    Mat[] patches = new Mat[iMax * jMax];
    
    int N = Covariance.patchSize;

    // Create mean-subtracted and gaussian-masked patches
    for (int i = 0; i < iMax; i++) {
      int x = (cvImage.cols() - N) * i / iMax;
      for (int j = 0; j < jMax; j++) {
        int y = (cvImage.rows() - N) * j / jMax;
        
        Mat patch = cvImage.apply(new Rect(x, y, N, N)).clone();
        
        opencv_core.subtract(patch, mean, patch);
        opencv_core.multiply(patch, gaussian, patch);
        
        patches[(iMax * i) + j] = patch;
      }
    }
    
    /////
    // Run covariance computation
    /////

    // Stores the (N^2 x N^2) covariance matrix patchMat*transpose(patchMat)
    Mat covarianceMat = new Mat(N * N, N * N, opencv_core.CV_32FC1, new Scalar(0.0));
    
    // Stores patches as column vectors
    Mat patchMat = new Mat(N * N, patches.length, opencv_core.CV_32FC1, new Scalar(0.0));
   
    // Create patchMat
    for(int i = 0; i < patches.length; i++) {
      patches[i].reshape(0, N * N).copyTo(patchMat.col(i));
    }
    
    // Compute patchMat*transpose(patchMat)
    covarianceMat = opencv_core.multiply(patchMat, patchMat.t().asMat()).asMat();
    
    context.write(new IntWritable(0), new OpenCVMatWritable(covarianceMat));
  }
}

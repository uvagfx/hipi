package org.hipi.examples.covar;

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

  Mat mean; //stores result of computeMean job which has been stored in the job's cache.
  Mat gaussian; //stores gaussian matrix computed in mapper setup

  @Override
  public void setup(Context job) {
    
    int N = Covariance.patchSize;
    float sigma = Covariance.sigma;
    
    //access mean from cache
    try {
      // Access the job cache
      URI[] files = new URI[1];
      if (job.getCacheFiles() != null) {
        files = job.getCacheFiles();
      } else {
        System.err.println("Job cache files is null!");
      }

      // Read mean from previously run mean job
      Path cacheFilePath = new Path(files[0].toString());
      FSDataInputStream dis = FileSystem.get(job.getConfiguration()).open(cacheFilePath);
      dis.skip(4);
      OpenCVMatWritable meanWritable = new OpenCVMatWritable();
      meanWritable.readFields(dis);
      mean = meanWritable.getMat();
    } catch (IOException ioe) {
      System.err.println(ioe);
    }

    // Create a normalized gaussian array with standard deviation of 10 pixels for patch masking
    gaussian = new Mat(N, N, opencv_core.CV_32FC1, new Scalar(0.0));
    FloatBuffer gaussianBuffer = gaussian.createBuffer();
    float gaussianSum = 0;
    for (int i = 0; i < N; i++) {
      for (int j = 0; j < N; j++) {
        float gaussianValue =
            (float) Math.exp(-((i - N / 2) * (i - N / 2) / (sigma * sigma) + (j - N / 2)
                * (j - N / 2) / (sigma * sigma)));
        gaussianBuffer.put(i * N + j, gaussianValue);
        gaussianSum += gaussianValue;
      }
    }

    gaussian = opencv_core.multiply(gaussian, ((N * N) / gaussianSum)).asMat();
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
    
    // Stores FloatBuffers for each patch (prevents redundant FloatBuffer creation)
    // FloatBuffers refer to original memory segments contained by Mats, so mats don't need to be stored
    FloatBuffer[] patchBuffers = new FloatBuffer[iMax * jMax];
    
    //patch dimensions (N x N)
    int N = Covariance.patchSize;

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

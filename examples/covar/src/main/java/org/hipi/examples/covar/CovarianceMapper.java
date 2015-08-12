package org.hipi.examples.covar;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Rect;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.opencv.OpenCVMatWritable;
import org.hipi.opencv.

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
    FloatIndexer gaussianIndexer = gaussian.createIndexer();
    float gaussianSum = 0;
    for (int i = 0; i < N; i++) {
      for (int j = 0; j < N; j++) {
        float gaussianValue =
            (float) Math.exp(-((i - N / 2) * (i - N / 2) / (sigma * sigma) + (j - N / 2)
                * (j - N / 2) / (sigma * sigma)));
        gaussianIndexer.put(i * N + j, gaussianValue);
        gaussianSum += gaussianValue;
      }
    }

    // Normalize gaussian mask
    for (int i = 0; i < N; i++) {
      for (int j = 0; j < N; j++) {
        int index = i * N + j;
        gaussianIndexer.put(index, gaussianIndexer.get(index) * ((N * N) / gaussianSum));
      }
    }
  }

  @Override
  public void map(HipiImageHeader header, FloatImage image, Context context) throws IOException,
      InterruptedException {
    
    int N = Covariance.patchSize;
    
    //convert input FloatImage to grayscale mat
    Mat cvValue = OpenCVUtils.convertFloatImageToMat(image, opencv_core.CV_32FC1);
    
    ArrayList<Mat> patches = new ArrayList<Mat>(); //stores patches computed from image
    ArrayList<FloatIndexer> patchIndexers = new ArrayList<FloatIndexer>(); //stores indexer objects for patches
    
    
    // specify number of 2-D covariance patches to creates (iMax * jMax patches)
    int iMax = 10;
    int jMax = 10;

    for (int i = 0; i < iMax; i++) {
      int x = (cvValue.cols() - N) * i / iMax;
      for (int j = 0; j < jMax; j++) {
        int y = (cvValue.rows() - N) * j / jMax;
        Rect roi = new Rect(x, y, N, N);
        Mat patch = cvValue.apply(roi).clone();
        
        opencv_core.subtract(patch, mean, patch);
        opencv_core.multiply(patch, gaussian, patch);

        Mat clonedPatch = patch.clone();
        patches.add(clonedPatch);
        patchIndexers.add((FloatIndexer) clonedPatch.createIndexer()); //pre-creating mat indexers speeds up running time
      }
    }

    // Stores the (N^2 x N^2) covariance matrix AAt
    Mat covarianceMat = new Mat(N * N, N * N, opencv_core.CV_32FC1, new Scalar(0.0));
    FloatIndexer covarianceIndexer = covarianceMat.createIndexer();
    for (int i = 0; i < N * N; i++) {
      for (int j = 0; j < N * N; j++) {
        float accumulatedValue = 0.0f;
        for (int k = 0; k < patches.size(); k++) {
          accumulatedValue += patchIndexers.get(k).get(i) * patchIndexers.get(k).get(j);
        }
        covarianceIndexer.put(i * N * N + j, accumulatedValue);
      }
    }
    
    // Write out mat encapsulated in openCVMatWritable object
    context.write(new IntWritable(0), new OpenCVMatWritable(covarianceMat));
  }
}

package org.hipi.examples.covar;

import static org.bytedeco.javacpp.opencv_imgproc.CV_RGB2GRAY;
import static org.bytedeco.javacpp.opencv_imgproc.cvtColor;


import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Rect;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.opencv.MatUtils;
import org.hipi.opencv.OpenCVMatWritable;

public class CovarianceMapper extends
    Mapper<HipiImageHeader, FloatImage, IntWritable, OpenCVMatWritable> {

  public static final int N = Covariance.N;
  public static final float sigma = Covariance.sigma;

  Mat mean;
  Mat gaussian;

  @Override
  public void setup(Context job) {
    
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
      mean = meanWritable.getMat().clone();
      FloatIndexer meanIndexer = mean.createIndexer();
      
      System.out.println("mean info: ");
      System.out.println(mean.rows());
      System.out.println(mean.cols());
      
      for(int i = 0; i < 10; i++) {
        System.out.println(meanIndexer.get(i));
      }
     

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

    // Normalize Gaussian mask
    for (int i = 0; i < N; i++) {
      for (int j = 0; j < N; j++) {
        int index = i * N + j;
        gaussianIndexer.put(index, gaussianIndexer.get(index) * ((N * N) / gaussianSum));
      }
    }
//    opencv_highgui.imshow("mean", mean);
//    opencv_highgui.imshow("rst", gaussian);
  }

  @Override
  public void map(HipiImageHeader key, FloatImage value, Context context) throws IOException,
      InterruptedException {
    Mat cvValue = MatUtils.convertFloatImageToMat(value);
    opencv_highgui.imshow("test", cvValue);
    
    System.out.println("MEAN");
    
    FloatIndexer s = mean.createIndexer();
    for(int m = 0; m < 10; m++) {
      System.out.println(s.get(m));
    }

    ArrayList<Mat> patches = new ArrayList<Mat>();
    ArrayList<FloatIndexer> patchIndexers = new ArrayList<FloatIndexer>();

    for (int i = 0; i < 10; i++) {
      int x = (cvValue.cols() - N) * i / 10;
      for (int j = 0; j < 10; j++) {
        int y = (cvValue.rows() - N) * j / 10;
        Rect roi = new Rect(x, y, N, N);
        Mat patch = cvValue.apply(roi).clone();

        Mat grayPatch = new Mat(patch.rows(), patch.cols(), opencv_core.CV_32FC1, new Scalar(0.0));
        cvtColor(patch, grayPatch, CV_RGB2GRAY);
        
//        System.out.println("PATCH BEFORE");
//        FloatIndexer w = patch.createIndexer();
//        for(int m = 0; m < 10; m++) {
//          System.out.println(w.get(m));
//        }
//        
//        System.out.println("GRAY PATCH BEFORE");
//        FloatIndexer q = grayPatch.createIndexer();
//        for(int m = 0; m < 10; m++) {
//          System.out.println(q.get(m));
//        }
        
//        System.out.println("MEAN");
//        
//        FloatIndexer s = mean.createIndexer();
//        for(int m = 0; m < 10; m++) {
//          System.out.println(s.get(m));
//        }
        
//        System.out.println("GAUSSIAN");
//        
//        FloatIndexer h = gaussian.createIndexer();
//        for(int m = 0; m < 10; m++) {
//          System.out.println(h.get(m));
//        }

        opencv_core.subtract(grayPatch, mean, grayPatch);
        opencv_core.multiply(grayPatch, gaussian, grayPatch);

        Mat clonedGrayPatch = grayPatch.clone();
        //opencv_highgui.imshow("test", clonedGrayPatch);
//        System.out.println("PATCH");
//        FloatIndexer p = clonedGrayPatch.createIndexer();
//        for(int m = 0; m < 10; m++) {
//          System.out.println(p.get(m));
//        }
        patches.add(clonedGrayPatch);
        patchIndexers.add((FloatIndexer) clonedGrayPatch.createIndexer()); //pre-creating mat indexers speeds up running time
      }
    }

    // Stores the (N^2 x N^2) covariance matrix AAt
    float[] covarianceArray = new float[N * N * N * N];
    for (int i = 0; i < N * N; i++) {
      for (int j = 0; j < N * N; j++) {
        covarianceArray[i * N * N + j] = 0;
        for (int k = 0; k < patches.size(); k++) {
          covarianceArray[i * N * N + j] +=
              patchIndexers.get(k).get(i) * patchIndexers.get(k).get(j);
        }
      }
    }

    Mat covarianceMat = new Mat(N * N, N * N, opencv_core.CV_32FC1, new Scalar(0.0));
    FloatIndexer fi = covarianceMat.createIndexer();
    System.out.println("Covariance array:");
    for (int i = 0; i < covarianceArray.length; i++) {
      fi.put(i, covarianceArray[i]);
    }
    for (int i = 0; i < 10; i++) {
      System.out.println(fi.get(i));
    }
    opencv_highgui.imshow("test", covarianceMat);

    context.write(new IntWritable(0), new OpenCVMatWritable(covarianceMat));
  }
}

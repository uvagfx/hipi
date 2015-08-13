package org.hipi.examples.covar;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.MatExpr;
import org.bytedeco.javacpp.opencv_core.Rect;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.opencv.OpenCVMatWritable;
import org.hipi.opencv.OpenCVUtils;

public class MeanMapper extends Mapper<HipiImageHeader, FloatImage, IntWritable, OpenCVMatWritable> {

  @Override
  public void map(HipiImageHeader header, FloatImage image, Context context) throws IOException,
      InterruptedException {
    
    int N = Covariance.patchSize;
    
    //convert input FloatImage to grayscale mat
    Mat cvValue = OpenCVUtils.convertFloatImageToMat(image, opencv_core.CV_32FC1);

    //compute mean patch from input image
    Mat mean = new Mat(N, N, opencv_core.CV_32FC1, new Scalar(0.0));
    
    //specify number of patches to use to compute mean patch (iMax * jMax patches)
    int iMax = 100;
    int jMax = 100;

    //collect patches and add their values to mean patch mat
    for (int i = 0; i < iMax; i++) {
      int x = ((cvValue.cols() - N) * i) / iMax;
      for (int j = 0; j < jMax; j++) {
        int y = ((cvValue.rows() - N) * j) / jMax;
        Mat patch = cvValue.apply(new Rect(x, y, N, N));
        opencv_core.add(patch, mean, mean);
      }
    }
    
    mean = opencv_core.multiply(mean, (1.0 / (double)(iMax * jMax))).asMat();
    
    context.write(new IntWritable(0), new OpenCVMatWritable(mean));
  }
}

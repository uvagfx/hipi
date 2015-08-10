package org.hipi.examples.covar;

import static org.bytedeco.javacpp.opencv_imgproc.CV_RGB2GRAY;
import static org.bytedeco.javacpp.opencv_imgproc.cvtColor;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Rect;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.opencv.MatUtils;
import org.hipi.opencv.OpenCVMatWritable;

public class MeanMapper extends Mapper<HipiImageHeader, FloatImage, IntWritable, OpenCVMatWritable> {
  
  public static final int N = Covariance.N;

  @Override
  public void map(HipiImageHeader key, FloatImage value, Context context) throws IOException,
      InterruptedException {

    Mat srcImage = MatUtils.convertFloatImageToMat(value);
    Mat grayScaleSrcImage = new Mat(srcImage.rows(), srcImage.cols(), opencv_core.CV_32FC1, new Scalar(0.0));
    if (srcImage.type() != opencv_core.CV_32FC1) {
      cvtColor(srcImage, grayScaleSrcImage, CV_RGB2GRAY);
    } else {
      grayScaleSrcImage = srcImage;
    }

    Mat mean = new Mat(N, N, opencv_core.CV_32FC1, new Scalar(0.0));

    for (int i = 0; i < 100; i++) {
      int x = (value.getWidth() - N) * i / 100;
      for (int j = 0; j < 100; j++) {
        int y = (value.getHeight() - N) * j / 100;
        Mat valueCropped = grayScaleSrcImage.apply(new Rect(x, y, N, N)).clone();
        opencv_core.add(valueCropped, mean, mean);
      }
    }
    
    int elms = (int)(mean.total() * mean.channels());
    FloatIndexer fi = mean.createIndexer();
    for(int k = 0; k < elms; k++) {
      fi.put(k, (float)(fi.get(k) * (1.0 / (100 * 100))));
    }
    
    opencv_highgui.imshow("arsar", mean);

    context.write(new IntWritable(0), new OpenCVMatWritable(mean));
  }
}

package org.hipi.examples.covar;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.hipi.image.FloatImage;
import org.hipi.opencv.MatUtils;
import org.hipi.opencv.OpenCVMatWritable;

public class CovarianceReducer extends
    Reducer<IntWritable, OpenCVMatWritable, IntWritable, OpenCVMatWritable> {

  public static final int N = Covariance.N;
  
  @Override
  public void reduce(IntWritable key, Iterable<OpenCVMatWritable> values, Context context)
      throws IOException, InterruptedException {
    Mat cov = new Mat(N * N, N * N, opencv_core.CV_32FC1, new Scalar(0.0));
    for(OpenCVMatWritable value : values) {
      opencv_highgui.imshow("arstsr", value.getMat());
      
      FloatIndexer fis = value.getMat().createIndexer();
      for(int i = 0; i < 10; i++) {
        System.out.println(fis.get(i));
      }
      
      opencv_core.add(value.getMat(), cov, cov);
    }
    opencv_highgui.imshow("arstsr", cov);
    System.out.println("");
      FloatIndexer fi = cov.createIndexer();
      for(int i = 0; i < 3000; i++) {
        System.out.println(fi.get(i));
      }
      
    context.write(key, new OpenCVMatWritable(cov));
  }
}

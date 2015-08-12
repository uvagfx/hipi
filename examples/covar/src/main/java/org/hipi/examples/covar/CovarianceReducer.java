package org.hipi.examples.covar;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.hipi.opencv.OpenCVMatWritable;

public class CovarianceReducer extends
    Reducer<IntWritable, OpenCVMatWritable, IntWritable, OpenCVMatWritable> {

  public static final int N = Covariance.N;
  
  @Override
  public void reduce(IntWritable key, Iterable<OpenCVMatWritable> values, Context context)
      throws IOException, InterruptedException {
    Mat cov = new Mat(N * N, N * N, opencv_core.CV_32FC1, new Scalar(0.0));
    for(OpenCVMatWritable value : values) {
      opencv_core.add(value.getMat(), cov, cov);
    }
      
    context.write(key, new OpenCVMatWritable(cov));
  }
}

package org.hipi.examples.covar;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.hipi.opencv.OpenCVMatWritable;

public class MeanReducer extends Reducer<IntWritable, OpenCVMatWritable, IntWritable, OpenCVMatWritable> {

  public static final int N = Covariance.N;
  
  @Override
  public void reduce(IntWritable key, Iterable<OpenCVMatWritable> values, Context context)
      throws IOException, InterruptedException {
    
    //consolidate each mean patch computed in mappers
    Mat mean = new Mat(N, N, opencv_core.CV_32FC1, new Scalar(0.0));
    int total = 0;
    for (OpenCVMatWritable val : values) {
      opencv_core.add(val.getMat(), mean, mean);
      total++;
    }
    
    //normalize consolidated mean patch
    if (total > 0) {
      int elms = (int) (mean.total() * mean.channels());
      FloatIndexer fi = mean.createIndexer();
      for (int k = 0; k < elms; k++) {
        fi.put(k, (fi.get(k) * (1.0f / (float) total)));
      }
    }
    
    //write out consolidated patch
    context.write(key, new OpenCVMatWritable(mean));
  }


}

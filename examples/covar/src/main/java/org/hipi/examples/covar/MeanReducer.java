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
  
  @Override
  public void reduce(IntWritable key, Iterable<OpenCVMatWritable> meanPatches, Context context)
      throws IOException, InterruptedException {
    
    int N = Covariance.patchSize;
    
    //consolidate each mean patch computed in mappers
    Mat mean = new Mat(N, N, opencv_core.CV_32FC1, new Scalar(0.0));
    int total = 0;
    for (OpenCVMatWritable patch : meanPatches) {
      opencv_core.add(patch.getMat(), mean, mean);
      total++;
    }
    
    //normalize consolidated mean patch
    if (total > 0) {
      mean = opencv_core.multiply(mean, (1.0 / (double) total)).asMat();
    }
    
    //write out consolidated patch
    context.write(key, new OpenCVMatWritable(mean));
  }


}

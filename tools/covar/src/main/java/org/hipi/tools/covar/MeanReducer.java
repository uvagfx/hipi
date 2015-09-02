package org.hipi.tools.covar;

import org.hipi.opencv.OpenCVMatWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Scalar;

import java.io.IOException;

public class MeanReducer extends 
  Reducer<IntWritable, OpenCVMatWritable, NullWritable, OpenCVMatWritable> {
  
  @Override
  public void reduce(IntWritable key, Iterable<OpenCVMatWritable> meanPatches, Context context)
      throws IOException, InterruptedException {
    
    int N = Covariance.patchSize;
    
    //consolidate mean patches from mapper
    Mat mean = new Mat(N, N, opencv_core.CV_32FC1, new Scalar(0.0));
    
    int count = 0;
    for (OpenCVMatWritable patch : meanPatches) {
      opencv_core.add(patch.getMat(), mean, mean);
      count++;
    }
    
    //normalize consolidated mean patch
    if (count > 1) {
      mean = opencv_core.divide(mean, (double) count).asMat();
    }
    
    //write out consolidated patch
    context.write(NullWritable.get(), new OpenCVMatWritable(mean));
  }


}

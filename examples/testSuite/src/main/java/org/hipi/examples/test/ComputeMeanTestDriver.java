package org.hipi.examples.test;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hipi.examples.covar.*;

public class ComputeMeanTestDriver extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    return ComputeMean.run(args);
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ComputeMeanTestDriver(), args);
    System.exit(res);
  }
}

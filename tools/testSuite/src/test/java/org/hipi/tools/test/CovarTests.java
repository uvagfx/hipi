package org.hipi.tools.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class CovarTests {
  
  //configuring hdfs for test run (creating benchmark data and loading it onto hdfs)
  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    TestUtils.setupTestoutDirectory(fs);
  }
  
  //Input Error handling tests (should return 1 before job runs)
  @Test
  public void testCovarOneInput() throws IOException {
    assertEquals("Failed to catch invalid input.", 1, TestUtils.runCommand("../covar.sh one"));
  }
  
  @Test
  public void testCovarThreeInputs() throws IOException {
    assertEquals("Failed to catch invalid input.", 1, TestUtils.runCommand("../covar.sh one two three"));
  }
  
  @Test
  public void testCovarInvalidInputHibPath() throws IOException {
    assertEquals("Failed to catch invalid input.", 1, TestUtils.runCommand("../covar.sh badPath testOutDir"));
  }
  
  @Test
  public void testComputeMean() throws IOException {
    assertEquals("Failed to run covariance job. Check setup.", 0, TestUtils.runCommand("../covar.sh testout/covar/input/white-black.hib testout/covar/output"));
    TestUtils.runCommand("rm /tmp/mean-output-opencvmatwritable");
    assertEquals("Failed to copy mean output to local filesystem. Check setup.", 0, TestUtils.runCommand("hadoop fs -copyToLocal testout/covar/output/mean-output/part-r-00000 /tmp/mean-output-opencvmatwritable"));
    assertEquals("Failed to covert openCVMatWritable object to jpg.", true, TestUtils.convertFloatOpenCVMatWritableToJpg("/tmp/mean-output-opencvmatwritable", "/tmp/mean-output.jpg"));
    assertTrue("Psnr does not reach desired threshold.", TestUtils.checkPsnr("/tmp/mean-output.jpg", "../../testdata/covar/mean-benchmark.jpg", 30.0f));
  }
 
  @Test
  public void testComputeCovarianceWithSmallTestHib() throws IOException {
    assertEquals("Failed to run covariance job. Check setup.", 0, TestUtils.runCommand("../covar.sh testout/covar/input/smalltesthib.hib testout/covar/output"));
    TestUtils.runCommand("rm /tmp/covariance-output-opencvmatwritable");
    assertEquals("Failed to copy covariance output to local filesystem. Check setup.", 0, TestUtils.runCommand("hadoop fs -copyToLocal testout/covar/output/covariance-output/part-r-00000 /tmp/covariance-output-opencvmatwritable"));
    assertEquals("Failed to covert openCVMatWritable object to jpg.", true, TestUtils.convertFloatOpenCVMatWritableToJpg("/tmp/covariance-output-opencvmatwritable", "/tmp/covariance-output.jpg"));
    assertTrue("Psnr does not reach desired threshold.", TestUtils.checkPsnr("/tmp/covariance-output.jpg", "../../testdata/covar/covariance-benchmark.jpg", 30.0f));
  }
   
  @Test
  public void testComputeCovarianceWithMediumTestHib() throws IOException {
    assertEquals("Failed to run covariance job. Check setup.", 0, TestUtils.runCommand("../covar.sh testout/covar/input/mediumtesthib.hib testout/covar/output"));
    TestUtils.runCommand("rm /tmp/covariance-output-opencvmatwritable");
    assertEquals("Failed to copy covariance output to local filesystem. Check setup.", 0, TestUtils.runCommand("hadoop fs -copyToLocal testout/covar/output/covariance-output/part-r-00000 /tmp/covariance-output-opencvmatwritable"));
    assertEquals("Failed to covert openCVMatWritable object to jpg.", true, TestUtils.convertFloatOpenCVMatWritableToJpg("/tmp/covariance-output-opencvmatwritable", "/tmp/covariance-output.jpg"));
    assertTrue("Psnr does not reach desired threshold.", TestUtils.checkPsnr("/tmp/covariance-output.jpg", "../../testdata/covar/covariance-benchmark.jpg", 30.0f));
  }
}

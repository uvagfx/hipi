package org.hipi.examples.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageFactory;
import org.hipi.image.io.ImageEncoder;
import org.hipi.image.io.JpegCodec;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class CovarTest {
  
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
    assertEquals("Failed to catch invalid input.", 1, TestUtils.runCommand("../covar.sh /one"));
  }
  
  @Test
  public void testCovarThreeInputs() throws IOException {
    assertEquals("Failed to catch invalid input.", 1, TestUtils.runCommand("../covar.sh /one /two /three"));
  }
  
  @Test
  public void testCovarInvalidInputHibPath() throws IOException {
    assertEquals("Failed to catch invalid input.", 1, TestUtils.runCommand("../covar.sh /badPath /testOutDir"));
  }
  
  @Test
  public void testComputeMean() throws IOException {
    assertEquals("Failed to run covariance job. Check setup.", 0, TestUtils.runCommand("../covar.sh /testout/covar/white-black.hib /tmp/covar"));
    TestUtils.runCommand("rm /tmp/mean-output-opencvmatwritable");
    assertEquals("Failed to copy mean output to local filesystem. Check setup.", 0, TestUtils.runCommand("hadoop fs -copyToLocal /tmp/covar/mean-output/part-r-00000 /tmp/mean-output-opencvmatwritable"));
    assertEquals("Failed to covert openCVMatWritable object to jpg.", true, TestUtils.convertOpenCVMatWritableToJpg("/tmp/mean-output-opencvmatwritable", "/tmp/mean-output.jpg"));
    assertEquals("Psnr does not reach desired threshold.", true, TestUtils.checkPsnr("/tmp/mean-output.jpg", "../../testdata/covar/images/mean-benchmark.jpg", 30.0f));
  }
 
  //psnr on this test should indicate that new covariance image is identical to covariance benchmark because input hibs are identical.
  @Test
  public void testComputeCovariancWithSmallTestHib() throws IOException {
    assertEquals("Failed to run covariance job. Check setup.", 0, TestUtils.runCommand("../covar.sh /testout/covar/smalltesthib.hib /tmp/covar"));
    TestUtils.runCommand("rm /tmp/covariance-output-opencvmatwritable");
    assertEquals("Failed to copy covariance output to local filesystem. Check setup.", 0, TestUtils.runCommand("hadoop fs -copyToLocal /tmp/covar/covariance-output/part-r-00000 /tmp/covariance-output-opencvmatwritable"));
    assertEquals("Failed to covert openCVMatWritable object to jpg.", true, TestUtils.convertOpenCVMatWritableToJpg("/tmp/covariance-output-opencvmatwritable", "/tmp/covariance-output.jpg"));
    assertEquals("Psnr does not reach desired threshold.", true, TestUtils.checkPsnr("/tmp/covariance-output.jpg", "../../testdata/covar/images/covariance-benchmark.jpg", 30.0f));
  }
    
  //psnr on this test should indicate that new covariance image is appoximate to covariance benchmark, but will not be identical because input hibs are different.
  @Test
  public void testComputeCovariancWithMediumTestHib() throws IOException {
    assertEquals("Failed to run covariance job. Check setup.", 0, TestUtils.runCommand("../covar.sh /testout/covar/mediumtesthib.hib /tmp/covar"));
    TestUtils.runCommand("rm /tmp/covariance-output-opencvmatwritable");
    assertEquals("Failed to copy covariance output to local filesystem. Check setup.", 0, TestUtils.runCommand("hadoop fs -copyToLocal /tmp/covar/covariance-output/part-r-00000 /tmp/covariance-output-opencvmatwritable"));
    assertEquals("Failed to covert openCVMatWritable object to jpg.", true, TestUtils.convertOpenCVMatWritableToJpg("/tmp/covariance-output-opencvmatwritable", "/tmp/covariance-output.jpg"));
    assertEquals("Psnr does not reach desired threshold.", true, TestUtils.checkPsnr("/tmp/covariance-output.jpg", "../../testdata/covar/images/covariance-benchmark.jpg", 10.0f));
  }
}

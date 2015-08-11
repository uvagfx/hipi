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
  
  @Ignore
  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    TestUtils.createTestData(fs);
    TestUtils.setupTestoutDirectory(fs);
  }
  
  //Input Error handling tests
  @Test
  public void testComputeMeanInvalidNumberOfInputs() throws IOException {
      assertEquals("Failed to run mean. Check setup.", 0, TestUtils.runCommand("./computeMean.sh onlyOneInput"));
  }

  @Test
  public void testComputeCovarianceInvalidNumberOfInputs() throws IOException {
      assertEquals("Failed to run covariance. Check setup.", 0, TestUtils.runCommand("./computeCovariance.sh onlyOneInput"));
  }
  
  @Test 
  public void testComputeMeanInvalidInputPath() throws IOException {
      TestUtils.runCommand("./computeMean.sh invalidInput invalidOutput");
  }
  
  @Test
  public void testComputeCovarianceInvalidInputPath() throws IOException {
      TestUtils.runCommand("./computeCovariance.sh invalidInput invalidOutput");
  }
  
  //Compute mean implementation tests
  @Test
  public void testComputeMean() throws IOException {
    assertEquals("Failed to run mean. Check setup.", 0, TestUtils.runCommand("./computeMean.sh /testout/covar/white-black.hib /tmp/covar"));
    TestUtils.runCommand("rm /tmp/mean-output-opencvmatwritable");
    assertEquals("Failed to run mean. Check setup.", 0, TestUtils.runCommand("hadoop fs -copyToLocal /tmp/covar/mean-output/part-r-00000 /tmp/mean-output-opencvmatwritable"));
    assertEquals("Failed to covert openCVMatWritable object to jpg.", true, TestUtils.convertOpenCVMatWritableToJpg("/tmp/mean-output-opencvmatwritable", "/tmp/mean-output.jpg"));
    assertEquals("Images are not equivalent.", true, TestUtils.checkPsnr("/tmp/mean-output.jpg", "../../testdata/covar/images/mean.jpg", 30.0f));
  }
  
  @Test
  public void testComputeCovarianceWithSmallTestHib() throws IOException {
    assertEquals("Failed to run mean. Check setup.", 0, TestUtils.runCommand("./computeMean.sh /testout/covar/smalltesthib.hib /tmp/covar"));
    assertEquals("Failed to run covariance. Check setup.", 0, TestUtils.runCommand("./computeCovariance.sh /testout/covar/smalltesthib.hib /tmp/covar"));
    TestUtils.runCommand("rm /tmp/covariance-output-opencvmatwritable");
    assertEquals("Failed to run covariance. Check setup.", 0, TestUtils.runCommand("hadoop fs -copyToLocal /tmp/covar/covariance-output/part-r-00000 /tmp/covariance-output-opencvmatwritable"));
    assertEquals("Failed to covert openCVMatWritable object to jpg.", true, TestUtils.convertOpenCVMatWritableToJpg("/tmp/covariance-output-opencvmatwritable", "/tmp/covariance-output.jpg"));
    assertEquals("Images are not equivalent.", true, TestUtils.checkPsnr("/tmp/covariance-output.jpg", "../../testdata/covar/images/covariance-benchmark.jpg", 30.0f));
  }
  
  //psnr on this test should indicate that new covariance value is appoximate to benchmark, but will not be identical because input images are different.
  @Test
  public void testComputeCovarianceWithMediumTestHib() throws IOException {
    assertEquals("Failed to run mean. Check setup.", 0, TestUtils.runCommand("./computeMean.sh /testout/covar/mediumtesthib.hib /tmp/covar"));
    assertEquals("Failed to run covariance. Check setup.", 0, TestUtils.runCommand("./computeCovariance.sh /testout/covar/mediumtesthib.hib /tmp/covar"));
    TestUtils.runCommand("rm /tmp/covariance-output-opencvmatwritable");
    assertEquals("Failed to run covariance. Check setup.", 0, TestUtils.runCommand("hadoop fs -copyToLocal /tmp/covar/covariance-output/part-r-00000 /tmp/covariance-output-opencvmatwritable"));
    assertEquals("Failed to covert openCVMatWritable object to jpg.", true, TestUtils.convertOpenCVMatWritableToJpg("/tmp/covariance-output-opencvmatwritable", "/tmp/covariance-output.jpg"));
    assertEquals("Images are not equivalent.", true, TestUtils.checkPsnr("/tmp/covariance-output.jpg", "../../testdata/covar/images/covariance-benchmark.jpg", 10.0f));
  }

}

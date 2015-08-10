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
  public static void setup() throws Exception {
    Configuration conf = new Configuration();
    //TestUtils.createTestData();
    FileSystem fs = FileSystem.get(conf);
    //TestUtils.setupTestoutDirectory(fs);
  }
  
  //Input Error handling tests
  @Ignore
  @Test
  public void testComputeMeanInvalidNumberOfInputs() throws Exception {
      assertEquals("Failed to run mean. Check setup.", 0, TestUtils.runCommand("./computeMean.sh onlyOneInput"));
  }

  @Ignore
  @Test
  public void testComputeCovarianceInvalidNumberOfInputs() throws Exception {
      assertEquals("Failed to run covariance. Check setup.", 0, TestUtils.runCommand("./computeCovariance.sh onlyOneInput"));
  }
  
  @Ignore
  @Test(expected = IllegalArgumentException.class ) 
  public void testComputeMeanInvalidInputPath() throws Exception {
      TestUtils.runCommand("./computeMean.sh invalidInput invalidOutput");
  }
  
  @Ignore
  @Test(expected = IllegalArgumentException.class )
  public void testComputeCovarianceInvalidInputPath() throws Exception {
      TestUtils.runCommand("./computeCovariance.sh invalidInput invalidOutput");
  }
  
  //Compute mean implementation tests
  @Ignore
  @Test
  public void testComputeMean() throws Exception {
    assertEquals("Failed to run mean. Check setup.", 0, TestUtils.runCommand("./computeMean.sh /testout/covar/white-black.hib /tmp/covar"));
    TestUtils.runCommand("rm /tmp/mean-output-opencvmatwritable");
    assertEquals("Failed to run mean. Check setup.", 0, TestUtils.runCommand("hadoop fs -copyToLocal /tmp/covar/mean-output/part-r-00000 /tmp/mean-output-opencvmatwritable"));
    assertEquals("Mats are not equivalent.", true, TestUtils.compareOpenCVWritableToBenchmark("/tmp/mean-output-opencvmatwritable", "../../testData/covar/images/mean.jpg", 0.05f));
  }
  
  //Compute covariance implementation tests
  //TODO: think about how to test covariance results
  @Ignore
  @Test
  public void testComputeCovariance() throws Exception {
    assertEquals("Failed to run mean. Check setup.", 0, TestUtils.runCommand("./computeMean.sh /testout/covar/smalltesthib.hib /tmp/covar"));
    assertEquals("Failed to run covariance. Check setup.", 0, TestUtils.runCommand("./computeCovariance.sh /testout/covar/smalltesthib.hib /tmp/covar"));
    TestUtils.runCommand("rm /tmp/covariance-output-opencvmatwritable");
    assertEquals("Failed to run covariance. Check setup.", 0, TestUtils.runCommand("hadoop fs -copyToLocal /tmp/covar/covariance-output/part-r-00000 /tmp/covariance-output-opencvmatwritable"));
    assertEquals("Mats are not equivalent.", true, TestUtils.compareOpenCVWritableToBenchmark("/tmp/covariance-output-opencvmatwritable", "../../testData/covar/images/covariance-benchmark.jpg", 0.05f));
  }

}

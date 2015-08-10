package org.hipi.examples.test;

import static org.junit.Assert.*;

import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_imgproc;
import org.bytedeco.javacpp.opencv_imgcodecs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.ToolRunner;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.hipi.opencv.OpenCVMatWritable;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Scanner;

public class TestUtils {

  public static void setupTestoutDirectory(FileSystem fs) throws Exception {
    if (fs.exists(new Path("skipsetup"))) {
      return;
    }
    TestUtils.runCommand("hadoop fs -rm -r -f /testout");
    TestUtils.runCommand("hadoop fs -rm -r -f /tmp");
    assertEquals("Failed to create testout directory on HDFS. Check setup.", 0, TestUtils.runCommand("hadoop fs -mkdir -p /testout"));
    assertEquals("Failed to create testout directory on HDFS. Check setup.", 0, TestUtils.runCommand("hadoop fs -mkdir -p /tmp"));
    assertEquals("Failed to create testout/covar directory on HDFS. Check setup.", 0, TestUtils.runCommand("hadoop fs -mkdir -p /testout/covar"));
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/covar/hibs/white-black.hib /testout/covar/white-black.hib");
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/covar/hibs/white-black.hib.dat /testout/covar/white-black.hib.dat");
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/covar/hibs/smalltesthib.hib /testout/covar/smalltesthib.hib");
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/covar/hibs/smalltesthib.hib.dat /testout/covar/smalltesthib.hib.dat");
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/covar/images/mean.jpg /testout/covar/mean.jpg");
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/covar/images/covariance-benchmark.jpg /testout/covar/covariance-benchmark.jpg");
  }

  public static boolean checkPsnr(String imgPath, String truthPath, float thresh) 
  throws IOException {
    Runtime rt = Runtime.getRuntime();
    String cmd = "compare -metric PSNR " + imgPath + " " + truthPath + " /tmp/psnr.png";
    System.out.println(cmd);
    Process pr = rt.exec(cmd);
    Scanner scanner = new Scanner(new InputStreamReader(pr.getErrorStream()));
    float psnr = scanner.hasNextFloat() ? scanner.nextFloat() : 0;
    System.out.println("PSNR: " + psnr);
    //    assertTrue("PSNR is too low : " + psnr, psnr > 30);
    return (psnr >= thresh);
  } 
  
  
  //used to create test hibs - kept here for record-keeping purposes but shouldn't be run in normal testing
  public static void createTestData() throws Exception {
    
    //used to create white-black.hib and a mean benchmark image
    Mat whiteMat = new Mat(500, 500, opencv_core.CV_32FC1, new Scalar(255.0));
    Mat blackMat = new Mat(500, 500, opencv_core.CV_32FC1, new Scalar(0.0));
    Mat meanMat = new Mat(48, 48, opencv_core.CV_32FC1, new Scalar(127.5f));
    opencv_imgcodecs.imwrite("../../testData/covar/images/white.jpg", whiteMat);
    opencv_imgcodecs.imwrite("../../testData/covar/images/black.jpg", blackMat);
    opencv_imgcodecs.imwrite("../../testData/covar/images/mean.jpg", meanMat);
    
    DataInputStream dis = new DataInputStream(new FileInputStream("../../testData/covar/covar-opencvmw")); 
    dis.skip(4);
    OpenCVMatWritable openCVMatWritable = new OpenCVMatWritable();
    openCVMatWritable.readFields(dis);
    Mat mat = openCVMatWritable.getMat();
    mat.convertTo(mat, opencv_core.CV_32FC1);
    opencv_imgcodecs.imwrite("../../testData/covar/images/covariance-benchmark.jpg", mat);
    
  }
  
  
  //TODO: think about better way to implement comparison. Currently converting benchmark to mat to provide more fine-tuned control over data type.
  //There's probably a better way to do this.
  public static boolean compareOpenCVWritableToBenchmark(String openCVMatWritablePath, String benchmarkImagePath, float thresh) throws IOException {
    DataInputStream dis = new DataInputStream(new FileInputStream(openCVMatWritablePath));  
    dis.skip(4);
    OpenCVMatWritable openCVMatWritable = new OpenCVMatWritable();
    openCVMatWritable.readFields(dis);
    Mat mat = openCVMatWritable.getMat();
    mat.convertTo(mat, opencv_core.CV_32FC1);
    FloatIndexer fiNew = mat.createIndexer();
    
    //convert to (0.0, 255.0) range from (0.0, 1.0) range for comparison to read-in jpg
    //also requires rounding because benchmark image isn't stored using floats
    int elems = (int)(mat.total() * mat.channels());
    for(int i = 0; i < elems; i++) {
     
//      fiNew.put(i, fiNew.get(i));        
      //fiNew.put(i, Math.round((float)(fiNew.get(i) * 255.0f)));
    }

    Mat benchmark = opencv_imgcodecs.imread(benchmarkImagePath);
    benchmark.convertTo(benchmark, opencv_core.CV_32FC1);
    FloatIndexer fiBench = benchmark.createIndexer();
    
    for(int i = 0; i < elems; i++) {
      System.out.println(fiNew.get(i)  + ", " +  fiBench.get(i));
      //fiNew.put(i, Math.round((float)(fiNew.get(i) * 255.0f)));
    }
    
    if(mat.rows() != benchmark.rows() || mat.cols() != benchmark.cols()) {
      System.out.println("Sizes of mats are different!");
      System.out.println("benchmark: (" + benchmark.rows() + "," + benchmark.cols() +")");
      System.out.println("testData: (" + mat.rows() + "," + mat.cols() +")");
      return false;
    }
    
    for(int i = 0; i < elems; i++) {
      if(Math.abs(fiNew.get(i) - fiBench.get(i)) >= thresh) {
        System.out.println("Difference between vals is too great: " + fiNew.get(i) + ", " + fiBench.get(i));
        return false;
      }
    }
    return true;
    
    
  }

  public static int runCommand(String cmd) throws Exception {
    
    Runtime rt = Runtime.getRuntime();
    System.out.println(cmd);
    
    Process proc = rt.exec(cmd);

    BufferedReader stdInput = new BufferedReader(new 
     InputStreamReader(proc.getInputStream()));

    BufferedReader stdError = new BufferedReader(new 
     InputStreamReader(proc.getErrorStream()));

    // read the output from the command
    System.out.println("<STDOUT>");
    String s = null;
    while ((s = stdInput.readLine()) != null) {
      System.out.println(s);
    }
    System.out.println("</STDOUT>");
    
    boolean illegalArgument = false;
    // read any errors from the attempted command
    System.out.println("<STDERR>");
    
    while ((s = stdError.readLine()) != null) {
      System.out.println(s);
      if(s.contains("IllegalArgumentException")) {
       illegalArgument = true;
      }
    }
    System.out.println("</STDERR>");
    
    if(illegalArgument) {
      throw new IllegalArgumentException();
    }

    int exitVal = -1;
    try {
      exitVal = proc.waitFor();
    } catch (InterruptedException ex) {
      fail(ex.getLocalizedMessage());
    }
    System.out.println("EXITVAL: " + exitVal);

    return exitVal;
  }

  public static void runJar(String[] argv) {
    try {
      RunJar.main(argv);
    } catch (Throwable t) {
      fail(t.getLocalizedMessage());
    }
  }

  public static void runJar(ArrayList<String> argList) {
    String[] argv = new String[argList.size()];
    argv = argList.toArray(argv);
    runJar(argv);
  }

  public static void runHibImport(boolean overwrite, String sourceDir, String outputHib) {
    ArrayList<String> argList = new ArrayList<String>();
    argList.add("hibImport/build/install/hibImport/lib/hibImport.jar");
    if (overwrite) {
      argList.add("-f");
    }
    argList.add(sourceDir);
    argList.add(outputHib);
    runJar(argList);
  }



}
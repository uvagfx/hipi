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
import org.bytedeco.javacpp.indexer.UByteBufferIndexer;
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

  public static void setupTestoutDirectory(FileSystem fs) throws IOException {
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
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/covar/hibs/mediumtesthib.hib /testout/covar/mediumtesthib.hib");
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/covar/hibs/mediumtesthib.hib.dat /testout/covar/mediumtesthib.hib.dat");
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
    
    if (scanner.hasNext("inf")) {
      psnr = Float.MAX_VALUE;
      System.out.println("Images are identical (psnr == Float.MAX_VALUE)");
    }
    System.out.println("PSNR: " + psnr);
    return (psnr >= thresh);
  }
  
  public static boolean convertOpenCVMatWritableToJpg(String inputPath, String outputPath) throws IOException {
    DataInputStream dis = new DataInputStream(new FileInputStream(inputPath));  
    dis.skip(4);
    OpenCVMatWritable openCVMatWritable = new OpenCVMatWritable();
    openCVMatWritable.readFields(dis);
    Mat mat = openCVMatWritable.getMat();
    
    //convert mat to unsigned byte for saving as jpg in order to check psnr
    FloatIndexer floatIndexer = mat.createIndexer();
    for(int i = 0; i < (int)(mat.total() * mat.channels()); i++) {
      floatIndexer.put(i, floatIndexer.get(i) * 255.0f);
    }
    mat.convertTo(mat, opencv_core.CV_8UC1);
    
    opencv_imgcodecs.imwrite(outputPath, mat);
    return true;
  }
  
  
  //used to create test data - kept here for record-keeping purposes but shouldn't be run in normal testing
  public static void createTestData(FileSystem fs) throws IOException {
    if (fs.exists(new Path("skipsetup"))) {
      return;
    }
    //used to create white-black.hib and a mean benchmark image
    Mat whiteMat = new Mat(500, 500, opencv_core.CV_8UC1, new Scalar(255.0));
    Mat blackMat = new Mat(500, 500, opencv_core.CV_8UC1, new Scalar(0.0));
    Mat meanMat = new Mat(48, 48, opencv_core.CV_8UC1, new Scalar(127.5f));
    opencv_imgcodecs.imwrite("../../testData/covar/images/white.jpg", whiteMat);
    opencv_imgcodecs.imwrite("../../testData/covar/images/black.jpg", blackMat);
    opencv_imgcodecs.imwrite("../../testData/covar/images/mean.jpg", meanMat);
    
    DataInputStream dis = new DataInputStream(new FileInputStream("../../testData/covar/covariance-benchmark-opencvmatwritable")); 
    dis.skip(4);
    OpenCVMatWritable openCVMatWritable = new OpenCVMatWritable();
    openCVMatWritable.readFields(dis);
    Mat mat = openCVMatWritable.getMat();
    //convert mat to unsigned byte for saving as jpg in order to check psnr
    FloatIndexer floatIndexer = mat.createIndexer();
    for(int i = 0; i < (int)(mat.total() * mat.channels()); i++) {
      floatIndexer.put(i, floatIndexer.get(i) * 255.0f);
    }
    mat.convertTo(mat, opencv_core.CV_8UC1);
    opencv_imgcodecs.imwrite("../../testData/covar/images/covariance-benchmark.jpg", mat);
    
  }

  public static int runCommand(String cmd) throws IOException {
    
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
    
    // read any errors from the attempted command
    System.out.println("<STDERR>");   
    while ((s = stdError.readLine()) != null) {
      System.out.println(s);
    }
    System.out.println("</STDERR>");
    

    int exitVal = -1;
    try {
      exitVal = proc.waitFor();
    } catch (InterruptedException ex) {
      fail(ex.getLocalizedMessage());
    }
    System.out.println("EXITVAL: " + exitVal);

    return exitVal;
  }



}
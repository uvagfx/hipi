package org.hipi.tools.test;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.ToolRunner;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Scanner;

public class TestUtils {

  private static boolean setupOnce = false;

  public static void setupTestoutDirectory(FileSystem fs) throws IOException {
    if (setupOnce) {
      return;
    }
    if (fs.exists(new Path("skipsetup"))) {
      return;
    }
    TestUtils.runCommand("hadoop fs -rm -r -f testout");
    assertEquals("Failed to create testout directory on HDFS. Check setup.", 0, TestUtils.runCommand("hadoop fs -mkdir -p testout"));
//    assertEquals("Failed to create testout/jpeg-rgb on HDFS. Check setup.", 0, TestUtils.runCommand("hadoop fs -mkdir -p testout/jpeg-rgb"));
    assertEquals("Failed to create testout/downloader_src directory on HDFS. Check setup.", 0, TestUtils.runCommand("hadoop fs -mkdir -p testout/downloader_src"));
    assertEquals("Failed to create testout/flickr_src directory on HDFS. Check setup.", 0, TestUtils.runCommand("hadoop fs -mkdir -p testout/flickr_src"));
    assertEquals("Failed to create testout/flickr_bz2_src directory on HDFS. Check setup.", 0, TestUtils.runCommand("hadoop fs -mkdir -p testout/flickr_bz2_src"));
//    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/jpeg-rgb/*.jpg testout/jpeg-rgb/.");
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/jpeg-rgb testout/jpeg-rgb");    
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/downloader-images.txt testout/downloader_src/downloader-images.txt");
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/yfcc100m_dataset-100-temp-0 testout/flickr_src/yfcc100m_dataset-100-temp-0");
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/yfcc100m_dataset-100-temp-1 testout/flickr_src/yfcc100m_dataset-100-temp-1");
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/yfcc100m_dataset-100-temp-0.bz2 testout/flickr_bz2_src/yfcc100m_dataset-100-temp-0.bz2");
    TestUtils.runCommand("hadoop fs -copyFromLocal ../../testdata/yfcc100m_dataset-100-temp-1.bz2 testout/flickr_bz2_src/yfcc100m_dataset-100-temp-1.bz2");
    setupOnce = true;
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
    return (psnr >= thresh);
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
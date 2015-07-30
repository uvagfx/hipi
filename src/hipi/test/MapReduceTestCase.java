package hipi.test;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import org.junit.Test;
import org.junit.Ignore;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Scanner;

public class MapReduceTestCase {

  private static int runCommand(String cmd) throws IOException {

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

  @BeforeClass
  public static void setup() throws IOException {
    runCommand("hadoop fs -rm -R -f testout");
    assertEquals("Failed to create testout directory on HDFS. Check setup.", 0, runCommand("hadoop fs -mkdir -p testout"));
    assertEquals("Failed to create testout/flickr_src directory on HDFS. Check setup.", 0, runCommand("hadoop fs -mkdir -p testout/flickr_src"));
    assertEquals("Failed to create testout/flickr_bz2_src directory on HDFS. Check setup.", 0, runCommand("hadoop fs -mkdir -p testout/flickr_bz2_src"));
    runCommand("hadoop fs -copyFromLocal testdata/downloader-images.txt testout/downloader-images.txt");
    runCommand("hadoop fs -copyFromLocal testdata/yfcc100m_dataset-100-temp-0 testout/flickr_src/yfcc100m_dataset-100-temp-0");
    runCommand("hadoop fs -copyFromLocal testdata/yfcc100m_dataset-100-temp-1 testout/flickr_src/yfcc100m_dataset-100-temp-1");
    runCommand("hadoop fs -copyFromLocal testdata/yfcc100m_dataset-100-temp-0.bz2 testout/flickr_bz2_src/yfcc100m_dataset-100-temp-0.bz2");
    runCommand("hadoop fs -copyFromLocal testdata/yfcc100m_dataset-100-temp-1.bz2 testout/flickr_bz2_src/yfcc100m_dataset-100-temp-1.bz2");
    assertEquals("Failed to remove testout/import.hib and testout/import.hib.dat. Check setup.", 0, runCommand("hadoop fs -rm -r -f testout/import.hib testout/import.hib.dat"));
    assertEquals("Failed to remove testout/downloader.hib and testout/downloader.hib.dat. Check setup.", 0, runCommand("hadoop fs -rm -r -f testout/downloader.hib testout/downloader.hib.dat testout/downloader.hib_output"));
    assertEquals("Failed to remove testout/flickr.hib and testout/flickr.hib.dat. Check setup.", 0, runCommand("hadoop fs -rm -r -f testout/flickr.hib testout/flickr.hib.dat testout/flickr.hib_output"));
  }

  @Test
  public void testHibImport() throws IOException {
    assertEquals("Failed to run hibimport. Check setup.", 0, runCommand("hadoop jar bin/hibImport.jar -f ./testdata/jpeg-rgb testout/import.hib"));
    assertEquals("Failed to extract image 4 from testout/import.hib. Check setup.", 0, runCommand("hadoop jar bin/hibInfo.jar testout/import.hib 4 --extract /tmp/test.png"));
    assertTrue("Image 4 in testout/import.hib does match expected value.", ImageComparisonUtils.checkPsnr("testdata/jpeg-rgb/05.jpg", "/tmp/test.png", 30.0f));
    assertEquals("Failed to run hibimport. Check setup.", 0, runCommand("hadoop jar bin/hibImport.jar -f ./testdata/jpeg-and-png testout/import.hib"));
    assertEquals("Failed to extract image 1 from testout/import.hib. Check setup.", 0, runCommand("hadoop jar bin/hibInfo.jar testout/import.hib 1 --extract /tmp/test.jpg"));
    assertTrue("Image 1 in testout/import.hib does match expected value.", ImageComparisonUtils.checkPsnr("testdata/jpeg-and-png/01.png", "/tmp/test.jpg", 30.0f));
  }
  
  @Test
  public void testDownloader() throws IOException {
    assertEquals("Failed to run downloader. Check setup.", 0, runCommand("hadoop jar bin/downloader.jar testout/downloader-images.txt testout/downloader.hib 10"));
  }

  @Test
  public void testFlickrDownloader() throws IOException {
    assertEquals("Failed to run flickrDownloader. Check setup.", 0, runCommand("hadoop jar bin/flickrDownloader.jar testout/flickr_src testout/flickr.hib 10"));    
    assertEquals("Failed to run flickrDownloader. Check setup.", 0, runCommand("hadoop jar bin/flickrDownloader.jar testout/flickr_bz2_src testout/flickr_bz2.hib 10"));    
  }

  @Ignore
  @Test
  public void testCovarExample() {
  }

}

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
    assertEquals("Failed to create testout directory on HDFS. Check setup.", 0, runCommand("hadoop fs -mkdir -p testout"));
  }

  @Ignore
  @Test
  public void testHibImport() throws IOException {
    assertEquals("Failed to remove testout/import.hib and testout/import.hib.dat. Check setup.", 0, runCommand("hadoop fs -rm -r -f testout/import.hib testout/import.hib.dat"));
    assertEquals("Failed to run hibimport. Check setup.", 0, runCommand("hadoop jar util/hibimport.jar ./testimages/jpeg-rgb testout/import.hib"));
  }
  
  @Ignore
  @Test
  public void testDownloader() throws IOException {
    assertEquals("Failed to remove testout/downloader.hib and testout/downloader.hib.dat. Check setup.", 0, runCommand("hadoop fs -rm -r -f testout/downloader.hib testout/downloader.hib.dat testout/downloader.hib_output"));
    assertEquals("Failed to remove testout/downloader-images.txt. Check setup.", 0, runCommand("hadoop fs -rm -r -f testout/downloader-images.txt"));
    assertEquals("Failed to stage downloader-images.txt on HDFS. Check setup.", 0, runCommand("hadoop fs -copyFromLocal testimages/downloader-images.txt testout/downloader-images.txt"));
    assertEquals("Failed to run downloader. Check setup.", 0, runCommand("hadoop jar util/downloader.jar testout/downloader-images.txt testout/downloader.hib 10"));    
  }

  @Test
  public void testFlickrDownloader() throws IOException {
  }

  @Ignore
  @Test
  public void testCovarExample() {
  }

}

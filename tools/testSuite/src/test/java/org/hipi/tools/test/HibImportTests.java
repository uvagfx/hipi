package org.hipi.tools.test;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import org.hipi.tools.test.HibDump;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Scanner;

public class HibImportTests {

  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    TestUtils.setupTestoutDirectory(fs);
  }

  @Test
  public void testHibImportLocalFS() throws IOException {
    assertEquals("Failed to run hibImport. Check setup.", 0, TestUtils.runCommand("../hibImport.sh -f ../../testdata/jpeg-and-png testout/import.hib"));
    assertEquals("Failed to extract image 1 from testout/import.hib. Check setup.", 0, TestUtils.runCommand("../hibInfo.sh testout/import.hib --show-meta 1 --extract /tmp/test.jpg"));
    assertTrue("Image 1 in testout/import.hib does match expected value.", TestUtils.checkPsnr("../../testdata/jpeg-and-png/01.png", "/tmp/test.jpg", 30.0f));
  }

  @Test
  public void testHibImportHDFS() throws IOException {
    assertEquals("Failed to run hibImport. Check setup.", 0, TestUtils.runCommand("../hibImport.sh -f -h testout/jpeg-rgb testout/import.hib"));
    assertEquals("Failed to extract image 1 from testout/import.hib. Check setup.", 0, TestUtils.runCommand("../hibInfo.sh testout/import.hib --show-meta 6 --extract /tmp/test.jpg"));
    assertTrue("Image 6 in testout/import.hib does match expected value.", TestUtils.checkPsnr("../../testdata/jpeg-rgb/cat.jpg", "/tmp/test.jpg", 30.0f));
  }

  @Test
  public void testHibImportAndCull() throws IOException {
    assertEquals("Failed to run hibImport. Check setup.", 0, TestUtils.runCommand("../hibImport.sh -f ../../testdata/jpeg-rgb testout/import.hib"));
    assertEquals("Failed to extract image 4 from testout/import.hib. Check setup.", 0, TestUtils.runCommand("../hibInfo.sh testout/import.hib --show-meta 4 --extract /tmp/test.png"));
    assertTrue("Image 4 in testout/import.hib does match expected value.", TestUtils.checkPsnr("../../testdata/jpeg-rgb/05.jpg", "/tmp/test.png", 30.0f));
    TestUtils.runCommand("../runTool.sh ./build/libs/hibDump.jar testout/import.hib testout/import_dump");
    TestUtils.runCommand("rm -rf import_dump");
    TestUtils.runCommand("hadoop fs -copyToLocal testout/import_dump");
    File truthFile = new File("../../testdata/culltest_dump");
    File outputFile = new File("./import_dump/part-r-00000");
    assertTrue("Output of hibDump with cull did not meet expectation.", FileUtils.contentEquals(truthFile,outputFile));
  }
  
}
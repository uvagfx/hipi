package org.hipi.tools.test;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.junit.Test;
import org.junit.Ignore;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Scanner;

public class DownloaderTests {

  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    TestUtils.setupTestoutDirectory(fs);
  }

  @Test
  public void testDownloader() throws IOException {
    assertEquals("Failed to run downloader.", 0, TestUtils.runCommand("../hibDownload.sh -f --num-nodes 5 testout/downloader_src testout/downloader.hib"));
    assertEquals("Failed to extract image 0 from testout/downloader.hib.", 0, TestUtils.runCommand("../hibInfo.sh testout/downloader.hib 0 --extract /tmp/test.jpg"));
    assertTrue("Image 0 in testout/downloader.hib does match expected value.", TestUtils.checkPsnr("../../testdata/testimages/01.jpg", "/tmp/test.jpg", 30.0f));
    assertEquals("Failed to extract image 4 from testout/downloader.hib.", 0, TestUtils.runCommand("../hibInfo.sh testout/downloader.hib 4 --extract /tmp/test.jpg"));
    assertTrue("Image 4 in testout/downloader.hib does match expected value.", TestUtils.checkPsnr("../../testdata/testimages/05.jpg", "/tmp/test.jpg", 30.0f));
    assertEquals("Failed to extract image 11 from testout/downloader.hib.", 0, TestUtils.runCommand("../hibInfo.sh testout/downloader.hib 11 --extract /tmp/test.jpg"));
    assertTrue("Image 11 in testout/downloader.hib does match expected value.", TestUtils.checkPsnr("../../testdata/testimages/12.png", "/tmp/test.jpg", 30.0f));
  }

  @Test
  public void testFlickrDownloader() throws IOException {
    assertEquals("Failed to run flickrDownloader.", 0, TestUtils.runCommand("../hibDownload.sh --yfcc100m -f testout/flickr_src testout/flickr.hib"));
    assertEquals("Failed to run flickrDownloader.", 0, TestUtils.runCommand("../hibDownload.sh --yfcc100m -f testout/flickr_bz2_src testout/flickr_bz2.hib"));
  }

}
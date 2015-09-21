package org.hipi.test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.lang.System;

public class TestUtils {

  public static String getTmpPath(String file) {
    String userTmpPath = System.getProperty("user.home") + "/tmp";
    if (file != null && file.length() > 0) {
      userTmpPath += "/" + file;
    }
    return userTmpPath; 
  }

  private static boolean setupOnce = false;

  // Create user tmp directory if it doesn't already exist
  public static void setupTmpDirectory() throws IOException {
    if (setupOnce) {
      return;
    }
    File userTmpDir = new File(TestUtils.getTmpPath(null));
    try {
      if (!userTmpDir.exists()) {
        if (!userTmpDir.mkdir()) {
          fail("Failed to create temp directory: " + userTmpDir.getPath());
        } else {
          System.out.println("Created temp directory: " + userTmpDir.getPath());
        }
      } 
    } catch(SecurityException se) {
      fail(se.getMessage());
    }
    setupOnce = true;
  }

}
package org.hipi.test;

import java.io.InputStreamReader;
import java.util.Scanner;
import java.io.IOException;

public class ImageComparisonUtils {

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

}

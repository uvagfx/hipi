package org.hipi.test;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.image.io.ImageDecoder;
import org.hipi.image.io.ImageEncoder;
import org.hipi.image.io.PngCodec;

import org.junit.Test;
import org.junit.Ignore;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

public class PngCodecTestCase {

  /*
  @Test
  public void testTwelveMonkeysPlugIn() {
    boolean foundTwelveMonkeys = false;
    Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("PNG");
    while (readers.hasNext()) {
      ImageReader imageReader = readers.next();
      System.out.println("ImageReader: " + imageReader);
    }
    assertTrue("FATAL ERROR: failed to locate TwelveMonkeys ImageIO plugins", foundTwelveMonkeys);
  }
  */

  @Test
  public void testDecodeImageHeader() throws IOException {
    ImageDecoder decoder = PngCodec.getInstance();
    FileInputStream fis;
    HipiImageHeader header;
    String[] fileName =
       {"canon-ixus", "fujifilm-dx10", "fujifilm-finepix40i", "fujifilm-mx1700", "kodak-dc210",
        "kodak-dc240", "nikon-e950", "olympus-c960", "ricoh-rdc5300", "sanyo-vpcg250",
        "sanyo-vpcsx550", "sony-cybershot", "sony-d700"};
    int[] width = {640, 1024, 600, 640, 640, 640, 800, 640, 896, 640, 640, 640, 672};
    int[] height = {480, 768, 450, 480, 480, 480, 600, 480, 600, 480, 480, 480, 512};
    int[] bit_depth = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
    for (int i = 0; i < fileName.length; i++) {
      fis = new FileInputStream("../testdata/png-exif-test/" + fileName[i] + ".png");
      header = decoder.decodeHeader(fis);
      assertEquals("width not correct", width[i], header.getWidth());
      assertEquals("height not correct", height[i], header.getHeight());
      //      assertEquals("bit depth not correct", bit_depth[i], header.bitDepth);
    }
  }


  /**
   * Test method for {@link hipi.image.io.PNGImageUtil#decodeImage(java.io.InputStream)}.
   * 
   * @throws IOException
   */

  @Ignore
  @Test
  public void testDecodeImage() throws IOException {
    /*
    ImageDecoder pngDecoder, ppmDecoder;
    pngDecoder = PNGImageUtil.getInstance();
    ppmDecoder = PPMImageUtil.getInstance();
    ImageEncoder ppmEncoder = PPMImageUtil.getInstance();
    FileInputStream fis;
    FileOutputStream fos;
    FloatImage ppmImage, pngImage;
    String[] fileName = {"canon-ixus", "cmyk-jpeg-format"};
    for (int i = 0; i < fileName.length; i++) {
      fis = new FileInputStream("data/test/PNGImageUtilTestCase/truth/" + fileName[i] + ".ppm");
      ppmImage = ppmDecoder.decodeImage(fis);
      assumeNotNull(ppmImage);
      fis = new FileInputStream("data/test/PNGImageUtilTestCase/decode/" + fileName[i] + ".png");
      pngImage = pngDecoder.decodeImage(fis);
      fos = new FileOutputStream("/tmp/testpngdecode_" + fileName[i] + ".ppm");
      ppmEncoder.encodeImage(pngImage, null, fos);
      assumeNotNull(pngImage);
      assertEquals(fileName[i] + " decoding fails for " + fileName[i], ppmImage, pngImage);
    }
    */
  }

  /**
   * Test method for
   * {@link hipi.image.io.PNGImageUtil#encodeImage(hipi.image.FloatImage, hipi.image.ImageHeader, java.io.OutputStream)}
   * .
   * 
   * @throws IOException
   */
  @Ignore
  @Test
  public void testEncodeImage() throws IOException {
    /*
    ImageDecoder decoder = PPMImageUtil.getInstance();
    ImageEncoder encoder = PNGImageUtil.getInstance();
    FileInputStream pis;
    FileOutputStream pos;
    FloatImage image;
    String[] fileName = {"canon-ixus", "cmyk-jpeg-format"};
    for (int i = 0; i < fileName.length; i++) {
      pis = new FileInputStream("data/test/PNGImageUtilTestCase/truth/" + fileName[i] + ".ppm");
      image = decoder.decodeImage(pis);
      pos = new FileOutputStream("/tmp/" + fileName[i] + ".png");
      encoder.encodeImage(image, null, pos);
      Runtime rt = Runtime.getRuntime();
      Process pr =
          rt.exec("compare -metric MSE data/test/PNGImageUtilTestCase/truth/" + fileName[i]
              + ".ppm /tmp/" + fileName[i] + ".png /tmp/psnr.png");
      Scanner scanner = new Scanner(new InputStreamReader(pr.getErrorStream()));
      float mse = scanner.hasNextFloat() ? scanner.nextFloat() : -1;
      assertTrue(fileName[i] + " MSE is too high : " + mse, mse == 0);
    }
    */
  }

}

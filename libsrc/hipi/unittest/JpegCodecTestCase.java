package hipi.unittest;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.io.ImageDecoder;
import hipi.image.io.ImageEncoder;
import hipi.image.io.JpegCodec;
//import hipi.image.io.PPMImageUtil;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class JpegCodecTestCase {

  /**
   * Test method for {@link hipi.image.io.JPEGImageUtil#decodeImageHeader(java.io.InputStream)}.
   * 
   * @throws IOException
   */
  @Test
  public void testDecodeImageHeader() throws IOException {
    ImageDecoder decoder = JpegCodec.getInstance();
    FileInputStream fis;
    ImageHeader header;
    String[] fileName =
        {"canon-ixus", "fujifilm-dx10", "fujifilm-finepix40i", "fujifilm-mx1700", "kodak-dc210",
	 "kodak-dc240", "nikon-e950", "olympus-c960", "ricoh-rdc5300", "sanyo-vpcg250",
	 "sanyo-vpcsx550", "sony-cybershot", "sony-d700", "fujifilm-x100s"};
    String[] model =
        {"Canon DIGITAL IXUS", "DX-10", "FinePix40i", "MX-1700ZOOM", "DC210 Zoom (V05.00)",
            "KODAK DC240 ZOOM DIGITAL CAMERA", "E950", "C960Z,D460Z", "RDC-5300", "SR6", "SX113",
	 "CYBERSHOT", "DSC-D700", "X100S"};
    int[] width = {640, 1024, 600, 640, 640, 640, 800, 640, 896, 640, 640, 640, 672, 3456};
    int[] height = {480, 768, 450, 480, 480, 480, 600, 480, 600, 480, 480, 480, 512, 2304};
    int[] bit_depth = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
    for (int i = 0; i < fileName.length; i++) {
      String fname = "data/test/JPEGImageUtilTestCase/exif/" + fileName[i] + ".jpg";
      fis = new FileInputStream(fname);
      header = decoder.decodeHeader(fis);
      assertNotNull("failed to decode header: " + fname, header);
      //      assertEquals("exif model not correct: " + fname, model[i].trim(), header.getEXIFInformation("Model").trim());
      assertEquals("width not correct: " + fname, width[i], header.getWidth());
      assertEquals("height not correct: " + fname, height[i], header.getHeight());
      //      assertEquals("bit depth not correct: " + fname, bit_depth[i], header.bitDepth);
    }
  }

  /**
   * Test method for {@link hipi.image.io.JPEGImageUtil#decodeImage(java.io.InputStream)}.
   * 
   * @throws IOException
   */
  @Test
  public void testDecodeImage() throws IOException {
    /*
    ImageDecoder jpgDecoder, ppmDecoder;
    jpgDecoder = JPEGImageUtil.getInstance();
    ppmDecoder = PPMImageUtil.getInstance();
    FileInputStream fis;
    FloatImage ppmImage, jpgImage;
    String[] fileName = {"canon-ixus", "cmyk-jpeg-format"};
    for (int i = 0; i < fileName.length; i++) {
      fis = new FileInputStream("data/test/JPEGImageUtilTestCase/truth/" + fileName[i] + ".ppm");
      ppmImage = ppmDecoder.decodeImage(fis);
      assumeNotNull(ppmImage);
      fis = new FileInputStream("data/test/JPEGImageUtilTestCase/decode/" + fileName[i] + ".jpg");
      jpgImage = jpgDecoder.decodeImage(fis);
      assumeNotNull(jpgImage);
      assertEquals(fileName[i] + " decoding fails", ppmImage, jpgImage);
    }
    */
  }

  /**
   * Test method for
   * {@link hipi.image.io.JPEGImageUtil#encodeImage(hipi.image.FloatImage, hipi.image.ImageHeader, java.io.OutputStream)}
   * .
   * 
   * @throws IOException
   */
  @Test
  public void testEncodeImage() throws IOException {
    /*
    ImageDecoder decoder = PPMImageUtil.getInstance();
    ImageEncoder encoder = JPEGImageUtil.getInstance();
    FileInputStream pis;
    FileOutputStream jos;
    FloatImage image;
    String[] fileName = {"canon-ixus", "cmyk-jpeg-format"};
    for (int i = 0; i < fileName.length; i++) {
      pis = new FileInputStream("data/test/JPEGImageUtilTestCase/truth/" + fileName[i] + ".ppm");
      image = decoder.decodeImage(pis);
      jos = new FileOutputStream("/tmp/" + fileName[i] + ".jpg");
      encoder.encodeImage(image, null, jos);
      Runtime rt = Runtime.getRuntime();
      Process pr =
          rt.exec("compare -metric PSNR data/test/JPEGImageUtilTestCase/truth/" + fileName[i]
              + ".ppm /tmp/" + fileName[i] + ".jpg /tmp/psnr.png");
      Scanner scanner = new Scanner(new InputStreamReader(pr.getErrorStream()));
      float psnr = scanner.hasNextFloat() ? scanner.nextFloat() : 0;
      assertTrue(fileName[i] + " PSNR is too low : " + psnr, psnr > 30);
    }
    */
  }

}

package hipi.unittest;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import hipi.image.HipiImage;
import hipi.image.RasterImage;
import hipi.image.ByteImage;
import hipi.image.FloatImage;
import hipi.image.PixelArrayByte;
import hipi.image.PixelArrayFloat;
import hipi.image.PixelArray;
import hipi.image.HipiImageFactory;
import hipi.image.HipiImageHeader;
import hipi.image.io.ImageDecoder;
import hipi.image.io.ImageEncoder;
import hipi.image.io.JpegCodec;
import hipi.image.io.PpmCodec;
import hipi.image.io.ExifDataUtils;
import hipi.util.ByteUtils;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.ArrayUtils;

import org.junit.Test;
import org.junit.Ignore;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.ColorConvertOp;
import java.awt.color.ColorSpace;

import java.io.File;
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

public class JpegCodecTestCase {

  private void printExifData(HipiImage image) {
    // display EXIF data
    for (String key : image.getAllExifData().keySet()) {
      String value = image.getExifData(key);
      System.out.println(key + " : " + value);
    }
  }

  @Test
  public void testTwelveMonkeysPlugIn() {
    boolean foundTwelveMonkeys = false;
    Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("JPEG");
    while (readers.hasNext()) {
      ImageReader imageReader = readers.next();
      //      System.out.println("ImageReader: " + imageReader);
      if (imageReader.toString().startsWith("com.twelvemonkeys.imageio.plugins.jpeg")) {
	foundTwelveMonkeys = true;
      }
    }
    assertTrue("FATAL ERROR: failed to locate TwelveMonkeys ImageIO plugins", foundTwelveMonkeys);
  }

  /**
   * Test method for {@link hipi.image.io.JpegCodec#decodeHeader(java.io.InputStream)}.
   * 
   * @throws IOException
   */
  @Test
  public void testDecodeHeader() throws IOException {
    ImageDecoder decoder = JpegCodec.getInstance();
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
    for (int i = 0; i < fileName.length; i++) {
      String fname = "./testimages/jpeg-exif-test/" + fileName[i] + ".jpg";
      FileInputStream fis = new FileInputStream(fname);
      HipiImageHeader header = decoder.decodeHeader(fis, true);
      assertNotNull("failed to decode header: " + fname, header);
      assertEquals("exif model not correct: " + fname, model[i].trim(), header.getExifData("Model").trim());
      assertEquals("width not correct: " + fname, width[i], header.getWidth());
      assertEquals("height not correct: " + fname, height[i], header.getHeight());
    }
  }

  @Test
  public void testSRGBConversions() throws IOException {
    ImageDecoder jpegDecoder = JpegCodec.getInstance();
    ImageEncoder ppmEncoder = PpmCodec.getInstance();

    File[] cmykFiles = new File("./testimages/jpeg-cmyk").listFiles();
    File[] rgbFiles = new File("./testimages/jpeg-rgb").listFiles();
    File[] files = (File[])ArrayUtils.addAll(cmykFiles,rgbFiles);

    for (File file : files) {
      String ext = FilenameUtils.getExtension(file.getName());
      if (file.isFile() && ext.equalsIgnoreCase("jpg")) {

	String jpgPath = file.getPath();
	String ppmPath = FilenameUtils.removeExtension(file.getPath()) + "_hipi.ppm";

	System.out.println("Testing linear RGB color conversion for: " + jpgPath);

	// Using FloatImage forces conversion from non-linear sRGB to linear RGB by default
	FloatImage jpegImage = (FloatImage)jpegDecoder.decodeHeaderAndImage(new FileInputStream(jpgPath), HipiImageFactory.getFloatImageFactory(), true);
	assertNotNull(jpegImage);

	BufferedImage javaImage = ImageIO.read(new FileInputStream(jpgPath));
	assertNotNull(javaImage);

	ColorSpace ics = ColorSpace.getInstance(ColorSpace.CS_LINEAR_RGB);
	ColorConvertOp cco = new ColorConvertOp(ics, null);
	BufferedImage rgbImage = cco.filter(javaImage, null);
	assertNotNull(rgbImage);
	
	Raster raster = rgbImage.getData();
	DataBuffer dataBuffer = raster.getDataBuffer();
	int w = raster.getWidth();
	int h = raster.getHeight();
	
	assertEquals(w,jpegImage.getWidth());
	assertEquals(h,jpegImage.getHeight());
	assertEquals(3,raster.getNumBands());

	ppmEncoder.encodeImage(jpegImage, new FileOutputStream(ppmPath));
	System.out.println("wrote: " + ppmPath);

	String truthPath = FilenameUtils.removeExtension(file.getPath()) + "_photoshop.ppm";

	Runtime rt = Runtime.getRuntime();
	String cmd = "compare -metric PSNR " + ppmPath + " " + truthPath + " /tmp/psnr.png";
	System.out.println(cmd);
	Process pr = rt.exec(cmd);
	Scanner scanner = new Scanner(new InputStreamReader(pr.getErrorStream()));
	float psnr = scanner.hasNextFloat() ? scanner.nextFloat() : 0;
	System.out.println("PSNR with respect to Photoshop ground truth: " + psnr);
	assertTrue("PSNR is too low : " + psnr, psnr > 30);

      }
    }    
  }

  @Test
  public void testDecodeImage() throws IOException {
    ImageDecoder jpegDecoder = JpegCodec.getInstance();
    ImageDecoder ppmDecoder = PpmCodec.getInstance();

    File[] cmykFiles = new File("./testimages/jpeg-cmyk").listFiles();
    //    File[] rgbFiles = new File("./testimages/jpeg-rgb").listFiles();
    File[] rgbFiles = null;
    File[] files = (File[])ArrayUtils.addAll(cmykFiles,rgbFiles);

    for (int iter=0; iter<=1; iter++) {

      for (File file : files) {
	String ext = FilenameUtils.getExtension(file.getName());
	if (file.isFile() && ext.equalsIgnoreCase("jpg")) {

	  String jpgPath = file.getPath();
	  String ppmPath = FilenameUtils.removeExtension(file.getPath()) + "_photoshop.ppm";

	  System.out.println("Testing JPEG decoder (" + (iter == 0 ? "ByteImage" : "FloatImage") + ") for: " + jpgPath);
	  
	  FileInputStream fis = new FileInputStream(ppmPath);
	  RasterImage ppmImage = (RasterImage)ppmDecoder.decodeHeaderAndImage(fis, (iter == 0 ? HipiImageFactory.getByteImageFactory() : HipiImageFactory.getFloatImageFactory()), false);
	  assumeNotNull(ppmImage);
	  
	  fis = new FileInputStream(jpgPath);
	  RasterImage jpegImage = (RasterImage)jpegDecoder.decodeHeaderAndImage(fis, (iter == 0 ? HipiImageFactory.getByteImageFactory() : HipiImageFactory.getFloatImageFactory()), true);
	  assumeNotNull(jpegImage);
	  
	  float maxDiff = (iter == 0 ? 45.0f : 45.0f/255.0f);
	  if (!ppmImage.equalsWithTolerance((RasterImage)jpegImage, maxDiff)) { // allow 3 8-bit values difference to account for color space conversion
	    System.out.println(ppmImage);
	    System.out.println(jpegImage);
	    
	    // Get pointers to pixel arrays
	    PixelArray ppmPA = ppmImage.getPixelArray();
	    PixelArray jpegPA = jpegImage.getPixelArray();
	    
	    int w = ppmImage.getWidth();
	    int h = ppmImage.getHeight();
	    assertEquals(ppmImage.getNumBands(),3);
	    assertEquals(jpegImage.getNumBands(),3);
	    
	    // Check that pixel data is equal.
	    for (int i = 0; i < w*h*3; i++) {
	      float diff = (iter == 0 ? Math.abs(ppmPA.getElem(i) - jpegPA.getElem(i)) : Math.abs(ppmPA.getElemFloat(i) - jpegPA.getElemFloat(i)));
	      if (diff > maxDiff) {
		int j = (int)(i/3);
		int x = j%w;
		int y = (int)(j/w);
		if (iter == 0) {
		  System.out.println(String.format("(%d,%d)  PPM: %d %d %d | JPG: %d %d %d", x, y,
						   ppmPA.getElem(j*3+0), ppmPA.getElem(j*3+1), ppmPA.getElem(j*3+2),
						   jpegPA.getElem(j*3+0), jpegPA.getElem(j*3+1), jpegPA.getElem(j*3+2)));
		} else {
		  System.out.println(String.format("(%d,%d)  PPM: %.2f %.2f %.2f | JPG: %.2f %.2f %.2f", x, y,
						   ppmPA.getElemFloat(j*3+0), ppmPA.getElemFloat(j*3+1), ppmPA.getElemFloat(j*3+2),
						   jpegPA.getElemFloat(j*3+0), jpegPA.getElemFloat(j*3+1), jpegPA.getElemFloat(j*3+2)));
		}
		System.out.println(String.format("diff = %f [maxDiff = %f]", diff, maxDiff));
		break;
	      }
	    }
	    
	    fail("Found differences between decoded image and ground truth.");
	    
	  }
	}
      }
    }
  }

  @Test
  public void testEncodeImage() throws IOException {
    ImageDecoder ppmDecoder = PpmCodec.getInstance();
    ImageEncoder jpegEncoder = JpegCodec.getInstance();

    //    File[] cmykFiles = new File("./testimages/jpeg-cmyk").listFiles();
    //    File[] rgbFiles = new File("./testimages/jpeg-rgb").listFiles();
    //    File[] files = (File[])ArrayUtils.addAll(cmykFiles,rgbFiles);
    File[] files = new File("./testimages/jpeg-rgb").listFiles();

    // Tests PPM decode and JPEG encode routines using ByteImage
    for (File file : files) {
      if (file.isFile() && file.getName().endsWith("_photoshop.ppm")) {

	String ppmPath = file.getPath();
	String jpgPath = "/tmp/hipi_enc.jpg";

	System.out.println("Testing JPEG encoder (ByteImage) for: " + ppmPath);

	ByteImage image = (ByteImage)ppmDecoder.decodeHeaderAndImage(new FileInputStream(ppmPath), HipiImageFactory.getByteImageFactory(), false);
	jpegEncoder.encodeImage(image, new FileOutputStream(jpgPath));

	Runtime rt = Runtime.getRuntime();
	String cmd = "compare -metric PSNR " + ppmPath + " " + jpgPath + " /tmp/psnr.png";
	System.out.println("cmd: " + cmd);
	Process pr = rt.exec(cmd);
	Scanner scanner = new Scanner(new InputStreamReader(pr.getErrorStream()));
	float psnr = scanner.hasNextFloat() ? scanner.nextFloat() : 0;
	System.out.println("PSNR: " + psnr);
	assertTrue(ppmPath + " PSNR is too low : " + psnr, psnr > 30);
      }
    }

    // Tests PPM decode and JPEG encode routines using FloatImage
    for (File file : files) {
      if (file.isFile() && file.getName().endsWith("_photoshop.ppm")) {

	String ppmPath = file.getPath();
	String jpgPath = "/tmp/hipi_enc.jpg";

	System.out.println("Testing JPEG encoder (FloatImage) for: " + ppmPath);

	FloatImage image = (FloatImage)ppmDecoder.decodeHeaderAndImage(new FileInputStream(ppmPath), HipiImageFactory.getFloatImageFactory(), false);
	jpegEncoder.encodeImage(image, new FileOutputStream(jpgPath));

	Runtime rt = Runtime.getRuntime();
	String cmd = "compare -metric PSNR " + ppmPath + " " + jpgPath + " /tmp/psnr.png";
	System.out.println("cmd: " + cmd);
	Process pr = rt.exec(cmd);
	Scanner scanner = new Scanner(new InputStreamReader(pr.getErrorStream()));
	float psnr = scanner.hasNextFloat() ? scanner.nextFloat() : 0;
	System.out.println("PSNR: " + psnr);
	assertTrue(ppmPath + " PSNR is too low : " + psnr, psnr > 30);
      }
    }
  }

}

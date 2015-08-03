package org.hipi.test;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import org.hipi.image.HipiImage;
import org.hipi.image.RasterImage;
import org.hipi.image.ByteImage;
import org.hipi.image.FloatImage;
import org.hipi.image.PixelArrayByte;
import org.hipi.image.PixelArrayFloat;
import org.hipi.image.PixelArray;
import org.hipi.image.HipiImageFactory;
import org.hipi.image.HipiImageHeader;
import org.hipi.image.io.ImageDecoder;
import org.hipi.image.io.ImageEncoder;
import org.hipi.image.io.JpegCodec;
import org.hipi.image.io.PpmCodec;
import org.hipi.util.ByteUtils;

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

public class PpmCodecTestCase {

  @Test
  public void testTwelveMonkeysPlugIn() {
    boolean foundTwelveMonkeys = false;
    Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("PPM");
    while (readers.hasNext()) {
      ImageReader imageReader = readers.next();
      //      System.out.println("ImageReader: " + imageReader);
      if (imageReader.toString().startsWith("com.twelvemonkeys.imageio.plugins.pnm")) {
	foundTwelveMonkeys = true;
      }
    }
    assertTrue("FATAL ERROR: failed to locate TwelveMonkeys ImageIO plugins", foundTwelveMonkeys);
  }

  @Ignore
  @Test
  public void testEncodeImage() throws IOException {
    ImageDecoder ppmDecoder = PpmCodec.getInstance();
    ImageEncoder jpegEncoder = JpegCodec.getInstance();

    //    File[] cmykFiles = new File("./testimages/jpeg-cmyk").listFiles();
    //    File[] rgbFiles = new File("./testimages/jpeg-rgb").listFiles();
    //    File[] files = (File[])ArrayUtils.addAll(cmykFiles,rgbFiles);
    File[] files = new File("./testdata/jpeg-rgb").listFiles();

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

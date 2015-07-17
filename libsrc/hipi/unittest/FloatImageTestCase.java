package hipi.unittest;

import static org.junit.Assert.*;

import hipi.image.RasterImage;
import hipi.image.FloatImage;
import hipi.image.ByteImage;
import hipi.image.HipiImageFactory;
import hipi.image.HipiImageHeader;
import hipi.image.HipiImage.HipiImageType;
import hipi.image.io.ImageDecoder;
import hipi.image.io.PpmCodec;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.ArrayUtils;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.File;

public class FloatImageTestCase {

  @Test
  public void testSanity() throws IOException {
    FloatImage floatImage = new FloatImage();
    assertEquals(floatImage.getType(), HipiImageType.FLOAT);
    assertEquals(floatImage.getData(), null);
    assertFalse(floatImage.equalsWithTolerance(null, 1.0f));
  }
    
  @Test
  public void testFloatImageWritable() throws IOException {
    ImageDecoder ppmDecoder = PpmCodec.getInstance();

    File[] cmykFiles = new File("./testimages/jpeg-cmyk").listFiles();
    File[] rgbFiles = new File("./testimages/jpeg-rgb").listFiles();
    File[] files = (File[])ArrayUtils.addAll(cmykFiles,rgbFiles);

    for (int iter=0; iter<2; iter++) {
      for (File file : files) {
	if (file.isFile() && file.getName().endsWith("_photoshop.ppm")) {
	  String ppmPath = file.getPath();
	  System.out.println(ppmPath);

	  RasterImage image = (RasterImage)ppmDecoder.decodeHeaderAndImage(new FileInputStream(ppmPath), 
							      (iter == 0 ? HipiImageFactory.getByteImageFactory() : HipiImageFactory.getFloatImageFactory()), false);
	  ByteArrayOutputStream bos = new ByteArrayOutputStream();
	  image.write(new DataOutputStream(bos));
	  ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
	  RasterImage newImage = (iter == 0 ? new ByteImage() : new FloatImage());
	  newImage.readFields(new DataInputStream(bis));
	  assertEquals(ppmPath + " writable test fails", image, newImage);
	}
      }
    }

  }

}

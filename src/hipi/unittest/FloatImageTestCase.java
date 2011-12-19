package hipi.unittest;

import static org.junit.Assert.*;
import hipi.image.FloatImage;
import hipi.image.io.ImageDecoder;
import hipi.image.io.PPMImageUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.junit.Test;


public class FloatImageTestCase {

	@Test
	public void testFloatImageWritable() throws IOException {
		ImageDecoder decoder = PPMImageUtil.getInstance();
		FileInputStream fis;
		String[] fileName = {"canon-ixus", "cmyk-jpeg-format"};
		for (int i = 0; i < fileName.length; i++)
		{
			fis = new FileInputStream("data/test/JPEGImageUtilTestCase/truth/" + fileName[i] + ".ppm");
			FloatImage image = decoder.decodeImage(fis);
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			image.write(new DataOutputStream(bos));
			ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
			FloatImage newImage = new FloatImage();
			newImage.readFields(new DataInputStream(bis));
			assertEquals(fileName[i] + " writable test fails", image, newImage);
		}
	}
}

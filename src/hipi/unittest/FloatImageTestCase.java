package hipi.unittest;

import static org.junit.Assert.assertArrayEquals;
import hipi.image.FloatImage;
import hipi.image.io.ImageDecoder;
import hipi.image.io.ImageEncoder;
import hipi.image.io.JPEGImageUtil;
import hipi.image.io.PPMImageUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
			assertArrayEquals(fileName[i] + " writable test fails", image.getData(), newImage.getData(), 1.0f);
		}
	}
	
	@Test
	public void testConvolve() throws IOException {
		// Create 5x5 gaussian kernel
		float[][] gauss_kernel = new float[][]{{1, 4, 7, 4, 1},
				{4, 16, 26, 16, 4}, 
				{7, 26, 41, 26, 7}, 
				{4, 16, 26, 16, 4}, 
				{1, 4, 7, 4, 1}};
		for (int i = 0; i < 5; i++){
			for (int j = 0; j < 5; j++){
				gauss_kernel[i][j] /= (273.0);
			}
		}
		
		ImageDecoder decoder = PPMImageUtil.getInstance();
		FileInputStream fis;
		ImageEncoder encoder = JPEGImageUtil.getInstance();
		FileOutputStream jos;
		String[] fileName = {"canon-ixus"};
		for (int i = 0; i < fileName.length; i++)
		{
			fis = new FileInputStream("data/test/JPEGImageUtilTestCase/truth/" + fileName[i] + ".ppm");
			FloatImage image = decoder.decodeImage(fis);
			System.out.println("Width = " + image.getWidth() + " height = " + image.getHeight());
			image = image.convolve(gauss_kernel);
			System.out.println("Width = " + image.getWidth() + " height = " + image.getHeight());
			jos = new FileOutputStream("/tmp/" + fileName[i] + "_blurred.jpg");
			encoder.encodeImage(image, null, jos);
		}

	}
	
	@Test
	public void testDownsample() throws IOException {
		ImageDecoder decoder = PPMImageUtil.getInstance();
		FileInputStream fis;
		ImageEncoder encoder = PPMImageUtil.getInstance();
		FileOutputStream jos;
		String[] fileName = {"canon-ixus"};
		for (int i = 0; i < fileName.length; i++)
		{
			fis = new FileInputStream("data/test/JPEGImageUtilTestCase/truth/" + fileName[i] + ".ppm");
			FloatImage image = decoder.decodeImage(fis);
			image = image.convert(FloatImage.RGB2GRAY);
			System.out.println("Width = " + image.getWidth() + " height = " + image.getHeight());
			image = image.downsample(1);
			float[] data = image.getData();
			System.out.println("Data length = " + data.length);
			System.out.println("Width = " + image.getWidth() + " height = " + image.getHeight());
			jos = new FileOutputStream("/tmp/" + fileName[i] + "_downsampled.ppm");
			encoder.encodeImage(image, null, jos);
		}
	}

	@Test
	public void testUpsample() throws IOException {
		ImageDecoder decoder = PPMImageUtil.getInstance();
		FileInputStream fis;
		ImageEncoder encoder = PPMImageUtil.getInstance();
		FileOutputStream jos;
		String[] fileName = {"canon-ixus"};
		for (int i = 0; i < fileName.length; i++)
		{
			fis = new FileInputStream("data/test/JPEGImageUtilTestCase/truth/" + fileName[i] + ".ppm");
			FloatImage image = decoder.decodeImage(fis);
			image = image.convert(FloatImage.RGB2GRAY);
			System.out.println("Width = " + image.getWidth() + " height = " + image.getHeight());
			//image = image.downsample(1);
			image = image.upsample(1);
			float[] data = image.getData();
			System.out.println("Data length = " + data.length);
			System.out.println("Width = " + image.getWidth() + " height = " + image.getHeight());
			jos = new FileOutputStream("/tmp/" + fileName[i] + "_upsampled.ppm");
			encoder.encodeImage(image, null, jos);
		}
	}
	
	@Test
	public void testConcat() throws IOException {
		ImageDecoder decoder = PPMImageUtil.getInstance();
		FileInputStream fis;
		ImageEncoder encoder = JPEGImageUtil.getInstance();
		FileOutputStream jos;
		String fileName = "canon-ixus";
		
		fis = new FileInputStream("data/test/JPEGImageUtilTestCase/truth/" + fileName + ".ppm");
		FloatImage image = decoder.decodeImage(fis);
		FloatImage image_copy = image.deepCopy();
		System.out.println("Width = " + image.getWidth() + " height = " + image.getHeight());
		FloatImage concat_image = image.concatenate(image_copy);
		System.out.println("Width = " + concat_image.getWidth() + " height = " + concat_image.getHeight());
		jos = new FileOutputStream("/tmp/" + fileName + "_concat.jpg");
		encoder.encodeImage(concat_image, null, jos);
		
		FloatImage crop_left = concat_image.crop(0, 0, image.getWidth(), image.getHeight());
		FloatImage crop_right = concat_image.crop(image.getWidth(), image.getHeight(), image.getWidth(), image.getHeight());

		assertArrayEquals(fileName + " concat test fails", image.getData(), crop_left.getData(), 1.0f);
		assertArrayEquals(fileName + " concat test fails", image.getData(), crop_right.getData(), 1.0f);
	}
	
}

package hipi.unittest;

import static org.junit.Assert.*;
import hipi.image.FloatImage;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.ImageDecoder;
import hipi.image.io.JPEGImageUtil;
import hipi.imagebundle.AbstractImageBundle;

import java.io.FileInputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public abstract class AbstractImageBundleTestCase {

	public abstract AbstractImageBundle createImageBundleAndOpen(int mode) throws IOException;

	@Before
	public void setUp() throws IOException {
		AbstractImageBundle aib = createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_WRITE);
		aib.addImage(new FileInputStream("data/test/ImageBundleTestCase/read/0.jpg"), ImageType.JPEG_IMAGE);
		aib.addImage(new FileInputStream("data/test/ImageBundleTestCase/read/1.jpg"), ImageType.JPEG_IMAGE);
		aib.close();
	}

	@Test
	public void testIterator() throws IOException {
		AbstractImageBundle aib = createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_READ);
		ImageDecoder decoder = JPEGImageUtil.getInstance();
		int count = 0;
		while (aib.hasNext()) {
			aib.next();
			FloatImage image = aib.getCurrentImage();
			FloatImage source = decoder.decodeImage(new FileInputStream("data/test/ImageBundleTestCase/read/" + count + ".jpg"));
			assertEquals(count + " image fails", source, image);
			count++;
		}
		aib.close();
	}

	@Test
	public void testGetCurrentImage() throws IOException {
		AbstractImageBundle aib = createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_READ);
		ImageDecoder decoder = JPEGImageUtil.getInstance();
		int count = 0;
		while (aib.hasNext()) {
			aib.next();
			FloatImage source = decoder.decodeImage(new FileInputStream("data/test/ImageBundleTestCase/read/" + count + ".jpg"));
			FloatImage image = aib.getCurrentImage();
			assertEquals(count + " image, first trial fails", source, image);
			image = aib.getCurrentImage();
			assertEquals(count + " image, second trial fails", source, image);
			image = aib.getCurrentImage();
			assertEquals(count + " image, third trial fails", source, image);
			count++;
		}
		aib.close();
	}

	@Test
	public void testNext() throws IOException {
		AbstractImageBundle aib = createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_READ);
		ImageDecoder decoder = JPEGImageUtil.getInstance();
		aib.next(); aib.next();
		FloatImage source = decoder.decodeImage(new FileInputStream("data/test/ImageBundleTestCase/read/1.jpg"));
		FloatImage image = aib.getCurrentImage();
		assertEquals("skip image fails", source, image);
		aib.close();
	}

	@Test
	public void testHasNext() throws IOException {
		AbstractImageBundle aib = createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_READ);
		FloatImage source, image;
		ImageDecoder decoder = JPEGImageUtil.getInstance();
		assertTrue("first trial fail to assert hasNext", aib.hasNext());
		assertTrue("second trial fail to assert hasNext", aib.hasNext());
		assertTrue("third trial fail to assert hasNext", aib.hasNext());
		aib.next();
		source = decoder.decodeImage(new FileInputStream("data/test/ImageBundleTestCase/read/0.jpg"));
		image = aib.getCurrentImage();
		assertEquals("first image fails", source, image);
		assertTrue("first trial fail to assert hasNext", aib.hasNext());
		assertTrue("second trial fail to assert hasNext", aib.hasNext());
		assertTrue("third trial fail to assert hasNext", aib.hasNext());
		aib.next();
		source = decoder.decodeImage(new FileInputStream("data/test/ImageBundleTestCase/read/1.jpg"));
		image = aib.getCurrentImage();
		assertEquals("second image fails", source, image);
		assertFalse("first trial fail to assert hasNext", aib.hasNext());
		assertFalse("second trial fail to assert hasNext", aib.hasNext());
		assertFalse("third trial fail to assert hasNext", aib.hasNext());
		aib.close();
	}
}

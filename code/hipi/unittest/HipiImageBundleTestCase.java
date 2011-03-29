package hipi.unittest;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import hipi.image.ImageHeader.ImageType;
import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.HipiImageBundle;

public class HipiImageBundleTestCase extends AbstractImageBundleTestCase {

	@Override
	public AbstractImageBundle createImageBundleAndOpen(int mode) throws IOException {
		Configuration conf = new Configuration();
		HipiImageBundle hib = new HipiImageBundle(new Path("/tmp/bundle.hib"), conf);
		hib.open(mode, true);
		return hib;
	}
	
	@Test
	public void testOffsets() throws IOException {
		HipiImageBundle hib = (HipiImageBundle) createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_READ);
		List<Long> offsets = hib.getOffsets();
		assertEquals("the first offset should be the size of first image plus 8", (long) 128037 + 8, (long) offsets.get(0));
		assertEquals("the second offset should be the size of data file", (long) 171236, (long) offsets.get(1));
	}
	
	@Test
	public void testMerge() throws IOException {
		//create image bundles
		Configuration conf = new Configuration();
		HipiImageBundle aib1 = new HipiImageBundle(new Path("/tmp/bundle1.hib"), conf);
		aib1.open(AbstractImageBundle.FILE_MODE_WRITE, true);		
		aib1.addImage(new FileInputStream("data/test/ImageBundleTestCase/read/0.jpg"), ImageType.JPEG_IMAGE);
		aib1.addImage(new FileInputStream("data/test/ImageBundleTestCase/read/1.jpg"), ImageType.JPEG_IMAGE);
		aib1.close();

		HipiImageBundle aib2 = new HipiImageBundle(new Path("/tmp/bundle2.hib"), conf);
		aib2.open(AbstractImageBundle.FILE_MODE_WRITE, true);
		aib2.addImage(new FileInputStream("data/test/ImageBundleTestCase/read/2.jpg"), ImageType.JPEG_IMAGE);
		aib2.addImage(new FileInputStream("data/test/ImageBundleTestCase/read/3.jpg"), ImageType.JPEG_IMAGE);
		aib2.close();
		
		HipiImageBundle aib1_in = new HipiImageBundle(new Path("/tmp/bundle1.hib"), conf);
		aib1_in.open(AbstractImageBundle.FILE_MODE_READ, true);
		System.out.println("pics in bundle 1: + " + aib1_in.getImageCount());
		HipiImageBundle aib2_in = new HipiImageBundle(new Path("/tmp/bundle2.hib"), conf);
		aib2_in.open(AbstractImageBundle.FILE_MODE_READ, true);
		System.out.println("pics in bundle 1: + " + aib1_in.getImageCount());

		HipiImageBundle bundles[] = {aib1_in, aib2_in};
		HipiImageBundle merged_hib = HipiImageBundle.merge(new Path("/tmp/bundle.hib"), conf, bundles);
		merged_hib.close();
		
		merged_hib.open(AbstractImageBundle.FILE_MODE_READ, true);		
				
		setUp();
		testIterator();
		testGetCurrentImage();
		testNext();
		testHasNext();
	}
}

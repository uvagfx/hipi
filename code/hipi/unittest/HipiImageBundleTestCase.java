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
		HipiImageBundle hib = new HipiImageBundle(conf);
		hib.open(new Path("/tmp/bundle.hib"), mode, true);
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
		AbstractImageBundle aib1 = createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_WRITE);
		aib1.addImage(new FileInputStream("data/test/ImageBundleTestCase/read/0.jpg"), ImageType.JPEG_IMAGE);
		aib1.addImage(new FileInputStream("data/test/ImageBundleTestCase/read/1.jpg"), ImageType.JPEG_IMAGE);
		aib1.close();
		
		AbstractImageBundle aib2 = createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_WRITE);
		aib2.addImage(new FileInputStream("data/test/ImageBundleTestCase/read/2.jpg"), ImageType.JPEG_IMAGE);
		aib2.addImage(new FileInputStream("data/test/ImageBundleTestCase/read/3.jpg"), ImageType.JPEG_IMAGE);
		aib2.close();
		
		AbstractImageBundle merged_hib = createImageBundleAndOpen(AbstractImageBundle.FILE_MODE_WRITE);
		AbstractImageBundle bundles[] = {aib1, aib2};
		merged_hib.merge(bundles);
		
		setUp();
		testIterator();
		testGetCurrentImage();
		testNext();
		testHasNext();
	}
}

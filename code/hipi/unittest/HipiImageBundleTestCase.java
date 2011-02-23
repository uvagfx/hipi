package hipi.unittest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

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

}

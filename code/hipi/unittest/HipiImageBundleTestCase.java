package hipi.unittest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

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

}

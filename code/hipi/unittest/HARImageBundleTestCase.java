package hipi.unittest;

import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.HARImageBundle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class HARImageBundleTestCase extends AbstractImageBundleTestCase {

	@Override
	public AbstractImageBundle createImageBundleAndOpen(int mode) throws IOException {
		Configuration conf = new Configuration();
		HARImageBundle sib = new HARImageBundle(conf);
		sib.open(new Path("/tmp/bundle.har"), mode, true);
		return sib;
	}
}

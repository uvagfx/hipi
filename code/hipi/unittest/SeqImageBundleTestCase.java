package hipi.unittest;

import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.SeqImageBundle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class SeqImageBundleTestCase extends AbstractImageBundleTestCase {

	@Override
	public AbstractImageBundle createImageBundleAndOpen(int mode) throws IOException {
		Configuration conf = new Configuration();
		SeqImageBundle sib = new SeqImageBundle(conf);
		sib.open(new Path("/tmp/bundle.sib"), mode, true);
		return sib;
	}

}

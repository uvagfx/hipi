package hipi.imagebundle.mapred;

import hipi.excluder.ImageExcluder;
import hipi.image.ImageHeader;

public class ImageBundleFullRecordReader<U extends ImageExcluder> extends
		AbstractImageBundleRecordReader<ImageHeader, U> {

	public ImageBundleFullRecordReader(ImageBundleFileSplit fileSplit) {
		super(fileSplit);
	}

	@Override
	public ImageHeader createKey() {
		return new ImageHeader();
	}

	@Override
	public ImageHeader getNextKey() {
		return null;
	}

	

}

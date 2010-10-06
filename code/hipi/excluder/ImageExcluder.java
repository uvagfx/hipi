package hipi.excluder;

import hipi.image.ImageHeader;

public interface ImageExcluder {

	public boolean excludeImage(ImageHeader header);
	
}

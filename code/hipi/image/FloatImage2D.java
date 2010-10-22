package hipi.image;

/**
 * A specific FloatImage that only has one band
 * @author seanarietta
 *
 */
public class FloatImage2D extends FloatImage {

	protected FloatImage2D(int width, int height) {
		super(width, height, 1, null);
	}

}

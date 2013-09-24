package hipi.image.io;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

public class PPMImageUtil implements ImageDecoder, ImageEncoder {

	private static final PPMImageUtil static_object = new PPMImageUtil();
	
	public static PPMImageUtil getInstance() {
		return static_object;
	}

	public ImageHeader createSimpleHeader(FloatImage image) {
		return new ImageHeader();
	}

	public void encodeImage(FloatImage image, ImageHeader header, OutputStream os)
			throws IOException {
		PrintWriter writer = new PrintWriter(os);
		if (image.getBands() == 1)
			writer.println("P5");
		else 
			writer.println("P6");
		writer.println(image.getWidth() + " " + image.getHeight());
		writer.println("255");
		writer.flush();
		float[] pels = image.getData();
		byte[] raw = new byte[image.getWidth() * image.getHeight() * image.getBands()];
		for (int i = 0; i < image.getWidth() * image.getHeight() * image.getBands(); i++)
			raw[i] = (byte)((int)(pels[i] * 255));
		os.write(raw);
	}

	public ImageHeader decodeImageHeader(InputStream is) throws IOException {
		return new ImageHeader();
	}

	public FloatImage decodeImage(InputStream is) throws IOException {
		byte[] header = new byte[255];
		is.read(header);
		if (header[0] != 'P')
			throw new IOException("Unknown File Format");

		int bands = 0;
		if (header[1] == '6')
			bands = 3;
		else if (header[1] == '5')
			bands = 1;
		else
			throw new IOException("Unknown File Format");

		int off = 3;
		int w = 0;
		while (header[off] >= '0' && header[off] <= '9')
			w = w * 10 + (header[off++] - '0');
		off++;
		int h = 0;
		while (header[off] >= '0' && header[off] <= '9')
			h = h * 10 + (header[off++] - '0');
		off++;
		int d = 0;
		while (header[off] >= '0' && header[off] <= '9')
			d = d * 10 + (header[off++] - '0');
		off++;
		byte[] rest = new byte[w * h * bands - (255 - off)];
		is.read(rest);
		float[] pels = new float[w * h * bands];
		for (int i = 0; i < 255 - off; i++)
			pels[i] = (float)((header[i + off] & 0xff) / 255.0);
		for (int i = 0; i < w * h * bands - (255 - off); i++)
			pels[i + 255 - off] = (float)((rest[i] & 0xff) / 255.0);
		FloatImage image = new FloatImage(w, h, bands, pels);
		return image;
	}

}

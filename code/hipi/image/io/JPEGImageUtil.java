package hipi.image.io;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;

import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import javax.imageio.ImageIO;

import com.drew.imaging.jpeg.JpegMetadataReader;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;
import com.sun.image.codec.jpeg.JPEGCodec;
import com.sun.image.codec.jpeg.JPEGDecodeParam;
import com.sun.image.codec.jpeg.JPEGEncodeParam;
import com.sun.image.codec.jpeg.JPEGImageDecoder;
import com.sun.image.codec.jpeg.JPEGImageEncoder;

public class JPEGImageUtil implements ImageDecoder, ImageEncoder {
	
	private static final JPEGImageUtil static_object = new JPEGImageUtil();
	
	public static JPEGImageUtil getInstance() {
		return static_object;
	}

	public ImageHeader decodeImageHeader(InputStream is) throws IOException {
		try {
			Metadata metadata = JpegMetadataReader.readMetadata(is);
			ImageHeader header = new ImageHeader();
			Iterator directories = metadata.getDirectoryIterator();
			while (directories.hasNext()) {
				Directory directory = (Directory)directories.next();
				Iterator tags = directory.getTagIterator();
				while (tags.hasNext()) {
					Tag tag = (Tag)tags.next();
					header.addEXIFInformation(tag.getTagName(), tag.getDescription());
				}
			}
			return header;
		} catch (Exception e) {
			return null;
		}
	}

	public FloatImage decodeImage(InputStream is) throws IOException {
		JPEGImageDecoder decoder = JPEGCodec.createJPEGDecoder(is);
		Raster raster = decoder.decodeAsRaster();
		DataBuffer dataBuffer = raster.getDataBuffer();
		int w = raster.getWidth();
		int h = raster.getHeight();
		float[] pels = new float[w * h * 3];
		if (raster.getNumBands() == 4) {
			for (int i = 0; i < h; i++)
				for (int j = 0; j < w; j++) {
					int Y = dataBuffer.getElem(i * w * 4 + j * 4);
					int Cr = dataBuffer.getElem(i * w * 4 + j * 4 + 1);
					int Cb = dataBuffer.getElem(i * w * 4 + j * 4 + 2);
					int c = (int)Math.min(Math.max(Y + 1.772 * (Cb - 128), 0), 255);
					int m = (int)Math.min(Math.max(Y - 0.34414 * (Cb - 128) - 0.71414 * (Cr - 128), 0), 255);
					int y = (int)Math.min(Math.max(Y + 1.402 * (Cr - 128), 0), 255);
					int k = dataBuffer.getElem(i * w * 4 + j * 4 + 3);
					pels[i* w * 3 + j * 3] = k - (c * k >> 8);
					pels[i* w * 3 + j * 3 + 1] = k - (m * k >> 8);
					pels[i* w * 3 + j * 3 + 2] = k - (y * k >> 8);
				}
		} else if (raster.getNumBands() == 3) {
			for (int i = 0; i < h; i++)
				for (int j = 0; j < w; j++) {
					int Y = dataBuffer.getElem(i * w * 3 + j * 3);
					int Cr = dataBuffer.getElem(i * w * 3 + j * 3 + 1);
					int Cb = dataBuffer.getElem(i * w * 3 + j * 3 + 2);
					pels[i* w * 3 + j * 3] = Math.min(Math.max(Y + 1.772f * (Cb - 128), 0), 255);
					pels[i* w * 3 + j * 3 + 1] = Math.min(Math.max(Y - 0.34414f * (Cb - 128) - 0.71414f * (Cr - 128), 0), 255);
					pels[i* w * 3 + j * 3 + 2] = Math.min(Math.max(Y + 1.402f * (Cr - 128), 0), 255);
				}
		} else if (raster.getNumBands() == 1) {
			for (int i = 0; i < h; i++)
				for (int j = 0; j < w; j++) {
					int Y = dataBuffer.getElem(i * w + j);
					pels[i* w * 3 + j * 3] = Y;
					pels[i* w * 3 + j * 3 + 1] = Y;
					pels[i* w * 3 + j * 3 + 2] = Y;
				}
		}
		FloatImage image = new FloatImage(w, h, 3, pels);
		return image;
	}

public void encodeImage(FloatImage image, ImageHeader header, OutputStream os)
			throws IOException {
		JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(os);
		BufferedImage bufferedImage = new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_INT_RGB);
		float[] data = image.getData();
		int[] rgb = new int[image.getWidth() * image.getHeight()];
		for (int i = 0; i < image.getWidth() * image.getHeight(); i++)
		{
			int r = Math.min(Math.max((int)data[i * 3], 0), 255);
			int g = Math.min(Math.max((int)data[i * 3 + 1], 0), 255);
			int b = Math.min(Math.max((int)data[i * 3 + 2], 0), 255);
			rgb[i] = r << 16 | g << 8 | b;
		}
		bufferedImage.setRGB(0, 0, image.getWidth(), image.getHeight(), rgb, 0, image.getWidth());
		JPEGEncodeParam param = encoder.getDefaultJPEGEncodeParam(bufferedImage);
		encoder.encode(bufferedImage, param);
	}

	public ImageHeader createSimpleHeader(FloatImage image) {
		return new ImageHeader();
	}

}

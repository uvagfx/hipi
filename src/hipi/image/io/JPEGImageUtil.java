package hipi.image.io;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;

public class JPEGImageUtil implements ImageDecoder, ImageEncoder {
	
	private static final JPEGImageUtil static_object = new JPEGImageUtil();
	
	public static JPEGImageUtil getInstance() {
		return static_object;
	}

	@SuppressWarnings("rawtypes")
	public ImageHeader decodeImageHeader(InputStream is) throws IOException {
		ImageHeader header = new ImageHeader(ImageType.JPEG_IMAGE);
		try {
			DataInputStream dis = new DataInputStream(new BufferedInputStream(is));
			dis.mark(Integer.MAX_VALUE);
			short magic = dis.readShort();
			if (magic != -40)
				return null;
			byte[] data = new byte[6];
			// read in each block to find width / height / bitDepth
			for (;;) {
				dis.read(data, 0, 4);
				if ((data[0] & 0xff) != 0xff)
					return null;
				if ((data[1] & 0xff) == 0x01 || ((data[1] & 0xff) >= 0xd0 && (data[1] & 0xff) <= 0xd7))
					continue;
				long length = (((data[2] & 0xff) << 8) | (data[3] & 0xff)) - 2;
				if ((data[1] & 0xff) != 0xc0) {
					while (length > 0) {
						long skipped = dis.skip(length);
						if (skipped == 0)
							break;
						length -= skipped;
					}
				} else {
					dis.read(data);
					header.height = ((data[1] & 0xff) << 8) | (data[2] & 0xff);
					header.width = ((data[3] & 0xff) << 8) | (data[4] & 0xff);
					header.bitDepth = data[0] & 0xff;
					break;
				}
			}
			dis.reset();
			MetadataReader reader = new MetadataReader(dis);
			Metadata metadata = reader.extract();
			Iterator directories = metadata.getDirectoryIterator();
			while (directories.hasNext()) {
				Directory directory = (Directory)directories.next();
				Iterator tags = directory.getTagIterator();
				while (tags.hasNext()) {
					Tag tag = (Tag)tags.next();
					header.addEXIFInformation(tag.getTagName(), tag.getDescription());
				}
			}
		} catch (Exception e) {
		}
		return header;
	}

	public FloatImage decodeImage(InputStream is) throws IOException {
		ImageInputStream iis = ImageIO.createImageInputStream(is);
		Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("jpeg");
		ImageReader reader = readers.next();
		if (!reader.canReadRaster()) {
			while (readers.hasNext()) {
				reader = readers.next();
				if (reader.canReadRaster())
					break;
			}
		}
		reader.setInput(iis);
		Raster raster = reader.readRaster(0, new ImageReadParam());
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
					pels[i* w * 3 + j * 3] = (float) ((k - (c * k >> 8)) / 255.0);
					pels[i* w * 3 + j * 3 + 1] = (float) ((k - (m * k >> 8)) / 255.0);
					pels[i* w * 3 + j * 3 + 2] = (float) ((k - (y * k >> 8)) / 255.0);
				}
		} else if (raster.getNumBands() == 3) {
			for (int i = 0; i < h; i++)
				for (int j = 0; j < w; j++) {
					int Y = dataBuffer.getElem(i * w * 3 + j * 3);
					int Cr = dataBuffer.getElem(i * w * 3 + j * 3 + 1);
					int Cb = dataBuffer.getElem(i * w * 3 + j * 3 + 2);
					pels[i* w * 3 + j * 3] = (float) ((Math.min(Math.max(Y + 1.772f * (Cb - 128), 0), 255)) / 255.0);
					pels[i* w * 3 + j * 3 + 1] = (float) ((Math.min(Math.max(Y - 0.34414f * (Cb - 128) - 0.71414f * (Cr - 128), 0), 255)) / 255.0);
					pels[i* w * 3 + j * 3 + 2] = (float) ((Math.min(Math.max(Y + 1.402f * (Cr - 128), 0), 255)) / 255.0);
				}
		} else if (raster.getNumBands() == 1) {
			for (int i = 0; i < h; i++)
				for (int j = 0; j < w; j++) {
					int Y = dataBuffer.getElem(i * w + j);
					pels[i* w * 3 + j * 3] = (float) (Y / 255.0);
					pels[i* w * 3 + j * 3 + 1] = (float) (Y / 255.0);
					pels[i* w * 3 + j * 3 + 2] = (float) (Y / 255.0);
				}
		}
		FloatImage image = new FloatImage(w, h, 3, pels);
		return image;
	}

	public void encodeImage(FloatImage image, ImageHeader header, OutputStream os)
			throws IOException {
		ImageOutputStream ios = ImageIO.createImageOutputStream(os);
		Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
		ImageWriter writer = writers.next();
		writer.setOutput(ios);
		BufferedImage bufferedImage = new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_INT_RGB);
		float[] data = image.getData();
		int[] rgb = new int[image.getWidth() * image.getHeight()];
		for (int i = 0; i < image.getWidth() * image.getHeight(); i++)
		{
			int r = Math.min(Math.max((int)(data[i * 3] * 255), 0), 255);
			int g = Math.min(Math.max((int)(data[i * 3 + 1] * 255), 0), 255);
			int b = Math.min(Math.max((int)(data[i * 3 + 2] * 255), 0), 255);
			rgb[i] = r << 16 | g << 8 | b;
		}
		bufferedImage.setRGB(0, 0, image.getWidth(), image.getHeight(), rgb, 0, image.getWidth());
		IIOImage iioImage = new IIOImage(bufferedImage, null, null);
		ImageWriteParam param = writer.getDefaultWriteParam();
		writer.write(null, iioImage, param);
	}

	public ImageHeader createSimpleHeader(FloatImage image) {
		return new ImageHeader(ImageType.JPEG_IMAGE);
	}

}

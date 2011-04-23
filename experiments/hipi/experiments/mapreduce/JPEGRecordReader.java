package hipi.experiments.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.CodecManager;
import hipi.image.io.ImageDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Provides the basic functionality of an ImageBundle record reader's
 * constructor. The constructor is able to read ImageBundleFileSplits and setup
 * the necessary fields to allow the subclass the ability to actually read off
 * FloatImages from the ImageBundle.
 * 
 * This class should be subclassed with a specific Writable object that will
 * serve as the key in the Map tasks.
 * 
 * @author seanarietta
 * 
 * @param <T>
 *            a Writable object that will serve as the key to the Map tasks.
 *            Typically this is either an empty object or the RawImageHeader
 *            object.
 */
public class JPEGRecordReader extends
		RecordReader<ImageHeader, FloatImage> {

	protected Configuration conf;
	private ImageDecoder _decoder;
	private boolean _singleton_emit;
	byte[] _byte_array_data;
	private DataInputStream _is;
	private Path path;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit bundleSplit = (FileSplit) split;
		conf = context.getConfiguration();
		path = bundleSplit.getPath();
		FileSystem fs = path.getFileSystem(conf);
		System.out.println("Path: " + path.toString());
		System.out.println("Length: " + split.getLength());
		_is = new DataInputStream(fs.open(path));
		
		_singleton_emit = true;
		
		_decoder = CodecManager.getDecoder(ImageType.JPEG_IMAGE);
	}

	@Override
	public void close() throws IOException {
		_is.close();
	}

	@Override
	public ImageHeader getCurrentKey() throws IOException, InterruptedException {
		ByteArrayInputStream _byte_array_input_stream = new ByteArrayInputStream(_byte_array_data);
		ImageHeader header = _decoder.decodeImageHeader(_byte_array_input_stream);
		_byte_array_input_stream.close();
		header.addEXIFInformation("Path", path.toString());
		return header;
	}

	@Override
	public FloatImage getCurrentValue() throws IOException,
			InterruptedException {
		ByteArrayInputStream _byte_array_input_stream = new ByteArrayInputStream(_byte_array_data);
		FloatImage image = _decoder.decodeImage(_byte_array_input_stream);
		_byte_array_input_stream.close();
		return image;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return _singleton_emit == false ? 1.0f : 0.0f;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		
		if(_singleton_emit == true){
			_byte_array_data = readBytes(_is);
			_is.close();
			
			_singleton_emit = false;
			return true;
		}
		else
			return false;
		
	}
	
	private byte[] readBytes(InputStream stream) throws IOException {
	      if (stream == null) return new byte[] {};
	      byte[] buffer = new byte[1024*1024];
	      ByteArrayOutputStream output = new ByteArrayOutputStream();
	      boolean error = false;
	      try {
	          int numRead = 0;
	          while ((numRead = stream.read(buffer)) > -1) {
	              output.write(buffer, 0, numRead);
	          }
	      } catch (IOException e) {
	          error = true; // this error should be thrown, even if there is an error closing stream
	          throw e;
	      } catch (RuntimeException e) {
	          error = true; // this error should be thrown, even if there is an error closing stream
	          throw e;
	      } finally {
	          try {
	              stream.close();
	          } catch (IOException e) {
	              if (!error) throw e;
	          }
	      }
	      output.flush();
	      return output.toByteArray();
	  }
}

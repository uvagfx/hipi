package hipi.experiments.mapreduce;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.CodecManager;
import hipi.image.io.ImageDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JPEGSequenceFileRecordReader
extends RecordReader<ImageHeader, FloatImage> {
	private SequenceFile.Reader _reader;
	private byte _cacheData[];
	private int _cacheType;
	private long _start;
	private long _end;

	@Override
	public void initialize(InputSplit isplit, TaskAttemptContext job)
	throws IOException, InterruptedException {
		FileSplit split = (FileSplit) isplit;
		Path path = split.getPath();

		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		_reader = new SequenceFile.Reader(fs, path, conf);
		_start = split.getStart();
		_end = split.getStart() + split.getLength();
		if (split.getStart() > _reader.getPosition())
			_reader.sync(split.getStart());   

		_cacheData = null;
		_cacheType = 0;		
	}


	@Override
	public ImageHeader getCurrentKey() throws IOException, InterruptedException {
		if (_cacheData != null) {
			ImageDecoder decoder = CodecManager.getDecoder(ImageType.fromValue(_cacheType));
			if (decoder == null)
				return null;
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(_cacheData);
				ImageHeader header = decoder.decodeImageHeader(bis);
				bis.close();
				return header;
			} catch (Exception e) {
				return null;
			}
		}
		return null;
	}

	@Override
	public FloatImage getCurrentValue() throws IOException,
	InterruptedException {
		if (_cacheData != null) {
			ImageDecoder decoder = CodecManager.getDecoder(ImageType.fromValue(_cacheType));
			if (decoder == null)
				return null;
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(_cacheData);
				FloatImage image = decoder.decodeImage(bis);
				bis.close();
				return image;
			} catch (Exception e) {
				return null;
			}
		}
		return null;
	}


	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(_reader.getPosition() > _end){
			return false;			
		}
		try {
			LongWritable key = new LongWritable();
			BytesWritable data = new BytesWritable();
			if (_reader.next(key, data)) {
				_cacheType = (int)(key.get() & 0x3);
				_cacheData = data.getBytes();
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	public void close() throws IOException {
		_reader.close();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (_end == _start) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (float)((_reader.getPosition() - _start) /
					(double)(_end - _start)));
		}
	}
}

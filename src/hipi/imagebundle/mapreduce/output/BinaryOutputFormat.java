package hipi.imagebundle.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class BinaryOutputFormat<K, V> extends FileOutputFormat<K, V> {

	protected static class BinaryRecordWriter<K, V> extends RecordWriter<K, V> {

		protected DataOutputStream out;
		
		public BinaryRecordWriter(DataOutputStream out) {
			this.out = out;
		}
		
		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			out.close();
		}

		@Override
		public void write(K key, V value) throws IOException,
				InterruptedException {
			((Writable) key).write(out);
			((Writable) value).write(out);
		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
			codec = ReflectionUtils.newInstance(codecClass, conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, false);
		if (!isCompressed) {
			return new BinaryRecordWriter<K, V>(fileOut);
		} else {
			return new BinaryRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)));
		}
	}

}

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
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.mapreduce.OutputFormat; 
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Reporter;

public class BinaryOutputFormat<K, V> extends FileOutputFormat<K, V> {

	protected static class BinaryRecordWriter<K, V> implements RecordWriter<K, V> {

		protected DataOutputStream out;
		
		public BinaryRecordWriter(DataOutputStream out) {
			this.out = out;
		}
		
		@Override
		public void close(Reporter reporter) {
			try {
				out.close();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}

		@Override
		public void write(K key, V value) throws IOException {
			((Writable) key).write(out);
			((Writable) value).write(out);
		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
			throws IOException {
		boolean isCompressed = getCompressOutput(job);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
			codec = ReflectionUtils.newInstance(codecClass, job);
			extension = codec.getDefaultExtension();
		}
		Path file = getTaskOutputPath(job, "temp"+extension);
		FileSystem fs = file.getFileSystem(job);
		FSDataOutputStream fileOut = fs.create(file, false);
		if (!isCompressed) {
			return new BinaryRecordWriter<K, V>(fileOut);
		} else {
			return new BinaryRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)));
		}
	}

}

package hipi.examples.downloader;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Treats keys as index into training array and value as the training vector. 
 */
public class DownloaderRecordReader extends RecordReader<LongWritable, LongWritable> 
{
	private boolean singletonEmit;
	private long start;
	private long length;

	public void initialize(InputSplit split, TaskAttemptContext arg1)
	throws IOException, InterruptedException {
		FileSplit f = (FileSplit) split;

		start = f.getStart();
		length = f.getLength();

		singletonEmit = false;
	}


	/**
	 * Get the progress within the split
	 */
	public float getProgress() 
	{
		if (singletonEmit) {
			return 1.0f;
		} else {
			return 0.0f;
		}
	}

	public synchronized void close() throws IOException 
	{
		return;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
	InterruptedException {
		return new LongWritable(start);
	}

	@Override
	public LongWritable getCurrentValue() throws IOException,
	InterruptedException {
		return new LongWritable(length);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(singletonEmit == false){
			singletonEmit = true;
			return true;
		}
		else
			return false;
	}
}

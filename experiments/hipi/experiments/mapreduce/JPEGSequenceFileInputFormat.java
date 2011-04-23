package hipi.experiments.mapreduce;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class JPEGSequenceFileInputFormat  extends
SequenceFileInputFormat<ImageHeader, FloatImage>{
	
	@Override
	public RecordReader<ImageHeader, FloatImage> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new JPEGSequenceFileRecordReader();
	}

}

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Timeout extends Configured implements Tool {

	public static class Map extends
			Mapper<Text, LongWritable, Text, LongWritable> {

		public void map(Text key, LongWritable value, Context context)
				throws IOException, InterruptedException {
			// DO NOTHING
		}

		public void run(Context context) throws IOException,
				InterruptedException {
			long elapsed_time = (new Date()).getTime();
			elapsed_time = (new Date()).getTime() - elapsed_time;
			context.write(new Text("elapsed_time"), new LongWritable(
					elapsed_time));
		}
	}

	public static class IdleRecordReader extends
			RecordReader<Text, LongWritable> {

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public LongWritable getCurrentValue() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return false;
		}

	}

	public static class IdleInputFormat extends
			FileInputFormat<Text, LongWritable> {

		@Override
		public RecordReader<Text, LongWritable> createRecordReader(
				InputSplit arg0, TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			return null;
		}

		@Override
		public List<InputSplit> getSplits(JobContext job) throws IOException {
			List<InputSplit> splits = new ArrayList<InputSplit>();
			Configuration conf = job.getConfiguration();
			int numMapTasks = conf.getInt("hipi.map.tasks", 1);
			Path emptyPath = new Path("");
			splits.add(new FileSplit(emptyPath, lastOffset, 0, hosts.toArray(new String[hosts.size()])));
			return splits;
		}
	}

	public static class Reduce extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			for (LongWritable val : values) {
				context.write(key, val);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Timeout");
		job.setJarByClass(Timeout.class);
		job.setInputFormatClass(IdleInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Benchmark(), args);
		System.exit(ret);
	}

}

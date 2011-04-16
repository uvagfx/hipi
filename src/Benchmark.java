
import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.mapreduce.CullMapper;
import hipi.imagebundle.mapreduce.HipiJob;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class Benchmark extends Configured implements Tool {

	public static class Map extends
			CullMapper<ImageHeader, FloatImage, Text, LongWritable> {		

		public void map(ImageHeader key, FloatImage value, Context context)
				throws IOException, InterruptedException {
			// DO NOTHING
		}

		public void run(Context context) throws IOException, InterruptedException {
			long elapsed_time = (new Date()).getTime();
			setup(context);
			while (context.nextKeyValue()) {
				map(context.getCurrentKey(), context.getCurrentValue(), context);
			}
			cleanup(context);
			elapsed_time = (new Date()).getTime() - elapsed_time;
			context.write(new Text("elapsed_time"), new LongWritable(elapsed_time));
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

	public int run(String[] args) throws Exception {
		HipiJob job = new HipiJob(getConf(), "Benchmark");
		job.setDefault(Benchmark.class);

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

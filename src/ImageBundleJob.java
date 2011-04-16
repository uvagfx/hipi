
import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.mapreduce.CullMapper;
import hipi.imagebundle.mapreduce.HipiJob;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class ImageBundleJob extends Configured implements Tool {

	public static class Map extends
			CullMapper<ImageHeader, FloatImage, Text, IntWritable> {		
		
		public boolean cull(ImageHeader key) {
			return (key != null && key.getEXIFInformation("Model") != null && key.getEXIFInformation("Model").toLowerCase().indexOf("canon") >= 0);
		}
		
		public void map(ImageHeader key, FloatImage value, Context context)
				throws IOException, InterruptedException {
			if (value != null)
				context.write(new Text(key.getEXIFInformation("Model")), new IntWritable(value.getWidth()));
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			for (IntWritable val : values) {
				context.write(key, val);
			}
		}
	}

	public int run(String[] args) throws Exception {
		HipiJob job = new HipiJob(getConf(), "ImageBundleJob");
		job.setDefault(ImageBundleJob.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

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
		int ret = ToolRunner.run(new ImageBundleJob(), args);
		System.exit(ret);
	}

}

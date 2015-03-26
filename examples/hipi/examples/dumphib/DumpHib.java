package hipi.examples.dumphib;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;
import hipi.util.ByteUtils;

public class DumpHib extends Configured implements Tool {

	public static class DumpHibMapper extends Mapper<ImageHeader, FloatImage, IntWritable, Text> {

		@Override
		public void map(ImageHeader key, FloatImage value, Context context)
		throws IOException, InterruptedException {
			if (value != null) {
				int imageWidth = value.getWidth();
				int imageHeight = value.getHeight();
				String hexHash = ByteUtils.asHex(ByteUtils.FloatArraytoByteArray(value.getData()));
				String camera = key.getEXIFInformation("Model");
				String output = imageWidth + "x" + imageHeight + "\t(" + hexHash + ")\t	" + camera;
								
				context.write(new IntWritable(1), new Text(output));
			}
		}

	}
	
	public static class DumpHibReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length < 2) {
			System.out.println("Usage: dumphib <input hib> <outputdir>");
			System.exit(0);
		}
		String inputPath = args[0];
		String outputPath = args[1];

		Job job = new Job(conf, "dumphib");
		job.setJarByClass(DumpHib.class);
		job.setMapperClass(DumpHibMapper.class);
		job.setReducerClass(DumpHibReducer.class);

		// Set formats
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(ImageBundleInputFormat.class);

		// Set out/in paths
		removeDir(outputPath, conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileInputFormat.setInputPaths(job, new Path(inputPath));	

		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DumpHib(), args);
		System.exit(exitCode);
	}

	public static void removeDir(String path, Configuration conf) throws IOException {
		Path output_path = new Path(path);

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(output_path)) {
			fs.delete(output_path, true);
		}
	}
}

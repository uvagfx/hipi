package hipi.experiments.averageimage;

import hipi.experiments.mapreduce.JPEGFileInputFormat;
import hipi.experiments.mapreduce.JPEGSequenceFileInputFormat;
import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.mapreduce.CullMapper;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;
import hipi.imagebundle.mapreduce.output.BinaryOutputFormat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageImageCull extends Configured implements Tool{

	public static class MyMapper extends CullMapper<ImageHeader, FloatImage, NullWritable, FloatImage>
	{		
		public boolean cull(ImageHeader key) throws IOException, InterruptedException {
			if(key.getEXIFInformation("Model").equals("Canon PowerShot S500") && key.width == 2592 && key.height == 1944)
				return false;
			else
				return true;
		}
		
		public void map(ImageHeader key, FloatImage value, Context context) throws IOException, InterruptedException{
			if (key != null && value != null) {
				FloatImage gray = value.convert(FloatImage.RGB2GRAY);
				context.write(NullWritable.get(), gray);
			}
		}
	}
	public static class MyReducer extends Reducer<NullWritable, FloatImage, IntWritable, FloatImage> {
		// Just the basic indentity reducer... no extra functionality needed at this time
		public void reduce(NullWritable key, Iterable<FloatImage> values, Context context) 
		throws IOException, InterruptedException
		{
			FloatImage mean = new FloatImage(2592, 1944, 1);
			int num_pics = 0;
			for (FloatImage val : values) {
				mean.add(val);
				num_pics++;
			}
			float scale = 1.0f/num_pics;
			mean.scale(scale);
			System.out.println("Scale: " + scale);
			
			context.write(new IntWritable(0), mean);
		}
	}

	public int run(String[] args) throws Exception
	{	

		// Read in the configurations
		if (args.length < 3)
		{
			System.out.println("Usage: averageimage <inputdir> <outputdir> <input type: hib, har, sequence, small_files>");
			System.exit(0);
		}


		// Setup configuration
		Configuration conf = new Configuration();

		// set the dir to output the jpegs to
		String outputPath = args[1];
		String input_file_type = args[2];
		conf.setStrings("averageimage.filetype", input_file_type);

		Job job = new Job(conf, "averageimage cull");
		job.setJarByClass(AverageImageCull.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// Set formats
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatImage.class);       
		job.setOutputFormatClass(BinaryOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(FloatImage.class);

		// Set out/in paths
		removeDir(outputPath, conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		JPEGFileInputFormat.addInputPath(job, new Path(args[0]));

		if(input_file_type.equals("hib"))
			job.setInputFormatClass(ImageBundleInputFormat.class);
		else if(input_file_type.equals("har"))
			job.setInputFormatClass(JPEGFileInputFormat.class);
		else if(input_file_type.equals("small_files"))
			job.setInputFormatClass(JPEGFileInputFormat.class);
		else if (input_file_type.equals("sequence"))
			job.setInputFormatClass(JPEGSequenceFileInputFormat.class);
		else{
			System.out.println("Usage: averageimage <inputdir> <outputdir> <input type: hib, har, sequence, small_files>");
			System.exit(0);			
		}

		//conf.set("mapred.job.tracker", "local");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	public static void removeDir(String path, Configuration conf) throws IOException {
		Path output_path = new Path(path);

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(output_path)) {
			fs.delete(output_path, true);
		}
	}
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new AverageImageCull(), args);
		System.exit(res);
	}
}

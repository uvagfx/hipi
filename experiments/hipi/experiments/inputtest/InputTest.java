package hipi.experiments.inputtest;



import hipi.experiments.mapreduce.JPEGFileInputFormat;
import hipi.experiments.mapreduce.JPEGSequenceFileInputFormat;
import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.BooleanWritable;
public class InputTest extends Configured implements Tool{

	public static class MyMapper extends Mapper<ImageHeader, FloatImage, BooleanWritable, LongWritable>
	{
		public void map(ImageHeader key, FloatImage value, Context context) throws IOException, InterruptedException{			
		}
	}
	
	public static class MyReducer extends Reducer<BooleanWritable, LongWritable, BooleanWritable, LongWritable> {
		// Just the basic indentity reducer... no extra functionality needed at this time
		public void reduce(BooleanWritable key, Iterable<LongWritable> values, Context context) 
		throws IOException, InterruptedException
		{
			System.out.println("REDUCING");
			for (LongWritable temp_hash : values) {
				{	    
					context.write(key, temp_hash);
				}
			}
		}
	}
		public int run(String[] args) throws Exception
		{	

			// Read in the configurations
			if (args.length < 2)
			{
				System.out.println("Usage: inputtest <inputdir> <filetype>");
				System.exit(0);
			}


			// Setup configuration
			Configuration conf = new Configuration();

			String input_file_type = args[1];

			Job job = new Job(conf, "inputtest");
			job.setJarByClass(InputTest.class);
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);

			// Set formats
			job.setOutputKeyClass(BooleanWritable.class);
			job.setOutputValueClass(LongWritable.class);       

			// Set out/in paths
			removeDir("/virginia/uvagfx/cms2vp/out", conf);
			FileOutputFormat.setOutputPath(job, new Path("/virginia/uvagfx/cms2vp/out"));
			
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
				System.out.println("Usage: inputtest <inputdir> <filetype>");
				System.exit(0);			
			}

			job.setNumReduceTasks(1);
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
			int res = ToolRunner.run(new InputTest(), args);
			System.exit(res);
		}
	}

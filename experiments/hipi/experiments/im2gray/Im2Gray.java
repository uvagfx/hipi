package hipi.experiments.im2gray;

import hipi.experiments.mapreduce.JPEGFileInputFormat;
import hipi.experiments.mapreduce.JPEGSequenceFileInputFormat;
import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.io.ImageEncoder;
import hipi.image.io.JPEGImageUtil;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.BooleanWritable;

public class Im2Gray extends Configured implements Tool{

	public static class MyMapper extends Mapper<ImageHeader, FloatImage, BooleanWritable, LongWritable>
	{
		private Path path;
		private FileSystem fileSystem;
		private Configuration conf;
		public void setup(Context jc) throws IOException
		{
			conf = jc.getConfiguration();
			fileSystem = FileSystem.get(conf);
			path = new Path( conf.get("im2gray.outdir"));
			fileSystem.mkdirs(path);
		}
		public void map(ImageHeader key, FloatImage value, Context context) throws IOException, InterruptedException{
			if (value != null) {
				FloatImage gray = value.convert(FloatImage.RGB2GRAY);
				//note: images may have the same hash code
				context.write(new BooleanWritable(true), new LongWritable(gray.hashCode()));
				
				//If we later decide we want to output the image
				
				ImageEncoder encoder = JPEGImageUtil.getInstance();
				/*
				//a neccessary step to avoid files with duplicate hash values
				Path outpath = new Path(path + "/" + value.hashCode() + ".jpg");
				while(fileSystem.exists(outpath)){
					String temp = outpath.toString();
					outpath = new Path(temp.substring(0,temp.lastIndexOf('.')) + "1.jpg"); 
				}
				
				FSDataOutputStream os = fileSystem.create(outpath);
				encoder.encodeImage(value, key, os);
				os.flush();
				os.close();
				*/
				 
			}
			else
				context.write(new BooleanWritable(false), new LongWritable(0));
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
			if (args.length < 3)
			{
				System.out.println("Usage: im2gray <inputdir> <outputdir> <input type: hib, har, sequence, small_files>");
				System.exit(0);
			}


			// Setup configuration
			Configuration conf = new Configuration();

			// set the dir to output the jpegs to
			String outputPath = args[1];
			String input_file_type = args[2];
			conf.setStrings("im2gray.outdir", outputPath);
			conf.setStrings("im2gray.filetype", input_file_type);

			Job job = new Job(conf, "im2gray");
			job.setJarByClass(Im2Gray.class);
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);

			// Set formats
			job.setOutputKeyClass(BooleanWritable.class);
			job.setOutputValueClass(LongWritable.class);       

			// Set out/in paths
			removeDir("/virginia/uvagfx/cms2vp/out", conf);
			FileOutputFormat.setOutputPath(job, new Path("/virginia/uvagfx/cms2vp/out"));

			/*
			Path qualifiedPath = new Path("har://", new Path(args[0]).toUri().getPath());
			JPEGFileInputFormat.addInputPath(job, qualifiedPath);	
			*/
			
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
				System.out.println("Usage: im2gray <inputdir> <outputdir> <input type: hib, har, sequence, small_files>");
				System.exit(0);			
			}

			//conf.set("mapred.job.tracker", "local");
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
			int res = ToolRunner.run(new Im2Gray(), args);
			System.exit(res);
		}
	}

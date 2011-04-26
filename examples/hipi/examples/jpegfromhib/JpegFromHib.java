package hipi.examples.jpegfromhib;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.io.ImageEncoder;
import hipi.image.io.JPEGImageUtil;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;
import hipi.util.ByteUtils;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
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

public class JpegFromHib extends Configured implements Tool{

	public static class MyMapper extends Mapper<ImageHeader, FloatImage, BooleanWritable, LongWritable>
	{
		public Path path;
		public FileSystem fileSystem;
		public void setup(Context jc) throws IOException
		{
			Configuration conf = jc.getConfiguration();
			fileSystem = FileSystem.get(conf);
			path = new Path( conf.get("jpegfromhib.outdir"));
			fileSystem.mkdirs(path);
		}
		public void map(ImageHeader key, FloatImage value, Context context) throws IOException, InterruptedException{
			if(value == null)
				return;
			ImageEncoder encoder = JPEGImageUtil.getInstance();
			Path outpath = new Path(path + "/" + value.hex() + ".jpg");
			FSDataOutputStream os = fileSystem.create(outpath);
			encoder.encodeImage(value, key, os);
			os.close();
			context.write(new BooleanWritable(true), new LongWritable(value.hashCode()));
		}
	}
	
	public static class MyReducer extends Reducer<BooleanWritable, LongWritable, BooleanWritable, LongWritable> {
		// Just the basic indentity reducer... no extra functionality needed at this time
		public void reduce(BooleanWritable key, Iterator<LongWritable> values, Context context) 
		throws IOException, InterruptedException
		{
			if (key.get())
			{
				System.out.println("REDUCING");
				while(values.hasNext())
				{	    
					context.write(key, values.next());
				}
			}
		}
	}

	public int run(String[] args) throws Exception
	{	

		// Read in the configurations
		if (args.length < 2)
		{
			System.out.println("args: " + args.length);
			System.out.println("Usage: jpegfromhib <hibfile> <outputdir> <numnodes>");
			System.exit(0);
		}


		// Setup configuration
		Configuration conf = new Configuration();
		
		// set the dir to output the jpegs to
		String outputPath = args[1];
		conf.setStrings("jpegfromhib.outdir", outputPath);
		
		if(args.length >= 3){
			conf.setInt("hipi.map.tasks", Integer.parseInt(args[2]));
		}
		Job job = new Job(conf, "jpegfromhib");
		job.setJarByClass(JpegFromHib.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// Set formats
		job.setOutputKeyClass(BooleanWritable.class);
		job.setOutputValueClass(LongWritable.class);       
		job.setInputFormatClass(ImageBundleInputFormat.class);

		// Set out/in paths
		removeDir("/virginia/uvagfx/cms2vp/out", conf);
		FileOutputFormat.setOutputPath(job, new Path("/virginia/uvagfx/cms2vp/out"));
		ImageBundleInputFormat.setInputPaths(job, new Path(args[0]));	



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
		int res = ToolRunner.run(new JpegFromHib(), args);
		System.exit(res);
	}
}
package hipi.examples.jpegfromhib;

import hipi.image.ImageHeader.ImageType;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;
import hipi.util.ByteUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Iterator;
import java.io.IOException;

public class JpegFromHib extends Configured implements Tool{

	public static class JpegFromHibMapper extends 
		Mapper<NullWritable, BytesWritable, BooleanWritable, Text> {

		public Path path;
		public FileSystem fileSystem;

		@Override
		public void setup(Context jc) throws IOException {
			Configuration conf = jc.getConfiguration();
        	fileSystem = FileSystem.get(jc.getConfiguration());
        	path = new Path(conf.get("jpegfromhib.outdir"));
			fileSystem.mkdirs(path);
       	}

       	@Override
		public void map(NullWritable key, BytesWritable value, 
			Context context) throws IOException, InterruptedException {

			if (value == null) {
				return;
			}
			byte[] val = value.getBytes();
			
			String hashval = ByteUtils.asHex(val);
			Path outpath = new Path(path + "/" + hashval + ".jpg");
			FSDataOutputStream os = fileSystem.create(outpath);
			os.write(val);
			os.flush();
			os.close();
			
			long sig = 0<<2 | ImageType.JPEG_IMAGE.toValue();
			
			//if you want the images to be output as a sequence file, emit the line below
			//and change the output key and values appropriately
			//context.write(new LongWritable(sig), value);
			
			context.write(new BooleanWritable(true), new Text(hashval));
		}
	}

	public int run(String[] args) throws Exception
	{	

		// Read in the configurations
		if (args.length < 2)
		{
			System.out.println("args: " + args.length);
			System.out.println("Usage: hib2jpg <hibfile> <output dir>");
			System.exit(0);
		}
		
		// set the dir to output the jpegs to
		String outputPath = args[1];

		Configuration conf = new Configuration();
		conf.setStrings("jpegfromhib.outdir", outputPath);

		Job job = Job.getInstance(conf, "hib2jpg");
		job.setJarByClass(JpegFromHib.class);
		job.setMapperClass(JpegFromHibMapper.class);
		job.setReducerClass(Reducer.class);

		// Set formats
		job.setOutputKeyClass(BooleanWritable.class);
		job.setOutputValueClass(Text.class);       
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setInputFormatClass(JpegFromHibInputFormat.class);

		// Set out/in paths
		removeDir(args[1], conf);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		ImageBundleInputFormat.setInputPaths(job, new Path(args[0]));	

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
package  hipi.examples.createsequencefile;


import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.ImageEncoder;
import hipi.image.io.JPEGImageUtil;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;

public class CreateSequenceFile extends Configured
implements Tool {

	public static class SequenceFileMapper extends Mapper<ImageHeader, FloatImage, LongWritable, BytesWritable> {

		@Override
		public void map(ImageHeader key, FloatImage value, Context context)
		throws IOException, InterruptedException {
			if(value != null){
				ImageEncoder encoder = JPEGImageUtil.getInstance();
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				encoder.encodeImage(value, key, os);
				os.close();
				byte[] val = os.toByteArray();
				long sig = 0<<2 | ImageType.JPEG_IMAGE.toValue();
				context.write(new LongWritable(sig), new BytesWritable(val));
			}
		}

	}

	public int run(String[] args) throws Exception {
		//Setup configuration
		Configuration conf = new Configuration();
		if (args.length < 2)
		{
			System.out.println("args: " + args.length);
			System.out.println("Usage: createsequencefile <input hib> <outputdir>");
			System.exit(0);
		}
		// set the dir to output the jpegs to
		String outputPath = args[1];
		conf.setStrings("createsequencefile.outdir", outputPath);


		Job job = new Job(conf, "createsequencefile");
		job.setJarByClass(CreateSequenceFile.class);
		job.setMapperClass(SequenceFileMapper.class);
		job.setReducerClass(Reducer.class);

		// Set formats
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BytesWritable.class);   
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(ImageBundleInputFormat.class);

		// Set out/in paths
		removeDir(outputPath, conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileInputFormat.setInputPaths(job, new Path(args[0]));	

		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CreateSequenceFile(), args);
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

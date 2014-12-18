package  hipi.examples.createsequencefile;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.ImageEncoder;
import hipi.image.io.JPEGImageUtil;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;


public class CreateSequenceFile extends Configured
implements Tool {

	public static class SequenceFileMapper extends MapReduceBase implements Mapper<ImageHeader, FloatImage, LongWritable, BytesWritable> {
		@Override
		public void map(ImageHeader key, FloatImage value, 
			OutputCollector<LongWritable, BytesWritable> output, Reporter reporter)
		throws IOException {
			if(value != null){
				ImageEncoder encoder = JPEGImageUtil.getInstance();
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				encoder.encodeImage(value, key, os);
				os.close();
				byte[] val = os.toByteArray();
				long sig = 0<<2 | ImageType.JPEG_IMAGE.toValue();
				output.collect(new LongWritable(sig), new BytesWritable(val));
			}
		}

	}

	public static class SequenceFileReducer extends MapReduceBase implements 
		Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {
		@Override
		public void reduce(LongWritable key, Iterator<BytesWritable> values, 
			OutputCollector<LongWritable, BytesWritable> output, Reporter reporter) throws IOException {
			while(values.hasNext()) {
				output.collect(key, values.next());
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


		JobConf job = new JobConf();
		job.setJarByClass(CreateSequenceFile.class);
		job.setMapperClass(SequenceFileMapper.class);
		job.setReducerClass(SequenceFileReducer.class);

		// Set formats
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BytesWritable.class);   
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setInputFormat(ImageBundleInputFormat.class);

		// Set out/in paths
		removeDir(outputPath, conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileInputFormat.setInputPaths(job, new Path(args[0]));	

		job.setNumReduceTasks(1);
		JobClient.runJob(job);
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

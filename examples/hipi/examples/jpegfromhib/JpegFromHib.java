package hipi.examples.jpegfromhib;

import hipi.image.ImageHeader.ImageType;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;
import hipi.util.ByteUtils;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.FileOutputFormat; 
import org.apache.hadoop.mapred.JobClient;

import java.util.Iterator;

public class JpegFromHib extends Configured implements Tool{

	public static class JpegFromHibMapper extends MapReduceBase implements Mapper<NullWritable, BytesWritable, BooleanWritable, Text>
	{
		public Path path;
		public FileSystem fileSystem;

		@Override
		public void configure(JobConf jConf) {
	        try {
	        	fileSystem = FileSystem.get(jConf);
	        	path = new Path( jConf.get("jpegfromhib.outdir"));
				fileSystem.mkdirs(path);
			} catch (IOException ioe) {
				System.err.println(ioe);
			}

       	}

       	@Override
		public void map(NullWritable key, BytesWritable value, OutputCollector<BooleanWritable, Text> output, Reporter reporter) 
		throws IOException{

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
			
			output.collect(new BooleanWritable(true), new Text(hashval));
			System.out.println("OUT OF MAP");
		}
	}

	public static class JpegFromHibReducer extends MapReduceBase implements Reducer<BooleanWritable, Text, BooleanWritable, Text> {
		@Override
		public void reduce(BooleanWritable key, Iterator<Text> values, 
			OutputCollector<BooleanWritable, Text> output, Reporter reporter) throws IOException {
			while(values.hasNext()) {
				output.collect(key, values.next());
			}
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

		JobConf job = new JobConf();

		job.setStrings("jpegfromhib.outdir", outputPath);
		job.setJarByClass(JpegFromHib.class);
		job.setMapperClass(JpegFromHibMapper.class);
		job.setReducerClass(JpegFromHibReducer.class);

		// Set formats
		job.setOutputKeyClass(BooleanWritable.class);
		job.setOutputValueClass(Text.class);       
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setInputFormat(JpegFromHibInputFormat.class);

		// Set out/in paths
		removeDir(args[1], job);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		ImageBundleInputFormat.setInputPaths(job, new Path(args[0]));	

		job.setNumReduceTasks(1);

		JobClient.runJob(job);
		return 0;
	}
	public static void removeDir(String path, JobConf conf) throws IOException {
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
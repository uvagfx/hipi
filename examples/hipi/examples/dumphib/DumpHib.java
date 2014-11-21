package hipi.examples.dumphib;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.pipes.Submitter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;
import hipi.util.ByteUtils;

public class DumpHib extends Configured implements Tool {

	public static class DumpHibMapper extends MapReduceBase implements Mapper<ImageHeader, FloatImage, IntWritable, Text> {
        
        
		@Override
		public void map(ImageHeader key, FloatImage value, OutputCollector<IntWritable, Text> output, Reporter reporter)
		throws IOException {
			if (value != null) {
				int imageWidth = value.getWidth();
				int imageHeight = value.getHeight();
				String hexHash = ByteUtils.asHex(ByteUtils.FloatArraytoByteArray(value.getData()));
				String camera = key.getEXIFInformation("Model");
				String outputString = imageWidth + "x" + imageHeight + "\t(" + hexHash + ")\t	" + camera;
				output.collect(new IntWritable(1), new Text(outputString));				
				//context.write(new IntWritable(1), new Text(output));
			}
		}

	}
	
	public static class DumpHibReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) 
		throws IOException {
			while(values.hasNext()) {
				output.collect(key, values.next());
			}
		}
	}

	public int run(String[] args) throws Exception {
        
        // Set job configuration
        JobConf jConf = new JobConf();
        jConf.setJarByClass(DumpHib.class);
        jConf.setMapperClass(DumpHibMapper.class);
        jConf.setReducerClass(DumpHibReducer.class);
        jConf.setNumReduceTasks(1);
        
        // Set formats
		jConf.setOutputKeyClass(IntWritable.class);
		jConf.setOutputValueClass(Text.class);
		jConf.setInputFormat(ImageBundleInputFormat.class);
        
		if (args.length < 2) {
			System.out.println("Usage: dumphib <input hib> <outputdir>");
			System.exit(0);
		}
        
		String inputPath = args[0];
		String outputPath = args[1];

		Job job = new Job(jConf);
        job.setJobName("dumphib");

		// Set out/in paths
		removeDir(outputPath, jConf);
		FileOutputFormat.setOutputPath(jConf, new Path(outputPath));
		FileInputFormat.setInputPaths(jConf, new Path(inputPath));	

        //TODO
        Submitter submitter = new Submitter();
        submitter.runJob(jConf);
        
		System.exit(0);

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DumpHib(), args);
		System.exit(exitCode);
	}

	public static void removeDir(String path, JobConf jConf) throws IOException {
		Path output_path = new Path(path);

		FileSystem fs = FileSystem.get(jConf);

		if (fs.exists(output_path)) {
			fs.delete(output_path, true);
		}
	}
}

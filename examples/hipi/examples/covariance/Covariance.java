package hipi.examples.covariance;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.mapreduce.HipiJob;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;
import hipi.imagebundle.mapreduce.output.BinaryOutputFormat;
import hipi.imagebundle.HipiImageBundle;
import hipi.image.ImageHeader.ImageType;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.pipes.Submitter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;



public class Covariance extends Configured implements Tool {

	public static final int N = 48;
	public static final float sigma = 10;

	public static Job cacheJob;

	public static class MeanMap extends MapReduceBase implements 
		Mapper<ImageHeader, FloatImage, IntWritable, FloatImage> {	

		private static JobConf jConf;

		@Override
		public void configure(JobConf jConf) {
	        this.jConf = jConf;
       	}	
		
		@Override
		public void map(ImageHeader key, FloatImage value, 
			OutputCollector<IntWritable, FloatImage> output, Reporter reporter) throws IOException {
			if (value != null && value.getWidth() > N && value.getHeight() > N) {
				FloatImage mean = new FloatImage(N, N, 1);
				for (int i = 0; i < 10; i++) {
					int x = (value.getWidth() - N) * i / 10;
					for (int j = 0; j < 10; j++) {
						int y = (value.getHeight() - N) * j / 10;
						FloatImage patch = value.crop(x, y, N, N).convert(FloatImage.RGB2GRAY);
						mean.add(patch);
					}
				}
				mean.scale(0.01f);
				output.collect(new IntWritable(0), mean);
			}
		}
	}

	public static class MeanReduce extends MapReduceBase implements
			Reducer<IntWritable, FloatImage, IntWritable, FloatImage> {

		private static JobConf jConf;

		@Override
		public void configure(JobConf jConf) {
	        this.jConf = jConf;
       	}	

		public void reduce(IntWritable key, Iterator<FloatImage> values, 
			OutputCollector<IntWritable, FloatImage> output, Reporter reporter) throws IOException {
			FloatImage mean = new FloatImage(N, N, 1);
			int total = 0;
			while(values.hasNext()) {
				FloatImage temp = values.next();
				mean.add(temp);
				total++;
			}
			if (total > 0) {
				mean.scale(1.0f / total);
				output.collect(key, mean);
				reporter.progress();
				createTestHib(mean);
			}
		}

		private void createTestHib(FloatImage mean) throws IOException {
			HipiImageBundle hib = 
				new HipiImageBundle(new Path("zdv8rb/updated/covariance/output2.hib"), jConf);
			hib.open(HipiImageBundle.FILE_MODE_WRITE, true);
			hib.addImage(mean);
			hib.close();
		}
	}

	public static class CovarianceMap extends MapReduceBase implements
			Mapper<ImageHeader, FloatImage, IntWritable, FloatImage> {		

		float[] g;
		float[] mean;


		private static JobConf jConf;

		@Override
		public void configure(JobConf jConf) {
			try {
		        this.jConf = jConf;
		        Job job = Job.getInstance(jConf, "Covariance");
		        g = new float[N * N];
				float tg = 0;
				for (int i = 0; i < N; i++) {
					for (int j = 0; j < N; j++) {
						tg += g[i * N + j] = (float) Math.exp(-((i - N / 2) * (i - N / 2) / 
							(sigma * sigma) + (j - N / 2) * (j - N / 2) / (sigma * sigma)));
					}
				}
				tg = (N * N) / tg;

				for (int i = 0; i < N; i++) {
					for (int j = 0; j < N; j++) {
						g[i * N + j] *= tg;
					}
				}

				URI[] files = new URI[1];
				if (cacheJob.getCacheFiles() != null) {
		            files = cacheJob.getCacheFiles();
	        	} else {
	        		System.out.println("cache files null...");
	        	}
				
				FSDataInputStream dis = FileSystem.get(jConf).open(new Path(files[0].toString()));
				dis.skip(4);
				FloatImage image = new FloatImage();
				image.readFields(dis);
				mean = image.getData();
			} catch (IOException ioe) {
				System.err.println(ioe);
			}
       	}	

       	@Override
		public void map(ImageHeader key, FloatImage value, 
			OutputCollector<IntWritable, FloatImage> output, Reporter reporter) throws IOException {

			if (value != null && value.getWidth() > N && value.getHeight() > N) {
				float[][] tp = new float[100][N * N];
				for (int i = 0; i < 10; i++) {
					int x = (value.getWidth() - N) * i / 10;
					for (int j = 0; j < 10; j++) {
						int y = (value.getHeight() - N) * j / 10;
						FloatImage patch = value.crop(x, y, N, N).convert(FloatImage.RGB2GRAY);
						float[] pels = patch.getData();
						for (int k = 0; k < N * N; k++)
								tp[i * 10 + j][k] = (pels[k] - mean[k]) * g[k];
					}
				}
				float[] cov = new float[N * N * N * N];
				for (int i = 0; i < N * N; i++)
					for (int j = 0; j < N * N; j++) {
						cov[i * N * N + j] = 0;
						for (int k = 0; k < 100; k++)
							cov[i * N * N + j] += tp[k][i] * tp[k][j];
					}
				output.collect(new IntWritable(0), new FloatImage(N * N, N * N, 1, cov));
			}
		}
	}

	public static class CovarianceReduce extends MapReduceBase implements
			Reducer<IntWritable, FloatImage, IntWritable, FloatImage> {

		private static JobConf jConf;

		@Override
		public void configure(JobConf jConf) {
	        this.jConf = jConf;
       	}	

		public void reduce(IntWritable key, Iterator<FloatImage> values, 
			OutputCollector<IntWritable, FloatImage> output, Reporter reporter) throws IOException {

			FloatImage cov = new FloatImage(N * N, N * N, 1);

			while (values.hasNext()) {
				cov.add(values.next());
			}

			output.collect(key, cov);
			createTestHib(cov);
		}

		private void createTestHib(FloatImage mean) throws IOException {
			HipiImageBundle hib = 
				new HipiImageBundle(new Path("/zdv8rb/updated/covariance/final.hib"), jConf);
			hib.open(HipiImageBundle.FILE_MODE_WRITE, true);
			hib.addImage(mean);
			hib.close();
		}
	}

	public static void rmdir(String path, Configuration conf) throws IOException {
		Path output_path = new Path(path);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output_path)) {
			fs.delete(output_path, true);
		}
	}
	
	public static void mkdir(String path, JobConf conf) throws IOException {
		Path output_path = new Path(path);
		FileSystem fs = FileSystem.get(conf);
		if (!fs.exists(output_path))
			fs.mkdirs(output_path);
	}

	public int runMeanCompute(String[] args) throws Exception {


		HipiJob job = new HipiJob(getConf(), "Covariance");
		job.setJarByClass(Covariance.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatImage.class);

		job.setMapperClass(MeanMap.class);
		job.setCombinerClass(MeanReduce.class);
		job.setReducerClass(MeanReduce.class);

		String inputFileType = args[2];
		if(inputFileType.equals("hib"))
			job.setInputFormat(ImageBundleInputFormat.class);
		else {
			System.out.println("Usage: covariance <inputdir> <outputdir> <filetype>");
			System.exit(0);			
		}
		job.setOutputFormat(BinaryOutputFormat.class);
		// job.setCompressMapOutput(true);
		job.setMapSpeculativeExecution(true);
		job.setReduceSpeculativeExecution(true);
		// FileOutputFormat.setCompressOutput(job, true);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		mkdir(args[1], job);
		rmdir(args[1] + "/mean-output/", job);
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/mean-output/"));
		System.out.println("InputPath: "+args[0]);
		System.out.println("OutputPath: "+args[1]+"/mean-output/");
		job.setNumReduceTasks(1);
		JobClient.runJob(job);
        
		return 0;
	}
	
	public int runCovariance(String[] args) throws Exception {
		System.out.println("~~~~~~~IN COVARIANCE~~~~~~~~~~");
		HipiJob job = new HipiJob(getConf(), "Covariance");
		job.setJarByClass(Covariance.class);

		cacheJob = Job.getInstance(job, "Covariance");
        cacheJob.addCacheFile(new Path("hdfs://" + args[1] + "/mean-output/temp").toUri());
        
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatImage.class);

		job.setMapperClass(CovarianceMap.class);
		job.setCombinerClass(CovarianceReduce.class);
		job.setReducerClass(CovarianceReduce.class);

		String inputFileType = args[2];
		if(inputFileType.equals("hib"))
			job.setInputFormat(ImageBundleInputFormat.class);
		else{
			System.out.println("Usage: covariance <inputdir> <outputdir> <filetype>");
			System.exit(0);			
		}
		job.setOutputFormat(BinaryOutputFormat.class);
		job.setCompressMapOutput(true);
		job.setMapSpeculativeExecution(true);
		job.setReduceSpeculativeExecution(true);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		mkdir(args[1], job);
		rmdir(args[1] + "/covariance-output/", job);
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/covariance-output/"));

		JobClient.runJob(job);
        
		return 0;
	}
	
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
            System.out.println("Number of arguments is incorrect.");
            System.out.println("Expecting 3 arguments. Received "+args.length+".");
			System.out.println("Usage: covariance <inputdir> <outputdir> <filetype>");
			System.exit(0);
		}
		
		int success = runMeanCompute(args);
		if (success == 1)
			return 1;
		return runCovariance(args); 
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Covariance(), args);
		System.exit(0);
	}

}

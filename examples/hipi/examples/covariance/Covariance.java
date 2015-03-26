package hipi.examples.covariance;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.mapreduce.HipiJob;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;
import hipi.imagebundle.mapreduce.output.BinaryOutputFormat;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class Covariance extends Configured implements Tool {

	public static final int N = 48;
	public static final float sigma = 10;


	public static class MeanMap extends
			Mapper<ImageHeader, FloatImage, IntWritable, FloatImage> {		
		
		public void map(ImageHeader key, FloatImage value, Context context)
				throws IOException, InterruptedException {
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
				context.write(new IntWritable(0), mean);
			}
		}
	}

	public static class MeanReduce extends
			Reducer<IntWritable, FloatImage, IntWritable, FloatImage> {
		public void reduce(IntWritable key, Iterable<FloatImage> values,
				Context context) throws IOException, InterruptedException {
			FloatImage mean = new FloatImage(N, N, 1);
			int total = 0;
			for (FloatImage val : values) {
				mean.add(val);
				total++;
			}
			if (total > 0) {
				mean.scale(1.0f / total);
				context.write(key, mean);
			}
		}
	}

	public static class CovarianceMap extends
			Mapper<ImageHeader, FloatImage, IntWritable, FloatImage> {		

		float[] g;
		float[] mean;

		public void setup(Context job) throws IOException {
			g = new float[N * N];
			float tg = 0;
			for (int i = 0; i < N; i++)
				for (int j = 0; j < N; j++)
					tg += g[i * N + j] = (float) Math.exp(-((i - N / 2) * (i - N / 2) / (sigma * sigma) + (j - N / 2) * (j - N / 2) / (sigma * sigma)));
			tg = (N * N) / tg;
			for (int i = 0; i < N; i++)
				for (int j = 0; j < N; j++)
					g[i * N + j] *= tg;
			/* DistributedCache will be deprecated in 0.21 */
			Path file = DistributedCache.getLocalCacheFiles(job.getConfiguration())[0];
			FSDataInputStream dis = FileSystem.getLocal(job.getConfiguration()).open(file);
			dis.skip(4);
			FloatImage image = new FloatImage();
			image.readFields(dis);
			mean = image.getData();
		}

		public void map(ImageHeader key, FloatImage value, Context context)
				throws IOException, InterruptedException {
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
				context.write(new IntWritable(0), new FloatImage(N * N, N * N, 1, cov));
			}
		}
	}

	public static class CovarianceReduce extends
			Reducer<IntWritable, FloatImage, IntWritable, FloatImage> {
		public void reduce(IntWritable key, Iterable<FloatImage> values,
				Context context) throws IOException, InterruptedException {
			FloatImage cov = new FloatImage(N * N, N * N, 1);
			for (FloatImage val : values) {
				cov.add(val);
			}
			context.write(key, cov);
		}
	}

	public static void rmdir(String path, Configuration conf) throws IOException {
		Path output_path = new Path(path);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output_path)) {
			fs.delete(output_path, true);
		}
	}
	
	public static void mkdir(String path, Configuration conf) throws IOException {
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
			job.setInputFormatClass(ImageBundleInputFormat.class);
		else {
			System.out.println("Usage: covariance <inputdir> <outputdir> <filetype>");
			System.exit(0);			
		}
		job.setOutputFormatClass(BinaryOutputFormat.class);
		job.setCompressMapOutput(true);
		job.setMapSpeculativeExecution(true);
		job.setReduceSpeculativeExecution(true);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		mkdir(args[1], job.getConfiguration());
		rmdir(args[1] + "/mean-output/", job.getConfiguration());
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/mean-output/"));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public int runCovariance(String[] args) throws Exception {
		HipiJob job = new HipiJob(getConf(), "Covariance");
		job.setJarByClass(Covariance.class);

		/* DistributedCache will be deprecated in 0.21 */
		DistributedCache.addCacheFile(new URI("hdfs://" + args[1] + "/mean-output/part-r-00000"), job.getConfiguration());

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatImage.class);

		job.setMapperClass(CovarianceMap.class);
		job.setCombinerClass(CovarianceReduce.class);
		job.setReducerClass(CovarianceReduce.class);

		String inputFileType = args[2];
		if(inputFileType.equals("hib"))
			job.setInputFormatClass(ImageBundleInputFormat.class);
		else{
			System.out.println("Usage: covariance <inputdir> <outputdir> <filetype>");
			System.exit(0);			
		}
		job.setOutputFormatClass(BinaryOutputFormat.class);
		job.setCompressMapOutput(true);
		job.setMapSpeculativeExecution(true);
		job.setReduceSpeculativeExecution(true);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		mkdir(args[1], job.getConfiguration());
		rmdir(args[1] + "/covariance-output/", job.getConfiguration());
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/covariance-output/"));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
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

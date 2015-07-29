package hipi.examples.covariance;

import hipi.image.FloatImage;
import hipi.image.HipiImageHeader;
import hipi.image.HipiImageHeader.HipiImageFormat;
import hipi.imagebundle.mapreduce.HibInputFormat;
import hipi.imagebundle.mapreduce.output.BinaryOutputFormat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Iterator;

public class Covariance extends Configured implements Tool {

  public static final int N = 48; // Patch size is NxN
  public static final float sigma = 10; // Standard deviation of Gaussian weighting function

  public static class MeanMapper extends Mapper<HipiImageHeader, FloatImage, IntWritable, FloatImage> {

    @Override
    public void map(HipiImageHeader key, FloatImage value, Context context) throws IOException,
        InterruptedException {
      if (value != null && value.getWidth() > N && value.getHeight() > N) {
        context.write(new IntWritable(0), generateMeanImage(value, 100, 100));
      }
    }

    // Compute the mean of (xPatchCount * yPatchCount) patches within the input image
    private FloatImage generateMeanImage(FloatImage input, int xPatchCount, int yPatchCount) {
      FloatImage mean = new FloatImage(N, N, 1);
      for (int i = 0; i < xPatchCount; i++) {
        int x = (input.getWidth() - N) * i / xPatchCount;
        for (int j = 0; j < yPatchCount; j++) {
          int y = (input.getHeight() - N) * j / yPatchCount;
          FloatImage patch = input.crop(x, y, N, N).convert(FloatImage.RGB2GRAY);
          mean.add(patch);
        }
      }
      mean.scale((float) (1.0 / (xPatchCount * yPatchCount)));
      return mean;
    }
  }

  public static class MeanReducer extends Reducer<IntWritable, FloatImage, IntWritable, FloatImage> {

    @Override
    public void reduce(IntWritable key, Iterable<FloatImage> values, Context context)
        throws IOException, InterruptedException {
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

  public static class CovarianceMapper extends
      Mapper<HipiImageHeader, FloatImage, IntWritable, FloatImage> {

    float[] gaussianArray;
    float[] mean;

    @Override
    public void setup(Context job) {
      // Create a normalized gaussian array with standard deviation of 10 pixels for patch masking
      try {
        gaussianArray = new float[N * N];
        float gaussianSum = 0;
        for (int i = 0; i < N; i++) {
          for (int j = 0; j < N; j++) {
            // Bivariate Gaussian
            gaussianSum +=
                gaussianArray[i * N + j] =
                    (float) Math.exp(-((i - N / 2) * (i - N / 2) / (sigma * sigma) + (j - N / 2)
                        * (j - N / 2) / (sigma * sigma)));
          }
        }
	// Normalize Gaussian mask
        for (int i = 0; i < N; i++) {
          for (int j = 0; j < N; j++) {
            gaussianArray[i * N + j] *= (N * N) / gaussianSum;
          }
        }

	// Access the job cache
        URI[] files = new URI[1];
        if (job.getCacheFiles() != null) {
          files = job.getCacheFiles();
        } else {
          System.err.println("Job cache files is null!");
        }

        // Read mean from previously run mean job
        Path cacheFilePath = new Path(files[0].toString());
        FSDataInputStream dis = FileSystem.get(job.getConfiguration()).open(cacheFilePath);
        dis.skip(4);
        FloatImage image = new FloatImage();
        image.readFields(dis);
        mean = image.getData();
      } catch (IOException ioe) {
        System.err.println(ioe);
      }
    }

    @Override
    public void map(HipiImageHeader key, FloatImage value, Context context) throws IOException,
        InterruptedException {

      if (value != null && value.getWidth() > N && value.getHeight() > N) {
        // Holds 100 patches as they are collected from the image
        float[][] patchArray = new float[100][N * N];
        // Generate mean-subtracted (whitened) and Gaussian weighted patches and stores them in patchArray
        for (int i = 0; i < 10; i++) {
          int x = (value.getWidth() - N) * i / 10;
          for (int j = 0; j < 10; j++) {
            int y = (value.getHeight() - N) * j / 10;
            FloatImage patch = value.crop(x, y, N, N).convert(FloatImage.RGB2GRAY);
            float[] pels = patch.getData();
            for (int k = 0; k < N * N; k++) {
              // Subtract mean and weight using Gaussian mask
              patchArray[i * 10 + j][k] = (pels[k] - mean[k]) * gaussianArray[k];
            }
          }
        }
        // Stores the (N^2 x N^2) covariance matrix AAt
        float[] covarianceArray = new float[N * N * N * N];
        for (int i = 0; i < N * N; i++) {
          for (int j = 0; j < N * N; j++) {
            covarianceArray[i * N * N + j] = 0;
            for (int k = 0; k < 10; k++) {
              covarianceArray[i * N * N + j] += patchArray[k][i] * patchArray[k][j];
            }
          }
        }
        context.write(new IntWritable(0), new FloatImage(N * N, N * N, 1, covarianceArray));
      }
    }
  }

  public static class CovarianceReducer extends
      Reducer<IntWritable, FloatImage, IntWritable, FloatImage> {

    @Override
    public void reduce(IntWritable key, Iterable<FloatImage> values, Context context)
        throws IOException, InterruptedException {
      // Aggregate sub-matrices in full covariance calculation
      FloatImage cov = new FloatImage(N * N, N * N, 1);
      for (FloatImage val : values) {
        cov.add(val);
      }
      context.write(key, cov);
    }
  }

  public static void rmdir(String path, Configuration conf) throws IOException {
    Path outputPath = new Path(path);
    FileSystem fileSystem = FileSystem.get(conf);
    if (fileSystem.exists(outputPath)) {
      fileSystem.delete(outputPath, true);
    }
  }

  public static void mkdir(String path, Configuration conf) throws IOException {
    Path outputPath = new Path(path);
    FileSystem fileSystem = FileSystem.get(conf);
    if (!fileSystem.exists(outputPath))
      fileSystem.mkdirs(outputPath);
  }

  public boolean runComputeMean(String[] args) throws Exception {

    Job job = Job.getInstance();

    job.setJarByClass(Covariance.class);

    job.setInputFormatClass(ImageBundleInputFormat.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(FloatImage.class);

    job.setMapperClass(MeanMapper.class);
    job.setCombinerClass(MeanReducer.class);
    job.setReducerClass(MeanReducer.class);

    job.setOutputFormatClass(BinaryOutputFormat.class);
    job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
    job.setSpeculativeExecution(true);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    mkdir(args[1], job.getConfiguration());
    rmdir(args[1] + "/mean-output/", job.getConfiguration());
    FileOutputFormat.setOutputPath(job, new Path(args[1] + "/mean-output/"));

    return job.waitForCompletion(true);
  }

  public boolean runCovariance(String[] args) throws Exception {
    Job job = Job.getInstance();

    job.setJarByClass(Covariance.class);

    job.addCacheFile(new URI("hdfs://" + args[1] + "/mean-output/part-r-00000"));

    job.setInputFormatClass(ImageBundleInputFormat.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(FloatImage.class);

    job.setMapperClass(CovarianceMapper.class);
    job.setCombinerClass(CovarianceReducer.class);
    job.setReducerClass(CovarianceReducer.class);

    job.setOutputFormatClass(BinaryOutputFormat.class);
    job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
    job.setSpeculativeExecution(true);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    mkdir(args[1], job.getConfiguration());
    rmdir(args[1] + "/covariance-output/", job.getConfiguration());
    FileOutputFormat.setOutputPath(job, new Path(args[1] + "/covariance-output/"));

    return job.waitForCompletion(true);
  }

  public int run(String[] args) throws Exception {
    
    if (args.length !=2) {
      System.out.println("Usage: covariance <input HIB> <output directory>");
      System.exit(0);
    }
    
    if (!runComputeMean(args)) {
      return 1;
    }
      
    if (!runCovariance(args)) {
      return 1;
    }

    // Indicate success
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Covariance(), args);
    System.exit(res);
  }

}

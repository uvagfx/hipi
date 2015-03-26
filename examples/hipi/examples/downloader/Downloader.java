package hipi.examples.downloader;

import hipi.image.ImageHeader.ImageType;
import hipi.imagebundle.HipiImageBundle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A utility MapReduce program that takes a list of image URL's, downloads them, and creates 
 * a {@link hipi.imagebundle.HipiImageBundle} from them.
 * 
 * When running this program, the user must specify 3 parameters. The first is the location 
 * of the list of URL's (one URL per line), the second is the output path for the HIB that will
 * be generated, and the third is the number of nodes that should be used during the 
 * program's execution. This final parameter should be chosen with respect to the total
 * bandwidth your particular cluster is able to handle. An example usage would be:
 * <br /><br />
 * downloader.jar /path/to/urls.txt /path/to/output.hib 10
 * <br /><br />
 * This program will automatically force 10 nodes to download the set of URL's contained in 
 * the input list, thus if your list contains 100,000 images, each node in this example will
 * be responsible for downloading 10,000 images.
 *
 */
public class Downloader extends Configured implements Tool{

	
	public static class DownloaderMapper extends Mapper<IntWritable, Text, BooleanWritable, Text>
	{
		private static Configuration conf;
		// This method is called on every node
		public void setup(Context jc) throws IOException
		{
			conf = jc.getConfiguration(); 
		}

		public void map(IntWritable key, Text value, Context context) 
		throws IOException, InterruptedException
		{
			String temp_path = conf.get("downloader.outpath") + key.get() + ".hib.tmp";
			System.out.println("Temp path: " + temp_path);
			
			HipiImageBundle hib = new HipiImageBundle(new Path(temp_path), conf);
			hib.open(HipiImageBundle.FILE_MODE_WRITE, true);

			String word = value.toString();

			BufferedReader reader = new BufferedReader(new StringReader(word));
			String uri;
			int i = key.get();
			int iprev = i;
			while((uri = reader.readLine()) != null)			
			{
				if(i >= iprev+100) {
					hib.close();
					context.write(new BooleanWritable(true), new Text(hib.getPath().toString()));
					temp_path = conf.get("downloader.outpath") + i + ".hib.tmp";
					hib = new HipiImageBundle(new Path(temp_path), conf);
					hib.open(HipiImageBundle.FILE_MODE_WRITE, true);
					iprev = i;
				}
				long startT=0;
				long stopT=0;	   
				startT = System.currentTimeMillis();	    	    

				try {
					String type = "";
					URLConnection conn;
					// Attempt to download
					context.progress();

					try {
						URL link = new URL(uri);
						System.err.println("Downloading " + link.toString());
						conn = link.openConnection();
						conn.connect();
						type = conn.getContentType();
					} catch (Exception e)
					{
						System.err.println("Connection error to image : " + uri);
						continue;
					}

					if (type == null)
						continue;

					if (type.compareTo("image/gif") == 0)
						continue;

					if (type != null && type.compareTo("image/jpeg") == 0)
						hib.addImage(conn.getInputStream(), ImageType.JPEG_IMAGE);
					
				} catch(Exception e)
				{
					e.printStackTrace();
					System.err.println("Error... probably cluster downtime");
					try
					{
						Thread.sleep(1000);			    
					} catch (InterruptedException e1)
					{
						e1.printStackTrace();
					}
				}

				i++;
				
				// Emit success
				stopT = System.currentTimeMillis();
				float el = (float)(stopT-startT)/1000.0f;
				System.err.println("> Took " + el + " seconds\n");				
			}


			try
			{
				reader.close();
				hib.close();
				context.write(new BooleanWritable(true), new Text(hib.getPath().toString()));
			} catch (Exception e)
			{
				e.printStackTrace();
			}

		}
	}

	public static class DownloaderReducer extends Reducer<BooleanWritable, Text, BooleanWritable, Text> {

		private static Configuration conf;		
		public void setup(Context jc) throws IOException
		{
			conf = jc.getConfiguration();
		}

		public void reduce(BooleanWritable key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException
		{
			if(key.get()){
				FileSystem fileSystem = FileSystem.get(conf);
				HipiImageBundle hib = new HipiImageBundle(new Path(conf.get("downloader.outfile")), conf);
				hib.open(HipiImageBundle.FILE_MODE_WRITE, true);
				for (Text temp_string : values) {
					Path temp_path = new Path(temp_string.toString());
					HipiImageBundle input_bundle = new HipiImageBundle(temp_path, conf);
					hib.append(input_bundle);
					
					Path index_path = input_bundle.getPath();
					Path data_path = new Path(index_path.toString() + ".dat");
					System.out.println("Deleting: " + data_path.toString());
					fileSystem.delete(index_path, false);
					fileSystem.delete(data_path, false);
					
					context.write(new BooleanWritable(true), new Text(input_bundle.getPath().toString()));
					context.progress();
				}
				hib.close();
			}
		}
	}


	public int run(String[] args) throws Exception
	{	

		// Read in the configuration file
		if (args.length < 3)
		{
			System.out.println("Usage: downloader <input file> <output file> <nodes>");
			System.exit(0);
		}

		// Setup configuration
		Configuration conf = new Configuration();

		String inputFile = args[0];
		String outputFile = args[1];
		int nodes = Integer.parseInt(args[2]);

		String outputPath = outputFile.substring(0, outputFile.lastIndexOf('/')+1);
		System.out.println("Output HIB: " + outputPath);
		
		
		conf.setInt("downloader.nodes", nodes);
		conf.setStrings("downloader.outfile", outputFile);
		conf.setStrings("downloader.outpath", outputPath);

		Job job = new Job(conf, "downloader");
		job.setJarByClass(Downloader.class);
		job.setMapperClass(DownloaderMapper.class);
		job.setReducerClass(DownloaderReducer.class);

		// Set formats
		job.setOutputKeyClass(BooleanWritable.class);
		job.setOutputValueClass(Text.class);       
		job.setInputFormatClass(DownloaderInputFormat.class);

		//*************** IMPORTANT ****************\\
		job.setMapOutputKeyClass(BooleanWritable.class);
		job.setMapOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(outputFile + "_output"));

		DownloaderInputFormat.setInputPaths(job, new Path(inputFile));

		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void createDir(String path, Configuration conf) throws IOException {
		Path output_path = new Path(path);

		FileSystem fs = FileSystem.get(conf);

		if (!fs.exists(output_path)) {
			fs.mkdirs(output_path);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Downloader(), args);
		System.exit(res);
	}
}
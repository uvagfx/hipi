package hipi.examples.downloader;

import hipi.image.ImageHeader.ImageType;
import hipi.imagebundle.AbstractImageBundle;
import hipi.imagebundle.HipiImageBundle;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.io.BooleanWritable;

import org.apache.hadoop.fs.FileSystem;
import java.net.URL;
import java.net.URLConnection;

public class Downloader extends Configured implements Tool{

	public static class DownloaderMapper extends Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable>
	{
		private static int numRecords;
		private static int pause = 0;
		private static AbstractImageBundle hib;
		private int maxAlpha;

		// This method is called on every node
		public void setup(Context jc) throws IOException
		{
			Configuration conf = jc.getConfiguration(); 
			maxAlpha = conf.getInt("strontium.downloader.max", 0);
			numRecords = conf.getInt("strontium.downloader.numRecords", 1000);
			hib = new HipiImageBundle(new Path("/virginia/uvagfx/cms2vp/bigbundle.hib"), jc.getConfiguration());
			hib.open(AbstractImageBundle.FILE_MODE_WRITE, true);
		}

		public void map(LongWritable key, LongWritable value, Context context) 
		throws IOException, InterruptedException
		{
			Connection con; 

			int start = (int) key.get();
			int length = (int) value.get();

			System.out.println("maxalpha = " + maxAlpha);
			System.out.println("Getting records [" + start + ", " + (start+length) + ")");
			context.setStatus("Getting records [" + start + ", " + (start+length) + ")");
			
			try 
			{	    
				//Register the JDBC driver for MySQL.
				Class.forName("com.mysql.jdbc.Driver");
				String url = "jdbc:mysql://galicia.cs.virginia.edu/strontium";
				con = DriverManager.getConnection(url,"sean", "l33tgfx");

				//Display URL and connection information
				System.out.println("URL: " + url);
				System.out.println("Connection: " + con);

				//Get a Statement object	    
			} catch(Exception e)
			{
				e.printStackTrace();
				context.write(new BooleanWritable(false), new LongWritable(-1));
				return;
			}

			for (int alpha = start; alpha < (int) Math.min(maxAlpha,start+length); alpha++)
			{
				context.setStatus("Downloading record #" + alpha);
				System.out.println("Downloading record #" + alpha);
				String sql = 	"SELECT url ";
				sql +=		"FROM download_images ";
				sql +=		"WHERE id > " + alpha*numRecords + " AND id <= " + ((alpha+1)*numRecords);

				System.out.println("Beginning of alpha");
				try
				{
					long startT=0;
					long stopT=0;	   
					startT = System.currentTimeMillis();

					Statement stmt;
					ResultSet rs;

					stmt = con.createStatement();
					// Execute statment
					rs = stmt.executeQuery(sql); /** @throws SQLException */	    	    

					while(rs.next())
					{						
						try {
							String uri = rs.getString("url"); /** @throws SQLException */
							String type = "";
							URLConnection conn;
							// Attempt to download
							context.progress();

							try {
								URL link = new URL(uri);
								conn = link.openConnection();
								conn.connect();
								type = conn.getContentType();
								//System.out.println(type + ":" + fpath);
							} catch (Exception e)
							{
								System.err.println("Connection error to flickr image : " + uri);
								continue;
							}

							if (type == null)
								continue;

							if (type.compareTo("image/gif") == 0)
								continue;

							if (type != null)
							{										
								if (type.compareTo("image/jpeg") == 0)
									hib.addImage(conn.getInputStream(), ImageType.JPEG_IMAGE);
							}		
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
							rs.previous();			    
						}
					}

					// Emit success
					stopT = System.currentTimeMillis();
					float el = (float)(stopT-startT)/1000.0f;
					System.out.println("Took " + el + " seconds");
					System.out.println("-----------------------\n");
					context.write(new BooleanWritable(true), new LongWritable(alpha));
					try
					{
						Thread.sleep(pause);
						context.progress();
					} catch (InterruptedException e1)
					{
						e1.printStackTrace();
					}
				} catch(Exception e)
				{
					e.printStackTrace();
					try
					{
						Thread.sleep(300 * 1000);
						context.progress();
					} catch (InterruptedException e1)
					{
						e1.printStackTrace();
					}
					alpha--;
				}
			}

			try
			{
				con.close();
				hib.close();
			} catch (Exception e)
			{
				e.printStackTrace();
			}

		}
	}

	public static class DownloaderReducer extends Reducer<BooleanWritable, LongWritable, BooleanWritable, LongWritable> {
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


	public static int numRecords = 1000;

	public int run(String[] args) throws Exception
	{	

		// Read in the configuration file
		if (args.length < 4)
		{
			System.out.println("Usage: downloader <config file> <start> <end> <nodes>");
			System.exit(0);
		}

		String configFile = args[0];
		int start = Integer.parseInt(args[1]);
		int end = Integer.parseInt(args[2]);
		int nodes = Integer.parseInt(args[3]);

		// Setup configuration
		Configuration conf = new Configuration();
		

		Statement stmt;
		ResultSet rs;
		Connection con;    
		int maxAlpha;

		try 
		{	    
			//Register the JDBC driver for MySQL.
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://galicia.cs.virginia.edu/strontium";
			con = DriverManager.getConnection(url,"sean", "l33tgfx");

			//Display URL and connection information
			System.out.println("URL: " + url);
			System.out.println("Connection: " + con);

			//Get a Statement object
			stmt = con.createStatement();
			String sql = 	"SELECT COUNT(id) AS max ";
			sql +=		"FROM download_images ";

			// Execute statment
			rs = stmt.executeQuery(sql); /** @throws SQLException */	   
			rs.next();

			maxAlpha = (int) Math.ceil( (float) rs.getInt("max") / (float) numRecords); /** @throws SQLException */		

		} catch(Exception e)
		{
			e.printStackTrace();
			return 0;
		}    
		
		con.close();
		
		conf.setInt("strontium.downloader.start", start);
		conf.setInt("strontium.downloader.max", (int) Math.min(end, maxAlpha));
		conf.setInt("strontium.downloader.numRecords", numRecords);
		conf.setInt("strontium.downloader.nodes", nodes);

		// Add custom XML document with dimensions
		conf.addResource(new Path(configFile));
		
		Job job = new Job(conf, "downloader");
		job.setJarByClass(Downloader.class);
		job.setMapperClass(DownloaderMapper.class);
		job.setReducerClass(DownloaderReducer.class);

		// Set formats
		job.setOutputKeyClass(BooleanWritable.class);
		job.setOutputValueClass(LongWritable.class);       
		job.setInputFormatClass(DownloaderInputFormat.class);

		//*************** IMPORTANT ****************\\
		job.setMapOutputKeyClass(BooleanWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		// Set out/in paths
		removeDir("/virginia/uvagfx/out", conf);
		FileOutputFormat.setOutputPath(job, new Path("/virginia/uvagfx/out"));
		DownloaderInputFormat.setInputPaths(job, new Path(configFile));	


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
		int res = ToolRunner.run(new Downloader(), args);
		System.exit(res);
	}
}
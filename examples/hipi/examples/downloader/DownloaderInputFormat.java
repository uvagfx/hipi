package hipi.examples.downloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DownloaderInputFormat extends FileInputFormat<IntWritable, Text> 
{

	protected class QuickFile
	{
		public String host;
		public Path file;
	}

	/**
	 * Returns an object that can be used to read records of type ImageInputFormat
	 */
	@Override
	public RecordReader<IntWritable, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) 
	throws IOException, InterruptedException {
		return new DownloaderRecordReader();
	}


	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException 
	{
		Configuration conf = job.getConfiguration();
		int nodes = conf.getInt("downloader.nodes", 10);


		ArrayList<QuickFile> hosts = new ArrayList<QuickFile>(0);
		List<InputSplit> splits = new ArrayList<InputSplit>();

		FileSystem fileSystem = FileSystem.get(conf);
		String tempOutputPath = conf.get("downloader.outpath") + "_tmp";
		Path tempOutputDir = new Path(tempOutputPath);
		
		if (fileSystem.exists(tempOutputDir)) {
			fileSystem.delete(tempOutputDir, true);
		}
		fileSystem.mkdirs(tempOutputDir);
		
		int i = 0;
		while( hosts.size() < nodes && i < 2*nodes)
		{
			String tempFileString = tempOutputPath + "/" + i;
			Path tempFile = new Path(tempFileString);
			FSDataOutputStream os = fileSystem.create(tempFile);
			os.write(i);
			os.close();

			FileStatus match = fileSystem.getFileStatus(tempFile);
			long length = match.getLen();
			BlockLocation[] blocks = fileSystem.getFileBlockLocations(match, 0, length);

			boolean save = true;
			for (int j = 0; j < hosts.size(); j++)
			{
				if (blocks[0].getHosts()[0].compareTo(hosts.get(j).host) == 0)
				{
					save = false;
					System.out.println("Repeated host: " + i);
					break;
				}
			}

			if (save)
			{
				QuickFile Q = new QuickFile();
				Q.host = blocks[0].getHosts()[0];
				Q.file = tempFile;
				hosts.add(Q);
				System.out.println("Found host successfully: " + i);
			}
			i++;
		}

		System.out.println("Tried to get " + nodes + " nodes, got " + hosts.size());


		FileStatus file = listStatus(job).get(0);
		Path path = file.getPath();
		BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
		int num_lines = 0;
		while(reader.readLine() != null){
			num_lines++;
		}
		reader.close();
		
		int per = (int) Math.ceil(((float) (num_lines)) / ((float) hosts.size()));

		System.out.println("Each node responsible for " + per + " image downloads");

		int index = 0;
		FileSplit[] f = new FileSplit[hosts.size()];
		for (i = 0; i < f.length; i++)
		{
			String[] host = new String[1];
			host[0] = hosts.get(index++).host;
			if (index >= hosts.size())
				index = 0;

			splits.add( new FileSplit(path , (i*per) , per, host));
		}
		if (fileSystem.exists(tempOutputDir)) {
			fileSystem.delete(tempOutputDir, true);
		}
		return splits;    
	}
}

package hipi.examples.downloader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;

public class DownloaderInputFormat extends FileInputFormat<LongWritable, LongWritable> 
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
	public RecordReader<LongWritable, LongWritable> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) 
	throws IOException, InterruptedException {
		return new DownloaderRecordReader();
	}


	private static final PathFilter hiddenFileFilter = new PathFilter(){
		public boolean accept(Path p){
			String name = p.getName(); 
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException 
	{
		Configuration conf = job.getConfiguration();

		int maxAlpha = conf.getInt("strontium.downloader.max", 0);
		int start = conf.getInt("strontium.downloader.start", 0);
		int nodes = conf.getInt("strontium.downloader.nodes", 10);
		System.out.println("(start, maxAlpha) = (" + start + ", " + maxAlpha + ")");
		// Get DataNodes
		/*
	  FSNamesystem fsn = FSNamesystem.getFSNamesystem();
	  ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
	  ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();

	  fsn.DFSNodesStatus(live, dead);

		 */

		ArrayList<QuickFile> hosts = new ArrayList<QuickFile>(0);
		List<InputSplit> splits = new ArrayList<InputSplit>();

		FileSystem fileSystem = FileSystem.get(conf);
		Path dir = new Path("/virginia/shared/flickr/0/0/0/0/0/0");
		FileStatus[] matches = fileSystem.listStatus(FileUtil.stat2Paths(fileSystem.globStatus(dir, hiddenFileFilter)), hiddenFileFilter);

		for (FileStatus match: matches)
		{
			long length = match.getLen();
			Path file = fileSystem.makeQualified(match.getPath());
			BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileSystem.getFileStatus(file), 0, length);

			boolean save = true;
			for (int i = 0; i < hosts.size(); i++)
			{
				if (blocks[0].getHosts()[0].compareTo(hosts.get(i).host) == 0)
				{
					save = false;
					break;
				}
			}

			if (save)
			{
				QuickFile Q = new QuickFile();
				Q.host = blocks[0].getHosts()[0];
				Q.file = file;
				hosts.add(Q);
			}

			if (hosts.size() == nodes)
				break;
		}

		System.out.println("Tried to get " + nodes + " nodes, got " + hosts.size());

		int per = (int) Math.ceil(((float) (maxAlpha - start)) / ((float) hosts.size()));

		System.out.println("Each node responsible for " + per + " download sets");

		int index = 0;
		FileSplit[] f = new FileSplit[hosts.size()];
		for (int i = 0; i < f.length; i++)
		{
			String[] host = new String[1];
			host[0] = hosts.get(index++).host;
			if (index >= hosts.size())
				index = 0;

			splits.add( new FileSplit(hosts.get(index).file , (start+i*per) , per, host));
		}

		return splits;    
	}

}

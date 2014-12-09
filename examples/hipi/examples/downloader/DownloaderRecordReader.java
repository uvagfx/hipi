package hipi.examples.downloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
/**
 * Treats keys as index into training array and value as the training vector. 
 */
public class DownloaderRecordReader implements RecordReader<IntWritable, Text> 
{
	private boolean singletonEmit;
	private String urls;
	private long start_line;
	private long current_line;
	private String[] urlList;

	public DownloaderRecordReader(InputSplit split, JobConf jConf) {
		super();
		try {
			initialize(split, jConf);
		} catch (IOException ioe) {
			System.err.println(ioe);
		}
	}
	
	public void initialize(InputSplit split, JobConf jConf) throws IOException {
		FileSplit f = (FileSplit) split;
		Path path = f.getPath();
		FileSystem fs = path.getFileSystem(jConf);

		start_line = f.getStart();
		long num_lines = f.getLength();

		singletonEmit = false;

		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
		int i = 0;
		while(i < start_line && reader.readLine() != null){
			i++;
		}
		
		urls = "";
		String line;
		for(i = 0; i < num_lines && (line = reader.readLine()) != null; i++){
			urls += line + '\n';
		}

		urlList = urls.split("\n");
		current_line = 0;
		reader.close();
	}


	/**
	 * Get the progress within the split
	 */
	@Override
	public float getProgress() 
	{
		if (singletonEmit) {
			return 1.0f;
		} else {
			return 0.0f;
		}
	}

	@Override
	public void close() throws IOException 
	{
		return;
	}

	@Override
	public IntWritable createKey() {
		return new IntWritable((int)current_line);
	}

	public IntWritable getKey(int location) {
		return new IntWritable(location);
	}

	@Override
	public Text createValue() {
		return new Text(urlList[(int)current_line]);
	}

	public Text getValue(int location) {
		return new Text(urlList[location]);
	}

	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(singletonEmit == false){
			singletonEmit = true;
			return true;
		}
		else
			return false;
	}

	@Override
	public boolean next(IntWritable key, Text value) throws IOException {
		System.out.println("Current Position: "+getPos());
		System.out.println("Length: "+urlList.length);
		if(getPos() < urlList.length) {
			key = getKey((int)getPos());
			value = getValue((int)getPos());
			current_line = current_line + 1;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public long getPos() throws IOException {
		return current_line;
	}


}

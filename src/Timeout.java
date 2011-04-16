import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Timeout extends Configured implements Tool {

	public static class Map extends
			Mapper<Text, LongWritable, Text, LongWritable> {

		public void map(Text key, LongWritable value, Context context)
				throws IOException, InterruptedException {
			// DO NOTHING
		}

		public void run(Context context) throws IOException,
				InterruptedException {
			setup(context);
			long expectedTime = context.getCurrentValue().get();
			long currentTime = (new Date()).getTime();
			System.out.println("expectedTime: " + expectedTime + ", currentTime: " + currentTime);
			while (currentTime < expectedTime) {
				currentTime = (new Date()).getTime();
			}
			URL ping = new URL("http://liuliu.me/hipi/ping.php");
			URLConnection pc = ping.openConnection();
	        BufferedReader in = new BufferedReader(new InputStreamReader(pc.getInputStream()));
	        String serverTime = in.readLine();
			context.write(new Text("serverTime"), new LongWritable(Long.valueOf(serverTime)));
		}
	}

	public static class IdleRecordReader extends
			RecordReader<Text, LongWritable> {

		@Override
		public void close() throws IOException {
			// DO NOTHING
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public LongWritable getCurrentValue() throws IOException,
				InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 1;
		}

		private static final Text key = new Text("expectedTime");
		private LongWritable value;
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) split;
			value = new LongWritable(fileSplit.getStart());
			System.out.println("record expected time " + fileSplit.getStart());
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return false;
		}

	}

	public static class IdleInputFormat extends
			FileInputFormat<Text, LongWritable> {

		@Override
		public RecordReader<Text, LongWritable> createRecordReader(
				InputSplit arg0, TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			return new IdleRecordReader();
		}

		private static final PathFilter hiddenFileFilter = new PathFilter(){
			public boolean accept(Path p){
				String name = p.getName(); 
				return !name.startsWith("_") && !name.startsWith(".");
			}
		};

		@Override
		public List<InputSplit> getSplits(JobContext job) throws IOException {
			Configuration conf = job.getConfiguration();
			int numMapTasks = conf.getInt("hipi.map.tasks", 1);
			Path emptyPath = new Path("/");
			Set<String> hostSet = new HashSet<String>();
			List<InputSplit> splits = new ArrayList<InputSplit>();

			FileSystem fileSystem = FileSystem.get(conf);
			Path dir = new Path("/virginia/shared/flickr/0/0/0/0/0/0");
			FileStatus[] matches = fileSystem.listStatus(FileUtil.stat2Paths(fileSystem.globStatus(dir, hiddenFileFilter)), hiddenFileFilter);

			for (FileStatus match: matches)
			{
				long length = match.getLen();
				Path file = fileSystem.makeQualified(match.getPath());
				BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileSystem.getFileStatus(file), 0, length);

				if (!hostSet.contains(blocks[0].getHosts()[0]))
					hostSet.add(blocks[0].getHosts()[0]);

				if (hostSet.size() == numMapTasks)
					break;
			}
			String[] hosts = (String[]) hostSet.toArray(new String[hostSet.size()]);
			System.out.println("Tried to initiate " + numMapTasks + " mapper, got " + hosts.length);
			long currentTime = (new Date()).getTime();
			for (int i = 0; i < hosts.length; i++) {
				String[] localHosts = new String[1];
				localHosts[0] = hosts[i];
				splits.add(new FileSplit(emptyPath, currentTime + 120 * 1000, 0, localHosts));
			}
			return splits;
		}
	}

	public static class Reduce extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			for (LongWritable val : values) {
				context.write(key, val);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Timeout");
		job.setJarByClass(Timeout.class);
		job.setInputFormatClass(IdleInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		URL ping = new URL("http://liuliu.me/hipi/ping.php");
		URLConnection pc = ping.openConnection();
        BufferedReader in = new BufferedReader(new InputStreamReader(pc.getInputStream()));
        String serverTime = in.readLine();
        System.out.println(Long.valueOf(serverTime));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Timeout(), args);
		System.exit(ret);
	}

}

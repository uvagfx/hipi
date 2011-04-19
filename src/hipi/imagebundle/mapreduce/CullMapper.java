package hipi.imagebundle.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

public class CullMapper <KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	
	protected boolean cull(KEYIN key) throws IOException, InterruptedException {
		return true;
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			if (context.getCurrentKey() != null && cull(context.getCurrentKey()))
				map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}

}

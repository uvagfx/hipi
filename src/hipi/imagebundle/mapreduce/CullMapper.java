package hipi.imagebundle.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * The CullMapper class allows for a culling stage to occur prior to the Map phase being executed. This gives
 * users the ability to filter input data from their image sets. The user specifies a 
 * {@link hipi.imagebundle.mapreduce.CullMapper#cull(Object)} method that determines which inputs pass through the filter
 * i.e. which inputs return true for the cull method. Users should implement a class that extends the CullMapper
 * and implements the cull method accordingly
 *
 * @param <KEYIN> The class of the Mapper input key
 * @param <VALUEIN> The class of the Mapper input value
 * @param <KEYOUT> The class of the Mapper output key
 * @param <VALUEOUT> The class of the Mapper output value
 */
public class CullMapper <KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	
	/**
	 * Determines which inputs should be presented to the map phase. Users should implement this method according to
	 * how they would like to cull the input data.
	 * @param key
	 * @return True if the input should be culled. False if it should not.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected boolean cull(KEYIN key) throws IOException, InterruptedException {
		return false;
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			if (context.getCurrentKey() != null && !cull(context.getCurrentKey()))
				map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}

}

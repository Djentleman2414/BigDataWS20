package types;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;

/**
 * Example class for float[] based on ArrayWritable
 */

public class FloatArrayWritable extends ArrayWritable {

	public FloatArrayWritable() {
		super(FloatWritable.class);
	}

	public FloatArrayWritable(FloatWritable[] values) {
		super(FloatWritable.class, values);
	}

}

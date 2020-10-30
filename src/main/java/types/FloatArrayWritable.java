package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

/**
 * Example class for float[] based on ArrayWritable
 */

public class FloatArrayWritable implements WritableComparable<FloatArrayWritable>, Cloneable  {

	private float data[];

	public FloatArrayWritable() {
		data = new float[0];
	}

	public FloatArrayWritable(float... values) {
		data = values.clone();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt((data == null) ? 0 : data.length);
		for (float d : data)
			out.writeFloat(d);
	}

	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		data = null;
		if (length > 0) {
			data = new float[length];
			for (int i = 0; i < length; ++i)
				data[i] = in.readFloat();
		}
	}

	public float get(int i) {
		return data[i];
	}

	public void set(int i,float val) {
		data[i]=val;
	}

	public int compareTo(FloatArrayWritable o) {
		float s = 0; 
		int i = 0;
		do {
			s = this.data[i] - o.data[i];
		} while ((++i < data.length) && (s == 0));
		if (s == 0) return 0;
		return s > 0 ? 1 : -1;
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof FloatArrayWritable)) return false;
		return compareTo((FloatArrayWritable)o)==0;
	}

	public int hashCode() {
	    return Arrays.hashCode(data);
	}

	public String toString() {
		return Arrays.toString(data);
	}
	
	public Object clone() {
		return new FloatArrayWritable(data);
	}

}

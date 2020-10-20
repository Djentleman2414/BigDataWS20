package ncdc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

public class IntArrayWritable implements WritableComparable<IntArrayWritable>, Cloneable {

	private int data[];

	public IntArrayWritable() {
		data = new int[0];
	}

	public IntArrayWritable(int... values) {
		data = values.clone();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt((data == null) ? 0 : data.length);
		for (int d : data)
			out.writeInt(d);
	}

	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		data = null;
		if (length > 0) {
			data = new int[length];
			for (int i = 0; i < length; ++i)
				data[i] = in.readInt();
		}
	}

	public int get(int i) {
		return data[i];
	}

	public void set(int i,int val) {
		data[i]=val;
	}

	public int compareTo(IntArrayWritable o) {
		int s, i = 0;
		do {
			s = this.data[i] - o.data[i];
		} while ((++i < data.length) && (s == 0));
		return s;
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof IntArrayWritable)) return false;
		return compareTo((IntArrayWritable)o)==0;
	}

	public int hashCode() {
	    return Arrays.hashCode(data);
	}

	public String toString() {
		return Arrays.toString(data);
	}
	
	public Object clone() {
		return new IntArrayWritable(data);
	}
}

package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntDoubleWritable implements WritableComparable<IntDoubleWritable>, Cloneable {

	private int i;
	private double v;

	public IntDoubleWritable() {
		// intern unbedingt benoetigt
	}

	public IntDoubleWritable(int i, double v) {
		this.i = i;
		this.v = v;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(i);
		out.writeDouble(v);
	}

	public void readFields(DataInput in) throws IOException {
		i = in.readInt();
		v = in.readDouble();
	}

	public String toString() {
		return i + "\t" + v;
	}
	
	public void set(int i, double v) {
		this.i = i;
		this.v = v;
	}
	
	public void setInt(int i) {
		this.i = i;
	}

	public int getInt() {
		return i;
	}
	
	public void setDouble(double v) {
		this.v = v;
	}

	public double getDouble() {
		return v;
	}

	@Override
	public int hashCode() {
	    return Double.hashCode(v) ^ Integer.reverse(i);
	}

	public int compareTo(IntDoubleWritable other) {
		int delta = this.i - other.i;
		if (delta != 0)
			return delta;
		else
			return (int) Math.signum(this.v - other.v);
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof IntDoubleWritable)) return false;
		return compareTo((IntDoubleWritable)o)==0;
	}

	public Object clone() {
		return new IntDoubleWritable(i,v);
	}
	
}

package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DoublePairWritable implements Cloneable, WritableComparable<DoublePairWritable> {

	private double x, y;

	public DoublePairWritable() {
	}

	public DoublePairWritable(double x, double y) {
		this.x = x;
		this.y = y;
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
	}

	public void readFields(DataInput in) throws IOException {
		x = in.readDouble();
		y = in.readDouble();
	}

	public String toString() {
		return x + "\t" + y;
	}

	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}

	public void set(double u,double v) {
		x=u; y=v;
	}

	public void setX(double v) {
		x=v;
	}

	public void setY(double v) {
		y=v;
	}

	@Override
	public int hashCode() {
	    return Double.hashCode(x) + Double.hashCode(y);
	}

	public int compareTo(DoublePairWritable other) {
		int comp = Double.compare(x, other.x);
		if(comp != 0)
			return comp;
		return Double.compare(y, other.y);
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof DoublePairWritable)) return false;
		return compareTo((DoublePairWritable)o)==0;
	}

	public Object clone() {
		return new DoublePairWritable(x,y);
	}

}

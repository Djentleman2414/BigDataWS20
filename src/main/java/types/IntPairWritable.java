package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntPairWritable implements WritableComparable<IntPairWritable>, Cloneable {

	private int x, y;

	public IntPairWritable() {
		// intern unbedingt benoetigt
	}

	public IntPairWritable(int x, int y) {
		this.x = x;
		this.y = y;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(x);
		out.writeInt(y);
	}

	public void readFields(DataInput in) throws IOException {
		x = in.readInt();
		y = in.readInt();
	}

	public String toString() {
		return x + "\t" + y;
	}

	public int getX() {
		return x;
	}

	public int getY() {
		return y;
	}

	public void set(int u,int v) {
		x=u; y=v;
	}

	public void setX(int v) {
		x=v;
	}

	public void setY(int v) {
		y=v;
	}

	@Override
	public int hashCode() {
	    return Integer.reverse(x) ^ y;
	}

	public int compareTo(IntPairWritable other) {
		int delta = this.x - other.x;
		if (delta != 0)
			return delta;
		else
			return this.y - other.y;
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof IntPairWritable)) return false;
		return compareTo((IntPairWritable)o)==0;
	}

	public Object clone() {
		return new IntPairWritable(x,y);
	}
	
}

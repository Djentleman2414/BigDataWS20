package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class LongPairWritable implements WritableComparable<LongPairWritable>, Cloneable {

	private long x, y;

	public LongPairWritable() {
		// intern unbedingt benoetigt
	}

	public LongPairWritable(long x, long y) {
		this.x = x;
		this.y = y;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(x);
		out.writeLong(y);
	}

	public void readFields(DataInput in) throws IOException {
		x = in.readLong();
		y = in.readLong();
	}

	public String toString() {
		return x + "\t" + y;
	}

	public long getX() {
		return x;
	}

	public long getY() {
		return y;
	}

	public void set(long u,long v) {
		x=u; y=v;
	}

	public void setX(long v) {
		x=v;
	}

	public void setY(long v) {
		y=v;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(x) + Long.hashCode(y);
	}

	public int compareTo(LongPairWritable other) {
		int comp = Long.compare(x, other.x);
		if(comp != 0)
			return comp;
		return Long.compare(y, other.y);
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof LongPairWritable)) return false;
		return compareTo((LongPairWritable)o)==0;
	}

	public Object clone() {
		return new LongPairWritable(x,y);
	}
	
}

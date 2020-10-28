package assignment.tmpres;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class StringIntPairWritable implements WritableComparable<StringIntPairWritable>, Cloneable {

	private String x;
	private int y;

	public StringIntPairWritable() {
		// intern unbedingt benoetigt
	}

	public StringIntPairWritable(String x, int y) {
		this.x = x;
		this.y = y;
	}

	public void write(DataOutput out) throws IOException {
		out.writeChars(x);
		out.writeInt(y);
	}

	public void readFields(DataInput in) throws IOException {
		String[] line = in.readLine().split("[+]");
		x = line[0];
		System.out.println(x + "+" + line[1].charAt(1) + line[1].charAt(3));
		y = Integer.parseInt("" +line[1].charAt(1) + line[1].charAt(3));
	}

	public String toString() {
		return x + "\t" + y;
	}

	public String getX() {
		return x;
	}

	public int getY() {
		return y;
	}

	public void set(String u,int v) {
		x=u; y=v;
	}

	public void setX(String v) {
		x=v;
	}

	public void setY(int v) {
		y=v;
	}

	@Override
	public int hashCode() {
	    return Integer.reverse(x.hashCode()) ^ y;
	}

	public int compareTo(StringIntPairWritable other) {
		int delta = this.x.compareTo(other.x);
		if (delta != 0)
			return delta;
		else
			return this.y - other.y;
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof StringIntPairWritable)) return false;
		return compareTo((StringIntPairWritable)o)==0;
	}

	public Object clone() {
		return new StringIntPairWritable(x,y);
	}
}

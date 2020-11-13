package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntTrippleWritable implements WritableComparable<IntTrippleWritable>, Cloneable {
	
	private int x , y ,z;
	
	public IntTrippleWritable() {
	}
	
	public IntTrippleWritable(int x, int y, int z) {
		set(x, y, z);
	}
	
	public void set(int x, int y, int z) {
		this.x = x;
		this.y = y;
		this.z = z;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(x);
		out.writeInt(y);
		out.writeInt(z);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readInt();
		y = in.readInt();
		z = in.readInt();
		
	}

	@Override
	public int compareTo(IntTrippleWritable o) {
		int comp = Integer.compare(x, o.x);
		if(comp != 0)
			return comp;
		comp = Integer.compare(y, o.y);
		if(comp != 0)
			return comp;
		return Integer.compare(z, o.z);
	}

	public int getX() {
		return x;
	}

	public void setX(int x) {
		this.x = x;
	}

	public int getY() {
		return y;
	}

	public void setY(int y) {
		this.y = y;
	}

	public int getZ() {
		return z;
	}

	public void setZ(int z) {
		this.z = z;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + x;
		result = prime * result + y;
		result = prime * result + z;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntTrippleWritable other = (IntTrippleWritable) obj;
		if (x != other.x)
			return false;
		if (y != other.y)
			return false;
		if (z != other.z)
			return false;
		return true;
	}
	
	public IntTrippleWritable clone() {
		IntTrippleWritable clone = new IntTrippleWritable(x,y,z);
		return clone;
	}
	
	public String toString() {
		return x + "\t" + y + "\t" + z;
	}

}

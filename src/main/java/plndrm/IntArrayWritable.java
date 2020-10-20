package plndrm;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {

	public IntArrayWritable() {
		super(IntWritable.class);
	}

	@Override
	public boolean equals(Object obj) {
		if(this == obj)
			return true;
		if (obj.getClass() != this.getClass())
			return false;
		IntArrayWritable other = (IntArrayWritable) obj;
		
		IntWritable[] valueArray = (IntWritable[]) this.toArray();
		int[] thisValue = new int[valueArray.length];
		for(int i = 0; i < thisValue.length; i++)
			thisValue[i] = valueArray[i].get();
		
		valueArray = (IntWritable[]) other.toArray();
		int[] otherValue = new int[valueArray.length];
		for(int i = 0; i < otherValue.length; i++)
			otherValue[i] = valueArray[i].get();
			
		return Arrays.equals(thisValue, otherValue);
	}
	
	

}

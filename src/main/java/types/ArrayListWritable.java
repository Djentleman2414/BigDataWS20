package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class ArrayListWritable<T> implements Writable {
	
	private ArrayList<T> list;
	
	public ArrayListWritable() {
		list = new ArrayList<>();
	}
	
	public int size() {
		return list.size();
	}
	
	public T get(int index) {
		return list.get(index);
	}
	
	public void add(T e) {
		list.add(e);
	}
	
	public ArrayList<T> getList() {
		return list;
	}
	
	public void set(ArrayList<T> list) {
		this.list = list;
	}

	@Override
	public void write(DataOutput out) throws IOException {

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

}

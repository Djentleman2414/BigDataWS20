package datagen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit implements Writable {

	private int shard;
	
	public void setShard(int id) {
		this.shard=id;
	}
	
	public int getShard() {
		return shard;
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[0];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(shard);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		shard = in.readInt();
	}

}

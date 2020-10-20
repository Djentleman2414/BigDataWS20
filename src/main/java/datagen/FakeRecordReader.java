package datagen;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FakeRecordReader extends RecordReader<LongWritable, Text> {

	public static final String FAKERECORDSIZE = "fake.record.size";
	
	private LongWritable key;
	private Text value;
	private long recordSize = 0, count = 0;
	private ShardedIterator iter;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		key = new LongWritable();
		value = new Text();
		recordSize = context.getConfiguration().getInt(FAKERECORDSIZE, 6);
		// must call setIterator to complete initialization
	}

	public void setIterator(ShardedIterator wi) {
		iter=wi;
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean valid = iter.hasNext();
		if (valid) {
			++count;
			key.set(iter.getPosition()*recordSize); 
			value.set(iter.next());
		}
		return valid;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (iter.hasNext()) ? (float)count/(float)(count+1000) : 1; // simplified
	}

	@Override
	public void close() throws IOException {
		// no files were opened		
	}
	
}
